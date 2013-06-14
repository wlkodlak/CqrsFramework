using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace CqrsFramework.Infrastructure
{
    public interface ITableProvider : IDisposable
    {
        void Insert(TableProviderRow row);
        void Update(TableProviderRow row);
        void Delete(TableProviderRow row);
        TableProviderRow NewRow();
        TableProviderColumn[] GetColumns();
        TableProviderRow[] GetRows(TableProviderFilter[] filter);
        TableProviderFilterable GetRows();
        long GetMaxRowNumber();
    }

    public class TableProviderRow
    {
        private ITableProvider _table;
        private int _rowNumber;
        private object[] _data;
        private Dictionary<string, TableProviderColumn> _columns;

        public TableProviderRow(ITableProvider table, int rowNumber, object[] data)
        {
            _table = table;
            _rowNumber = rowNumber;
            _data = data;
            _columns = new Dictionary<string, TableProviderColumn>();
            foreach (var column in table.GetColumns())
                _columns[column.Name] = column;
        }

        public ITableProvider Table { get { return _table; } }
        public int RowNumber
        {
            get { return _rowNumber; }
            set { _rowNumber = value; }
        }
        public object this[int index]
        {
            get { return _data[index - 1]; }
            set { _data[index - 1] = value; }
        }
        public object this[string name]
        {
            get { return _data[_columns[name].Ordinal - 1]; }
            set { Set(name, value); }
        }

        private void Set(string name, object value)
        {
            var column = _columns[name];
            VerifyType(column.Type, value);
            _data[column.Ordinal - 1] = value;
        }

        private void VerifyType(Type type, object value)
        {
            if (value != null && type != value.GetType())
                throw new InvalidCastException(string.Format("Value {0} is not a {1}", value, type.Name));
        }

        public T Get<T>(string name)
        {
            return (T)_data[_columns[name].Ordinal - 1];
        }
    }

    public class TableProviderColumn
    {
        public int Ordinal { get; private set; }
        public string Name { get; private set; }
        public bool HasIndex { get; private set; }
        public Type Type { get; private set; }
        public TableProviderColumn(int ordinal, string name, Type type, bool hasIndex)
        {
            this.Ordinal = ordinal;
            this.Name = name;
            this.Type = type;
            this.HasIndex = hasIndex;
        }
        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }
        public override bool Equals(object obj)
        {
            var oth = obj as TableProviderColumn;
            return oth != null && Ordinal == oth.Ordinal && Name == oth.Name && Type == oth.Type;
        }
        public override string ToString()
        {
            return string.Format("{0} ({1}{2}: {3})", Name, Ordinal, HasIndex ? " Idx" : "", Type.Name);
        }
    }

    public enum TableProviderFilterType
    {
        Nonfiltered,
        Exact,
        Minimum,
        Maximum,
        Range
    }

    public class TableProviderFilter
    {
        public TableProviderFilterType Type { get; set; }
        public int ColumnIndex { get; set; }
        public object MinValue { get; set; }
        public object MaxValue { get; set; }
    }

    public class TableProviderFilterable : IEnumerable<TableProviderRow>
    {
        private ITableProvider _table;
        private Dictionary<string, int> _columns;
        private Dictionary<int, TableProviderFilter> _filters;
        private bool _noResults;

        public TableProviderFilterable(ITableProvider table)
        {
            _table = table;
            _columns = new Dictionary<string, int>();
            _filters = new Dictionary<int, TableProviderFilter>();
            _filters[0] = new TableProviderFilter() { ColumnIndex = 0 };
            foreach (var column in table.GetColumns())
            {
                _columns[column.Name] = column.Ordinal;
                _filters[column.Ordinal] = new TableProviderFilter() { ColumnIndex = column.Ordinal };
            }
            _noResults = false;
        }

        public TableProviderFilterable SetFilter(string name, object exactly, object minimum, object maximum)
        {
            if (_noResults)
                return this;

            var column = (name == "id") ? 0 : _columns[name];
            var oldFilter = _filters[column];
            var newFilter = CreateNewFilter(exactly, minimum, maximum, column);

            var combined = CombineFilters(oldFilter, newFilter);
            if (combined == null)
            {
                _noResults = true;
                return this;
            }
            else
            {
                _filters[column] = combined;
                return this;
            }
        }

        private static TableProviderFilter CombineFilters(TableProviderFilter oldFilter, TableProviderFilter newFilter)
        {
            if (oldFilter.Type == TableProviderFilterType.Nonfiltered)
                return newFilter;
            if (newFilter.Type == TableProviderFilterType.Nonfiltered)
                return oldFilter;

            if (oldFilter.Type == TableProviderFilterType.Exact)
                return CombineExact(oldFilter, newFilter);
            else if (newFilter.Type == TableProviderFilterType.Exact)
                return CombineExact(newFilter, oldFilter);

            var combined = new TableProviderFilter();
            combined.ColumnIndex = oldFilter.ColumnIndex;

            var oldHasMin = oldFilter.Type == TableProviderFilterType.Minimum || oldFilter.Type == TableProviderFilterType.Range;
            var oldHasMax = oldFilter.Type == TableProviderFilterType.Maximum || oldFilter.Type == TableProviderFilterType.Range;
            var newHasMin = newFilter.Type == TableProviderFilterType.Minimum || newFilter.Type == TableProviderFilterType.Range;
            var newHasMax = newFilter.Type == TableProviderFilterType.Maximum || newFilter.Type == TableProviderFilterType.Range;
            var cmbHasMin = oldHasMin || newHasMin;
            var cmbHasMax = oldHasMax || newHasMax;

            if (oldHasMin)
            {
                if (newHasMin)
                    combined.MinValue = Minimum(oldFilter.MinValue, newFilter.MinValue);
                else
                    combined.MinValue = oldFilter.MinValue;
            }
            else if (newHasMin)
                combined.MinValue = newFilter.MinValue;

            if (oldHasMax)
            {
                if (newHasMax)
                    combined.MaxValue = Maximum(oldFilter.MaxValue, newFilter.MaxValue);
                else
                    combined.MaxValue = oldFilter.MaxValue;
            }
            else if (newHasMax)
                combined.MaxValue = newFilter.MaxValue;

            if (cmbHasMin)
                combined.Type = cmbHasMax ? TableProviderFilterType.Range : TableProviderFilterType.Minimum;
            else if (cmbHasMax)
                combined.Type = TableProviderFilterType.Maximum;
            else
                throw new InvalidOperationException("WTF!?");

            return combined;
        }

        private static TableProviderFilter CombineExact(TableProviderFilter ex, TableProviderFilter oth)
        {
            if (oth.Type == TableProviderFilterType.Exact)
                return AreEqual(ex.MinValue, oth.MinValue) ? ex : null;
            else if (oth.Type == TableProviderFilterType.Minimum)
                return IsAtLeast(ex.MinValue, oth.MinValue) ? ex : null;
            else if (oth.Type == TableProviderFilterType.Maximum)
                return IsAtMost(ex.MinValue, oth.MaxValue) ? ex : null;
            else
                return IsAtLeast(ex.MinValue, oth.MinValue) && IsAtMost(ex.MinValue, oth.MaxValue) ? ex : null;
        }

        private static bool AreEqual(object a, object b)
        {
            return a != null && a.Equals(b);
        }

        private static bool IsAtLeast(object a, object b)
        {
            if (a == null)
                return false;
            if (a.Equals(b))
                return true;
            var ca = a as IComparable;
            if (ca != null)
                return ca.CompareTo(b) >= 0;
            else
                return a.GetHashCode() >= b.GetHashCode();
        }

        private static bool IsAtMost(object a, object b)
        {
            if (a == null)
                return false;
            if (a.Equals(b))
                return true;
            var ca = a as IComparable;
            if (ca != null)
                return ca.CompareTo(b) <= 0;
            else
                return a.GetHashCode() <= b.GetHashCode();
        }

        private static object Minimum(object a, object b)
        {
            return IsAtLeast(a, b) ? b : a;
        }

        private static object Maximum(object a, object b)
        {
            return IsAtLeast(a, b) ? a : b;
        }

        private static TableProviderFilter CreateNewFilter(object exactly, object minimum, object maximum, int column)
        {
            var newFilter = new TableProviderFilter();
            newFilter.ColumnIndex = column;

            if (exactly != null)
            {
                newFilter.Type = TableProviderFilterType.Exact;
                newFilter.MinValue = exactly;
                newFilter.MaxValue = exactly;
            }
            else if (minimum != null)
            {
                if (maximum != null)
                {
                    newFilter.Type = TableProviderFilterType.Range;
                    newFilter.MinValue = minimum;
                    newFilter.MaxValue = maximum;
                }
                else
                {
                    newFilter.Type = TableProviderFilterType.Minimum;
                    newFilter.MinValue = minimum;
                }
            }
            else if (maximum != null)
            {
                newFilter.Type = TableProviderFilterType.Maximum;
                newFilter.MaxValue = maximum;
            }
            else
            {
                newFilter.Type = TableProviderFilterType.Nonfiltered;
            }
            return newFilter;
        }

        public TableProviderFilterableColumn Where(string name)
        {
            return new TableProviderFilterableColumn(this, name);
        }

        public TableProviderFilterableColumn And(string name)
        {
            return new TableProviderFilterableColumn(this, name);
        }

        private TableProviderFilter[] GenerateFilter()
        {
            return _filters.Values.Where(f => f.Type != TableProviderFilterType.Nonfiltered).ToArray();
        }

        public IEnumerator<TableProviderRow> GetEnumerator()
        {
            IList<TableProviderRow> list;
            if (_noResults)
                list = new TableProviderRow[0];
            else
                list = _table.GetRows(GenerateFilter());
            return list.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public class TableProviderFilterableColumn
    {
        private TableProviderFilterable _filterable;
        private string _name;

        public TableProviderFilterableColumn(TableProviderFilterable filterable, string name)
        {
            this._filterable = filterable;
            this._name = name;
        }

        public TableProviderFilterable Is(object value)
        {
            return _filterable.SetFilter(_name, value, null, null);
        }
        public TableProviderFilterable IsAtLeast(object value)
        {
            return _filterable.SetFilter(_name, null, value, null);
        }
        public TableProviderFilterable IsAtMost(object value)
        {
            return _filterable.SetFilter(_name, null, null, value);
        }
        public TableProviderFilterable IsInRange(object min, object max)
        {
            return _filterable.SetFilter(_name, null, min, max);
        }
    }
}
