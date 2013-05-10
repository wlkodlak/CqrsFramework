using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.InMemory
{
    public class MemoryTableProvider : ITableProvider
    {
        private DataTable _table;
        private List<TableProviderColumn> _columns;
        private int _nextRowNumber = 1;

        public MemoryTableProvider(DataTable table)
        {
            _table = table;
            _columns = new List<TableProviderColumn>();
            for (int i = 0; i < _table.Columns.Count; i++)
            {
                var dataColumn = _table.Columns[i];
                _columns.Add(new TableProviderColumn(i, dataColumn.ColumnName, dataColumn.DataType, false));
            }
            foreach (DataRow row in _table.Rows)
                _nextRowNumber = Math.Max(_nextRowNumber, row.Field<int>(0) + 1);
        }

        public void Insert(TableProviderRow row)
        {
            var dataRow = _table.NewRow();
            dataRow[0] = row.RowNumber;
            for (int i = 1; i < _columns.Count; i++)
                dataRow[i] = row[i];
            _table.Rows.Add(dataRow);
        }

        public void Update(TableProviderRow row)
        {
            var dataRow = _table.AsEnumerable().Single(r => r.Field<int>(0) == row.RowNumber);
            for (int i = 1; i < _columns.Count; i++)
                dataRow[i] = row[i];
        }

        public void Delete(TableProviderRow row)
        {
            var dataRow = _table.AsEnumerable().Single(r => r.Field<int>(0) == row.RowNumber);
            _table.Rows.Remove(dataRow);
        }

        public TableProviderRow NewRow()
        {
            return new TableProviderRow(this, -1, new object[_columns.Count]);
        }

        public TableProviderColumn[] GetColumns()
        {
            return _columns.ToArray();
        }

        public TableProviderRow[] GetRows(TableProviderFilter[] filter)
        {
            var result = new List<TableProviderRow>();
            foreach (DataRow dataRow in _table.Rows)
            {
                bool satisfied = true;
                if (filter != null)
                {
                    foreach (var filterElement in filter)
                        satisfied = satisfied && Satisfied(dataRow, filterElement);
                }
                if (satisfied)
                {
                    var rowNumber = dataRow.Field<int>(0);
                    var contents = dataRow.ItemArray.Skip(1).ToArray();
                    var tableRow = new TableProviderRow(this, rowNumber, contents);
                    result.Add(tableRow);
                }
            }
            return result.ToArray();
        }

        private bool Satisfied(DataRow row, TableProviderFilter filter)
        {
            object value = row[filter.ColumnIndex];

            if (filter.MinValue == null && filter.MaxValue == null)
                return true;
            if (filter.MinValue != null && filter.MinValue.Equals(filter.MaxValue))
                return filter.MinValue.Equals(value);

            bool minimumSatisfied, maximumSatisfied;

            if (filter.MinValue == null)
                minimumSatisfied = true;
            else if (filter.MinValue is IComparable)
                minimumSatisfied = (filter.MinValue as IComparable).CompareTo(value) <= 0;
            else
                throw new InvalidOperationException();

            if (filter.MaxValue == null)
                maximumSatisfied = true;
            else if (filter.MaxValue is IComparable)
                maximumSatisfied = (filter.MaxValue as IComparable).CompareTo(value) >= 0;
            else
                throw new InvalidOperationException();

            return minimumSatisfied && maximumSatisfied;
        }


        public TableProviderFilterable GetRows()
        {
            return new TableProviderFilterable(this);
        }

        public void Dispose()
        {
            _table.Dispose();
        }
    }
}
