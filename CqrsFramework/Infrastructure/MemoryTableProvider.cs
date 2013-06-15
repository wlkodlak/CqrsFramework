using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Infrastructure
{
    public class MemoryTableProvider : ITableProvider
    {
        private DataTable _table;
        private IList<TableProviderColumn> _columns;
        private int[] _mapping;
        private int _nextRowNumber = 1;

        public MemoryTableProvider(DataTable table, IList<TableProviderColumn> columns)
        {
            _table = table;
            if (columns == null)
            {
                _columns = new List<TableProviderColumn>(table.Columns.Count);
                _mapping = new int[table.Columns.Count];
                int columnIndex = 1;
                foreach (DataColumn dataColumn in _table.Columns)
                {
                    if (dataColumn.ColumnName == "id")
                        _mapping[0] = dataColumn.Ordinal;
                    else
                    {
                        var column = new TableProviderColumn(columnIndex, dataColumn.ColumnName, dataColumn.DataType, false);
                        _columns.Add(column);
                        _mapping[columnIndex] = dataColumn.Ordinal;
                        columnIndex++;
                    }
                }
            }
            else
            {
                _columns = columns;
                _mapping = new int[columns.Count + 1];
                _mapping[0] = _table.Columns["id"].Ordinal;
                foreach (var column in _columns)
                    _mapping[column.Ordinal] = _table.Columns[column.Name].Ordinal;
            }
            foreach (DataRow row in _table.Rows)
                _nextRowNumber = Math.Max(_nextRowNumber, row.Field<int>(_mapping[0]) + 1);
        }

        public void Insert(TableProviderRow row)
        {
            var dataRow = _table.NewRow();
            row.RowNumber = _nextRowNumber++;
            dataRow[_mapping[0]] = row.RowNumber;
            for (int i = 1; i <= _columns.Count; i++)
            {
                var value = row[i];
                dataRow[_mapping[i]] = value == null ? (object)DBNull.Value : value;
            }
            _table.Rows.Add(dataRow);
        }

        public void Update(TableProviderRow row)
        {
            var dataRow = _table.AsEnumerable().Single(r => r.Field<int>(_mapping[0]) == row.RowNumber);
            for (int i = 1; i <= _columns.Count; i++)
                dataRow[_mapping[i]] = row[i];
        }

        public void Delete(TableProviderRow row)
        {
            var dataRow = _table.AsEnumerable().Single(r => r.Field<int>(_mapping[0]) == row.RowNumber);
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
                    var rowNumber = dataRow.Field<int>(_mapping[0]);
                    var contents = new object[_columns.Count];
                    for (int i = 0; i < contents.Length; i++)
                    {
                        var dataValue = dataRow[_mapping[i + 1]];
                        contents[i] = dataValue is DBNull ? null : dataValue;
                    }
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

        public int GetMaxRowNumber()
        {
            return _nextRowNumber - 1;
        }
    }
}
