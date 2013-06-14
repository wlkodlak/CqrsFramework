using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Infrastructure
{
    public class SqlTableProvider : ITableProvider
    {
        private Func<IDbConnection> _connectionFactory;
        private IList<TableProviderColumn> _columns;
        private string _tableName;
        private TableProviderColumn _idColumn;

        public SqlTableProvider(Func<IDbConnection> connectionFactory, string tableName, IList<TableProviderColumn> columns)
        {
            _connectionFactory = connectionFactory;
            _tableName = tableName;
            _idColumn = new TableProviderColumn(0, "id", typeof(int), true);
            _columns = columns;
        }

        public void Insert(TableProviderRow row)
        {
            using (var conn = _connectionFactory())
            {
                int newRowId = 1;
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = string.Format("SELECT MAX(id) FROM {0}", _tableName);
                    var maxId = cmd.ExecuteScalar();
                    if (maxId is int)
                        newRowId = (int)maxId + 1;
                }
                using (var cmd = conn.CreateCommand())
                {
                    var sql1 = new StringBuilder();
                    var sql2 = new StringBuilder();
                    sql1.AppendFormat("INSERT INTO {0} (id", _tableName);
                    sql2.Append(") VALUES (@id");
                    AddParameter(cmd, "@id", typeof(int), newRowId);
                    foreach (var column in _columns)
                    {
                        sql1.AppendFormat(", {0}", column.Name);
                        sql2.AppendFormat(", @{0}", column.Name);
                        AddParameter(cmd, "@" + column.Name, column.Type, row[column.Ordinal]);
                    }
                    sql1.Append(sql2);
                    sql1.Append(")");
                    cmd.CommandText = sql1.ToString();
                    cmd.ExecuteNonQuery();
                }
                row.RowNumber = newRowId;
            }
        }

        public void Update(TableProviderRow row)
        {
            using (var conn = _connectionFactory())
            using (var cmd = conn.CreateCommand())
            {
                var sql = new StringBuilder();
                sql.AppendFormat("UPDATE {0} SET ", _tableName);
                bool first = true;
                foreach (var column in _columns)
                {
                    if (first)
                        first = false;
                    else
                        sql.Append(", ");
                    sql.AppendFormat("{0} = @{0}", column.Name);
                    AddParameter(cmd, "@" + column.Name, column.Type, row[column.Ordinal]);
                }
                sql.Append(" WHERE id = @id");
                AddParameter(cmd, "@id", typeof(int), row.RowNumber);
                cmd.CommandText = sql.ToString();
                cmd.ExecuteNonQuery();
            }
        }

        public void Delete(TableProviderRow row)
        {
            using (var conn = _connectionFactory())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = string.Format("DELETE FROM {0} WHERE id = @id", _tableName);
                AddParameter(cmd, "@id", typeof(int), row.RowNumber);
                cmd.ExecuteNonQuery();
            }
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
            using (var conn = _connectionFactory())
            using (var cmd = conn.CreateCommand())
            {
                BuildSelectCommand(cmd, filter ?? new TableProviderFilter[0]);
                using (var reader = cmd.ExecuteReader())
                    return BuildRowsFromReader(reader).ToArray();
            }
        }

        private void BuildSelectCommand(IDbCommand cmd, TableProviderFilter[] filter)
        {
            var sql = new StringBuilder();
            sql.Append("SELECT id");
            foreach (var column in _columns)
                sql.AppendFormat(", {0}", column.Name);
            sql.AppendFormat(" FROM {0}", _tableName);

            bool hasWhereClause = false;
            foreach (var elem in filter)
            {
                if (hasWhereClause)
                    sql.Append(" AND ");
                else
                {
                    sql.Append(" WHERE ");
                    hasWhereClause = true;
                }
                var column = (elem.ColumnIndex == 0) ? _idColumn : _columns[elem.ColumnIndex - 1];
                switch (elem.Type)
                {
                    case TableProviderFilterType.Exact:
                        sql.AppendFormat("{0} = @{0}", column.Name);
                        AddParameter(cmd, "@" + column.Name, column.Type, elem.MinValue);
                        break;
                    case TableProviderFilterType.Minimum:
                        sql.AppendFormat("{0} >= @{0}", column.Name);
                        AddParameter(cmd, "@" + column.Name, column.Type, elem.MinValue);
                        break;
                    case TableProviderFilterType.Maximum:
                        sql.AppendFormat("{0} <= @{0}", column.Name);
                        AddParameter(cmd, "@" + column.Name, column.Type, elem.MaxValue);
                        break;
                    case TableProviderFilterType.Range:
                        sql.AppendFormat("{0} >= @min{0} AND {0} <= @max{0}", column.Name);
                        AddParameter(cmd, "@min" + column.Name, column.Type, elem.MinValue);
                        AddParameter(cmd, "@max" + column.Name, column.Type, elem.MaxValue);
                        break;
                }
            }

            cmd.CommandText = sql.ToString();
        }

        private void AddParameter(IDbCommand cmd, string name, Type type, object value)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = name;
            param.Value = value;
            if (type == typeof(int))
                param.DbType = DbType.Int32;
            else if (type == typeof(decimal))
                param.DbType = DbType.Decimal;
            else if (type == typeof(string))
                param.DbType = DbType.String;
            else if (type == typeof(DateTime))
                param.DbType = DbType.DateTime;
            else if (type == typeof(Guid))
                param.DbType = DbType.Guid;
            else if (type == typeof(byte[]))
                param.DbType = DbType.Binary;
            cmd.Parameters.Add(param);
        }

        private List<TableProviderRow> BuildRowsFromReader(IDataReader reader)
        {
            var result = new List<TableProviderRow>();
            while (reader.Read())
            {
                var rowNumber = reader.GetInt32(0);
                var data = new object[_columns.Count];
                foreach (var column in _columns)
                {
                    var ordinal = column.Ordinal;
                    if (reader.IsDBNull(ordinal))
                        data[ordinal - 1] = null;
                    else if (column.Type == typeof(int))
                        data[ordinal - 1] = reader.GetInt32(ordinal);
                    else if (column.Type == typeof(string))
                        data[ordinal - 1] = reader.GetString(ordinal);
                    else if (column.Type == typeof(DateTime))
                        data[ordinal - 1] = reader.GetDateTime(ordinal);
                    else if (column.Type == typeof(decimal))
                        data[ordinal - 1] = reader.GetDecimal(ordinal);
                    else if (column.Type == typeof(Guid))
                        data[ordinal - 1] = reader.GetGuid(ordinal);
                    else if (column.Type == typeof(byte[]))
                    {
                        var stream = new MemoryStream();
                        int bytesRead;
                        int offset = 0;
                        var buffer = new byte[4096];
                        while ((bytesRead = (int)reader.GetBytes(ordinal, offset, buffer, 0, 4096)) > 0)
                        {
                            stream.Write(buffer, 0, bytesRead);
                            offset += bytesRead;
                        }
                        data[ordinal - 1] = stream.ToArray();
                    }
                }
                result.Add(new TableProviderRow(this, rowNumber, data));
            }
            return result;
        }

        public TableProviderFilterable GetRows()
        {
            return new TableProviderFilterable(this);
        }

        public void Dispose()
        {
        }


        public long GetMaxRowNumber()
        {
            using (var conn = _connectionFactory())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = string.Format("SELECT MAX(id) FROM {0}", _tableName);
                    var maxId = cmd.ExecuteScalar();
                    return (maxId is int) ? (int)maxId : 0;
                }
            }
        }
    }
}
