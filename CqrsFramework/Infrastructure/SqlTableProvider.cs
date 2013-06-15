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

        public virtual bool CreatesRowIdInAdvance()
        {
            return true;
        }

        protected virtual int GetNextRowId(IDbConnection conn)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = string.Format("SELECT MAX(id) FROM {0}", _tableName);
                var maxId = cmd.ExecuteScalar();
                if (maxId is int)
                    return (int)maxId + 1;
            }
            return 1;
        }

        protected virtual int GetInsertedRowId(IDbConnection conn)
        {
            throw new InvalidOperationException("This gets Row ID in advance");
        }

        protected virtual string GetParameterPlaceholder(string name)
        {
            return string.Concat("@", name);
        }

        public void Insert(TableProviderRow row)
        {
            using (var conn = _connectionFactory())
            {
                int newRowId = CreatesRowIdInAdvance() ? GetNextRowId(conn) : -1;
                using (var cmd = conn.CreateCommand())
                {
                    bool isFirstColumn = true;
                    var sql1 = new StringBuilder();
                    var sql2 = new StringBuilder();
                    sql1.AppendFormat("INSERT INTO {0} (", _tableName);
                    sql2.Append(") VALUES (");
                    if (newRowId >= 0)
                    {
                        sql1.Append("id");
                        sql2.Append(GetParameterPlaceholder("id"));
                        AddParameter(cmd, "id", typeof(int), newRowId);
                        isFirstColumn = false;
                    }
                    foreach (var column in _columns)
                    {
                        var comma = isFirstColumn ? "" : ", ";
                        sql1.AppendFormat("{0}{1}", comma, column.Name);
                        sql2.AppendFormat("{0}{1}", comma, GetParameterPlaceholder(column.Name));
                        AddParameter(cmd, column.Name, column.Type, row[column.Ordinal]);
                    }
                    sql1.Append(sql2);
                    sql1.Append(")");
                    cmd.CommandText = sql1.ToString();
                    cmd.ExecuteNonQuery();
                    if (newRowId >= 0)
                        row.RowNumber = newRowId;
                    else
                        row.RowNumber = GetInsertedRowId(conn);
                }
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
                    sql.AppendFormat("{0} = {1}", column.Name, GetParameterPlaceholder(column.Name));
                    AddParameter(cmd, column.Name, column.Type, row[column.Ordinal]);
                }
                sql.AppendFormat(" WHERE id = {0}", GetParameterPlaceholder("id"));
                AddParameter(cmd, "id", typeof(int), row.RowNumber);
                cmd.CommandText = sql.ToString();
                cmd.ExecuteNonQuery();
            }
        }

        public void Delete(TableProviderRow row)
        {
            using (var conn = _connectionFactory())
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = string.Format("DELETE FROM {0} WHERE id = {1}", _tableName, GetParameterPlaceholder("id"));
                AddParameter(cmd, "id", typeof(int), row.RowNumber);
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
                        sql.AppendFormat("{0} = {1}", column.Name, GetParameterPlaceholder(column.Name));
                        AddParameter(cmd, column.Name, column.Type, elem.MinValue);
                        break;
                    case TableProviderFilterType.Minimum:
                        sql.AppendFormat("{0} >= {1}", column.Name, GetParameterPlaceholder(column.Name));
                        AddParameter(cmd, column.Name, column.Type, elem.MinValue);
                        break;
                    case TableProviderFilterType.Maximum:
                        sql.AppendFormat("{0} <= {1}", column.Name, GetParameterPlaceholder(column.Name));
                        AddParameter(cmd, column.Name, column.Type, elem.MaxValue);
                        break;
                    case TableProviderFilterType.Range:
                        sql.AppendFormat("{0} >= {1} AND {0} <= {2}", 
                            column.Name, 
                            GetParameterPlaceholder("min" + column.Name),
                            GetParameterPlaceholder("max" + column.Name));
                        AddParameter(cmd, "min" + column.Name, column.Type, elem.MinValue);
                        AddParameter(cmd, "max" + column.Name, column.Type, elem.MaxValue);
                        break;
                }
            }

            cmd.CommandText = sql.ToString();
        }

        protected virtual string ParameterName(string name)
        {
            return string.Concat("@", name);
        }

        protected virtual DbType MapParametererType(Type type)
        {
            if (type == typeof(int))
                return DbType.Int32;
            else if (type == typeof(decimal))
                return DbType.Decimal;
            else if (type == typeof(string))
                return DbType.String;
            else if (type == typeof(DateTime))
                return DbType.DateTime;
            else if (type == typeof(Guid))
                return DbType.Guid;
            else if (type == typeof(byte[]))
                return DbType.Binary;
            else
                throw UnsupportedColumnTypeException(type);
        }

        private Exception UnsupportedColumnTypeException(Type type)
        {
            return new ArgumentOutOfRangeException(string.Format("Type {0} is not supported by this TableProvider", type.Name));
        }

        protected virtual object MapParameterValue(Type type, object value)
        {
            return value;
        }

        private void AddParameter(IDbCommand cmd, string name, Type type, object value)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = ParameterName(name);
            param.Value = MapParameterValue(type, value);
            param.DbType = MapParametererType(type);
            cmd.Parameters.Add(param);
        }

        protected virtual object ReadValue(IDataReader reader, Type type, int ordinal)
        {
            if (reader.IsDBNull(ordinal))
                return null;
            else if (type == typeof(int))
                return reader.GetInt32(ordinal);
            else if (type == typeof(string))
                return reader.GetString(ordinal);
            else if (type == typeof(DateTime))
                return reader.GetDateTime(ordinal);
            else if (type == typeof(decimal))
                return reader.GetDecimal(ordinal);
            else if (type == typeof(Guid))
                return reader.GetGuid(ordinal);
            else if (type == typeof(byte[]))
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
                return stream.ToArray();
            }
            else
                throw UnsupportedColumnTypeException(type);
        }

        private List<TableProviderRow> BuildRowsFromReader(IDataReader reader)
        {
            var result = new List<TableProviderRow>();
            while (reader.Read())
            {
                var rowNumber = reader.GetInt32(0);
                var data = new object[_columns.Count];
                foreach (var column in _columns)
                    data[column.Ordinal - 1] = ReadValue(reader, column.Type, column.Ordinal);
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

        public virtual int GetMaxRowNumber()
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
