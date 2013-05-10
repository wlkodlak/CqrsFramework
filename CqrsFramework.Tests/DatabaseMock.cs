using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Moq;

namespace CqrsFramework.Tests
{
    public class DatabaseMock
    {
        private Dictionary<string, RegisteredCommand> _commands = new Dictionary<string, RegisteredCommand>();

        public class ParametersCollection : List<IDbDataParameter>, IDataParameterCollection
        {
            private IDbDataParameter FindParameter(string name)
            {
                return this.FirstOrDefault(p => p.ParameterName == name);
            }

            public bool Contains(string parameterName)
            {
                return FindParameter(parameterName) != null;
            }

            public int IndexOf(string parameterName)
            {
                var param = FindParameter(parameterName);
                return (param == null) ? -1 : IndexOf(param);
            }

            public void RemoveAt(string parameterName)
            {
                var param = FindParameter(parameterName);
                Remove(param);
            }

            public object this[string parameterName]
            {
                get
                {
                    var param = FindParameter(parameterName);
                    return (param == null) ? null : param.Value;
                }
                set
                {
                    var param = FindParameter(parameterName);
                    if (param != null)
                        param.Value = value;
                }
            }

            public T Get<T>(string name)
            {
                var param = FindParameter(name);
                if (param == null)
                    return default(T);
                else if (param.Value is DBNull)
                    return default(T);
                else
                    return (T)param.Value;
            }
        }

        private class RegisteredCommand
        {
            public bool IsSelect;
            public string Sql;
            public Func<DataRow, ParametersCollection, bool> Filter;
            public string Sort;
            public Action<ParametersCollection, DataTable> Action;
            public DataTable Table;
            public string Aggregate;
            public string AggregatedColumn;

            public int ExecuteNonQuery(ParametersCollection parameters)
            {
                Action(parameters, Table);
                return 1;
            }

            public DataTable ExecuteTable(ParametersCollection parameters)
            {
                var enumerable = Table.AsEnumerable();
                if (Filter != null)
                    enumerable = enumerable.Where(r => Filter(r, parameters));
                if (!string.IsNullOrEmpty(Sort))
                    enumerable = enumerable.OrderBy(r => r[Sort]);
                if (Aggregate == null)
                {
                    var result = Table.Clone();
                    foreach (var row in enumerable)
                    {
                        var newRow = result.NewRow();
                        newRow.ItemArray = row.ItemArray;
                        result.Rows.Add(newRow);
                    }
                    result.AcceptChanges();
                    return result;
                }
                else if (Aggregate == "MAX")
                {
                    var result = new DataTable();
                    var type = Table.Columns[AggregatedColumn].DataType;
                    result.Columns.Add(AggregatedColumn, type);
                    object value = null;
                    if (type == typeof(int))
                        value = enumerable.Max(r => r.Field<int?>(AggregatedColumn)) ?? (object)null;
                    else if (type == typeof(decimal))
                        value = enumerable.Max(r => r.Field<decimal?>(AggregatedColumn)) ?? (object)null;
                    else if (type == typeof(string))
                        value = enumerable.Max(r => r.Field<string>(AggregatedColumn)) ?? (object)null;
                    else
                        throw new DataException("Unsupported aggregate type");
                    if (value != null)
                    {
                        var newRow = result.NewRow();
                        newRow[0] = value;
                        result.Rows.Add(newRow);
                    }
                    return result;
                }
                else
                    throw new DataException("Unsupported aggregate");
            }
        }

        public IDbConnection CreateConnection()
        {
            var mockConnection = new Mock<IDbConnection>();
            var mockTransaction = new Mock<IDbTransaction>();
            mockTransaction.SetupAllProperties();
            mockTransaction.Setup(t => t.Commit());
            mockTransaction.Setup(t => t.Dispose());

            mockConnection.Setup(c => c.State).Returns(ConnectionState.Open);
            mockConnection.SetupAllProperties();
            mockConnection.Setup(c => c.CreateCommand()).Returns(() => new DatabaseCommand(this, mockConnection.Object));
            mockConnection.Setup(c => c.BeginTransaction()).Returns(mockTransaction.Object);
            return mockConnection.Object;
        }

        public void SelectCommand(string sql, Func<DataRow, ParametersCollection, bool> filter, string sort, DataTable table)
        {
            var handler = new RegisteredCommand();
            handler.IsSelect = true;
            handler.Sql = sql;
            handler.Filter = filter;
            handler.Sort = sort;
            handler.Table = table;
            _commands[sql] = handler;
        }

        public void ModifyCommand(string sql, Action<ParametersCollection, DataTable> action, DataTable table)
        {
            var handler = new RegisteredCommand();
            handler.IsSelect = false;
            handler.Sql = sql;
            handler.Action = action;
            handler.Table = table;
            _commands[sql] = handler;
        }

        public void AggregateCommand(string sql, string aggregate, string column, Func<DataRow, ParametersCollection, bool> filter, DataTable table)
        {
            var handler = new RegisteredCommand();
            handler.IsSelect = true;
            handler.Sql = sql;
            handler.Filter = filter;
            handler.Aggregate = aggregate;
            handler.AggregatedColumn = column;
            handler.Table = table;
            _commands[sql] = handler;
        }

        private RegisteredCommand GetBySql(string sql)
        {
            return _commands[sql];
        }

        private class DatabaseCommand : IDbCommand
        {
            private ParametersCollection _parameters = new ParametersCollection();
            private DatabaseMock _dbMock;

            public DatabaseCommand(DatabaseMock mock, IDbConnection conn)
            {
                Connection = conn;
                _dbMock = mock;
            }
            public string CommandText { get; set; }
            public int CommandTimeout { get; set; }
            public CommandType CommandType { get; set; }
            public IDbConnection Connection { get; set; }
            public IDataParameterCollection Parameters { get { return _parameters; } }
            public IDbTransaction Transaction { get; set; }
            public UpdateRowSource UpdatedRowSource { get; set; }
            public void Cancel() { }
            public void Dispose() { }
            public IDbDataParameter CreateParameter()
            {
                var mock = new Mock<IDbDataParameter>();
                mock.SetupAllProperties();
                mock.Setup(p => p.IsNullable).Returns(true);
                return mock.Object;
            }
            public void Prepare() { }
            public int ExecuteNonQuery()
            {
                RegisteredCommand handler = _dbMock.GetBySql(CommandText);
                return handler.ExecuteNonQuery(_parameters);
            }
            public IDataReader ExecuteReader()
            {
                RegisteredCommand handler = _dbMock.GetBySql(CommandText);
                DataTable table = handler.ExecuteTable(_parameters);
                return new DataTableReader(table);
            }
            public IDataReader ExecuteReader(CommandBehavior behavior)
            {
                return ExecuteReader();
            }
            public object ExecuteScalar()
            {
                RegisteredCommand handler = _dbMock.GetBySql(CommandText);
                DataTable table = handler.ExecuteTable(_parameters);
                return table.Rows[0][0];
            }
        }
    }
}
