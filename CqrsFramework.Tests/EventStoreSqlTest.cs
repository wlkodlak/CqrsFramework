using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.EventStore.InSql;
using System.Data;
using System.Linq;
using Moq;
using System.Collections.Generic;

namespace CqrsFramework.Tests
{
    [TestClass]
    public class EventStoreSqlTest : EventStoreTestBase
    {
        protected override IEventStoreTestBuilder CreateBuilder()
        {
            return new Builder();
        }

        private class Builder : IEventStoreTestBuilder
        {
            private DataSet _dataset;
            private DataTable _tableStreams;
            private DataTable _tableEvents;

            public Builder()
            {
                _tableStreams = new DataTable("es_streams");
                _tableStreams.Columns.Add("name", typeof(string));
                _tableStreams.Columns.Add("version", typeof(int));
                _tableStreams.Columns.Add("snapshotversion", typeof(int));
                _tableStreams.Columns.Add("snapshot", typeof(byte[]));
                _tableEvents = new DataTable("es_events");
                _tableEvents.Columns.Add("name", typeof(string));
                _tableEvents.Columns.Add("version", typeof(int));
                _tableEvents.Columns.Add("clock", typeof(int));
                _tableEvents.Columns.Add("data", typeof(byte[]));
                _tableEvents.Columns.Add("published", typeof(int));
                _dataset = new DataSet();
                _dataset.Tables.Add(_tableStreams);
                _dataset.Tables.Add(_tableEvents);
            }

            public IEventStore Build()
            {
                return new SqlEventStore(GetConnection);
            }

            private IDbConnection GetConnection()
            {
                var mock = new Mock<IDbConnection>();
                mock.Setup(c => c.CreateCommand()).Returns(() => new DataCommand(_dataset, mock.Object));
                mock.Setup(c => c.Dispose());
                return mock.Object;
            }

            public void WithStream(string name, EventStoreSnapshot snapshot, EventStoreEvent[] events)
            {
                int version = 0;
                if (snapshot != null)
                    version = snapshot.Version;
                if (events.Length > 0)
                    version = events.Max(e => e.Version);

                var rowStream = _tableStreams.NewRow();
                rowStream.SetField<string>("name", name);
                rowStream.SetField<int>("version", version);
                rowStream.SetField<int>("snapshotversion", snapshot == null ? 0 : snapshot.Version);
                rowStream.SetField<byte[]>("snapshot", snapshot == null ? null : snapshot.Data);
                _tableStreams.Rows.Add(rowStream);

                foreach (var @event in events)
                {
                    var rowEvent = _tableEvents.NewRow();
                    rowEvent.SetField<string>("name", name);
                    rowEvent.SetField<int>("version", @event.Version);
                    rowEvent.SetField<byte[]>("data", @event.Data);
                    rowEvent.SetField<int>("published", @event.Published ? 1 : 0);
                    rowEvent.SetField<int>("clock", (int)@event.Clock);
                    _tableEvents.Rows.Add(rowEvent);
                }

                _dataset.AcceptChanges();
            }
        }

        private class DataParameter : IDbDataParameter
        {
            public bool IsNullable { get { return true; } }
            public DbType DbType { get; set; }
            public ParameterDirection Direction { get; set; }
            public string ParameterName { get; set; }
            public string SourceColumn { get; set; }
            public DataRowVersion SourceVersion { get; set; }
            public object Value { get; set; }
            public byte Precision { get; set; }
            public byte Scale { get; set; }
            public int Size { get; set; }
        }

        private class DataParameterList : List<DataParameter>, IDataParameterCollection
        {
            private DataParameter FindByName(string parameterName)
            {
                return this.FirstOrDefault(d => d.ParameterName == parameterName);
            }

            public bool Contains(string parameterName)
            {
                return FindByName(parameterName) != null;
            }

            public int IndexOf(string parameterName)
            {
                return IndexOf(FindByName(parameterName));
            }

            public void RemoveAt(string parameterName)
            {
                var found = FindByName(parameterName);
                if (found != null)
                    Remove(found);
            }

            public object this[string parameterName]
            {
                get { return FindByName(parameterName).Value; }
                set { FindByName(parameterName).Value = value; }
            }
        }

        private class DataReader : IDataReader
        {
            private DataView _view;
            private DataRow _current;
            private System.Collections.IEnumerator _enumerator;

            public DataReader(DataView view)
            {
                _view = view;
                _enumerator = _view.GetEnumerator();
            }

            public void Close()
            {
            }

            public int Depth { get { return 0; } }

            public DataTable GetSchemaTable()
            {
                throw new NotImplementedException();
            }

            public bool IsClosed { get { return false; } }

            public bool NextResult()
            {
                return false;
            }

            public bool Read()
            {
                if (_enumerator.MoveNext())
                {
                    var rowView = (DataRowView)_enumerator.Current;
                    _current = rowView.Row;
                    return true;
                }
                else
                {
                    _current = null;
                    return false;
                }
            }

            public int RecordsAffected
            {
                get { return 0; }
            }

            public void Dispose()
            {
            }

            public int FieldCount
            {
                get { return _view.Table.Columns.Count; }
            }

            public bool GetBoolean(int i)
            {
                return (bool)_current[i];
            }

            public byte GetByte(int i)
            {
                return (byte)_current[i];
            }

            public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public char GetChar(int i)
            {
                throw new NotImplementedException();
            }

            public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
            {
                throw new NotImplementedException();
            }

            public IDataReader GetData(int i)
            {
                throw new NotImplementedException();
            }

            public string GetDataTypeName(int i)
            {
                throw new NotImplementedException();
            }

            public DateTime GetDateTime(int i)
            {
                return (DateTime)_current[i];
            }

            public decimal GetDecimal(int i)
            {
                throw new NotImplementedException();
            }

            public double GetDouble(int i)
            {
                throw new NotImplementedException();
            }

            public Type GetFieldType(int i)
            {
                return _view.Table.Columns[i].DataType;
            }

            public float GetFloat(int i)
            {
                throw new NotImplementedException();
            }

            public Guid GetGuid(int i)
            {
                return (Guid)_current[i];
            }

            public short GetInt16(int i)
            {
                return (short)_current[i];
            }

            public int GetInt32(int i)
            {
                return (int)_current[i];
            }

            public long GetInt64(int i)
            {
                return (long)_current[i];
            }

            public string GetName(int i)
            {
                return _view.Table.Columns[i].ColumnName;
            }

            public int GetOrdinal(string name)
            {
                return _view.Table.Columns[name].Ordinal;
            }

            public string GetString(int i)
            {
                return (string)_current[i];
            }

            public object GetValue(int i)
            {
                return _current[i];
            }

            public int GetValues(object[] values)
            {
                Array.Copy(_current.ItemArray, values, values.Length);
                return values.Length;
            }

            public bool IsDBNull(int i)
            {
                return _current.IsNull(i);
            }

            public object this[string name]
            {
                get { return _current[name]; }
            }

            public object this[int i]
            {
                get { return _current[i]; }
            }
        }

        private class DataCommand : IDbCommand
        {
            private DataSet _dataset;
            private string _commandText;
            private IDbConnection _connection;
            private DataParameterList _parameters;

            public DataCommand(DataSet dataset, IDbConnection conn)
            {
                this._dataset = dataset;
                this._connection = conn;
                this._commandText = "";
                this._parameters = new DataParameterList();
            }

            public void Cancel()
            {
            }

            public string CommandText
            {
                get { return _commandText; }
                set { _commandText = value; }
            }

            public int CommandTimeout
            {
                get { return 0; }
                set { }
            }

            public CommandType CommandType
            {
                get { return System.Data.CommandType.Text; }
                set { throw new NotImplementedException(); }
            }

            public IDbConnection Connection
            {
                get { return _connection; }
                set { throw new NotImplementedException(); }
            }

            public IDbDataParameter CreateParameter()
            {
                return new DataParameter();
            }

            public int ExecuteNonQuery()
            {
                return 0;
            }

            public IDataReader ExecuteReader(CommandBehavior behavior)
            {
                throw new NotImplementedException();
            }

            public IDataReader ExecuteReader()
            {
                if (_commandText == "SELECT * FROM es_events WHERE published = 0")
                    return new DataReader(new DataView(_dataset.Tables["es_events"], "published = 0", "", DataViewRowState.CurrentRows));
                else if (_commandText == "SELECT * FROM es_events WHERE clock >= :clock ORDER BY clock")
                    return new DataReader(new DataView(
                        _dataset.Tables["es_events"],
                        string.Format("clock >= {0}", _parameters["clock"]), 
                        "clock ASC", 
                        DataViewRowState.CurrentRows));
                return null;
            }

            public object ExecuteScalar()
            {
                throw new NotImplementedException();
            }

            public IDataParameterCollection Parameters
            {
                get { return _parameters; }
            }

            public void Prepare()
            {
                throw new NotImplementedException();
            }

            public IDbTransaction Transaction
            {
                get { return null; }
                set { throw new NotImplementedException(); }
            }

            public UpdateRowSource UpdatedRowSource
            {
                get { return UpdateRowSource.None; }
                set { throw new NotImplementedException(); }
            }

            public void Dispose()
            {
            }
        }
    }
}
