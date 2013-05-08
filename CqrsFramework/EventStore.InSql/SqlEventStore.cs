using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace CqrsFramework.EventStore.InSql
{
    public class SqlEventStore : IEventStore
    {
        private Func<IDbConnection> _getConnection;

        public SqlEventStore(Func<IDbConnection> getConnection)
        {
            _getConnection = getConnection;
        }

        public IEnumerable<EventStoreEvent> GetUnpublishedEvents()
        {
            var result = new List<EventStoreEvent>();
            using (var conn = _getConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT * FROM es_events WHERE published = 0";
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var @event = new EventStoreEvent();
                            @event.Clock = (int)reader["clock"];
                            @event.Data = (byte[])reader["data"];
                            @event.Key = (string)reader["name"];
                            @event.Published = (int)reader["published"] == 1;
                            @event.Version = (int)reader["version"];
                            result.Add(@event);
                        }
                    }
                }
            }
            return result;
        }

        public IEventStream GetStream(string name, EventStreamOpenMode mode)
        {
            throw new NotImplementedException();
        }

        public void MarkAsPublished(EventStoreEvent @event)
        {
        }

        public IEnumerable<EventStoreEvent> GetSince(long clock)
        {
            var result = new List<EventStoreEvent>();
            using (var conn = _getConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT * FROM es_events WHERE clock >= :clock ORDER BY clock";
                    var paramClock = cmd.CreateParameter();
                    paramClock.ParameterName = "clock";
                    paramClock.DbType = DbType.Int32;
                    paramClock.Value = (int)clock;
                    cmd.Parameters.Add(paramClock);
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var @event = new EventStoreEvent();
                            @event.Clock = (int)reader["clock"];
                            @event.Data = (byte[])reader["data"];
                            @event.Key = (string)reader["name"];
                            @event.Published = (int)reader["published"] == 1;
                            @event.Version = (int)reader["version"];
                            result.Add(@event);
                        }
                    }
                }
            }
            return result;
        }

        public void Dispose()
        {
        }
    }
}
