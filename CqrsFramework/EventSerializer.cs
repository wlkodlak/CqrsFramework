using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework
{
    public interface IEventSerializer
    {
        IEvent DeserializeEvent(EventStoreEvent stored);
        object DeserializeSnapshot(EventStoreSnapshot storedSnapshot);
        EventStoreEvent SerializeEvent(IEvent @event);
        EventStoreSnapshot SerializeSnapshot(object snapshot);
    }
}
