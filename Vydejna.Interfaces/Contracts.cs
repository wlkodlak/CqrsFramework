using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vydejna.Interfaces
{
    [ProtoContract]
    public class MessageHeader
    {
        [ProtoMember(1)]
        public string Name { get; set; }
        [ProtoMember(2)]
        public string Value { get; set; }
    }
    public class MessageHeaders : IEnumerable<MessageHeader>
    {
        private Dictionary<string, string> _data;

        public MessageHeaders(IEnumerable<MessageHeader> original)
        {
            _data = new Dictionary<string, string>();
            if (original != null)
            {
                foreach (var item in original)
                    _data[item.Name] = item.Value;
            }
        }

        public string this[string name]
        {
            get
            {
                string value;
                _data.TryGetValue(name, out value);
                return value;
            }
            set
            {
                _data[name] = value;
            }
        }

        private List<MessageHeader> BuildList()
        {
            var result = new List<MessageHeader>(_data.Count);
            foreach (var pair in _data)
                result.Add(new MessageHeader { Name = pair.Key, Value = pair.Value });
            return result;
        }

        public MessageHeader[] ToArray()
        {
            return BuildList().ToArray();
        }

        public IEnumerator<MessageHeader> GetEnumerator()
        {
            return BuildList().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
    public interface ICommand
    {
        MessageHeaders Headers { get; }
    }
    public interface IEvent
    {
        MessageHeaders Headers { get; }
    }
    [ProtoContract]
    public class CommandBase : ICommand
    {
        private MessageHeaders _headers;

        public CommandBase()
        {
            _headers = new MessageHeaders(null);
        }

        public MessageHeaders Headers { get { return _headers; } }

        [ProtoMember(1023)]
        public MessageHeader[] RawHeaders
        {
            get { return _headers.ToArray(); }
            set { _headers = new MessageHeaders(value); }
        }
    }
    [ProtoContract]
    public class EventBase : IEvent
    {
        private MessageHeaders _headers;

        public EventBase()
        {
            _headers = new MessageHeaders(null);
        }

        public MessageHeaders Headers { get { return _headers; } }

        [ProtoMember(1023)]
        public MessageHeader[] RawHeaders
        {
            get { return _headers.ToArray(); }
            set { _headers = new MessageHeaders(value); }
        }
    }

}
