using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;

namespace CqrsFramework
{
    public interface IEvent
    {
    }

    public interface ICommand
    {
    }

    public interface IEventMessageFactory
    {
        Message CreateMessage(IEvent @event, object context);
    }

    public class Message
    {
        public MessageHeaders Headers { get; private set; }
        public object Payload { get; private set; }

        public Message(object payload)
        {
            this.Payload = payload;
            this.Headers = new MessageHeaders();
        }
    }

    public class MessageHeaders : IEnumerable<MessageHeader>
    {
        private Dictionary<string, string> _headers = new Dictionary<string, string>();

        public string this[string name]
        {
            get { return GetHeader(name); }
            set { SetHeader(name, value); }
        }

        public Guid MessageId { get; set; }
        public Guid CorellationId { get; set; }
        public DateTime CreatedOn { get; set; }
        public DateTime DeliverOn { get; set; }
        public DateTime ValidUntil { get; set; }
        public int RetryNumber { get; set; }
        public string ResourcePath { get; set; }
        public string TypePath { get; set; }
        public string PayloadFormat { get; set; }
        public int PayloadLength { get; set; }
        public string PayloadType { get; set; }

        private void SetHeader(string name, string value)
        {
            switch (name)
            {
                case "MessageId":
                    this.MessageId = TryParseGuid(value);
                    break;
                case "CorellationId":
                    this.CorellationId = TryParseGuid(value);
                    break;
                case "CreatedOn":
                    this.CreatedOn = TryParseDate(value);
                    break;
                case "DeliverOn":
                    this.DeliverOn = TryParseDate(value);
                    break;
                case "ValidUntil":
                    this.ValidUntil = TryParseDate(value);
                    break;
                case "RetryNumber":
                    this.RetryNumber = TryParseInt(value);
                    break;
                case "ResourcePath":
                    this.ResourcePath = value;
                    break;
                case "TypePath":
                    this.TypePath = value;
                    break;
                case "PayloadFormat":
                    this.PayloadFormat = value;
                    break;
                case "PayloadLength":
                    this.PayloadLength = TryParseInt(value);
                    break;
                case "PayloadType":
                    this.PayloadType = value;
                    break;
                default:
                    _headers[name] = value;
                    break;
            }
        }

        private static DateTime TryParseDate(string value)
        {
            DateTime dateTime;
            DateTime.TryParseExact(value, "yyyy-MM-dd HH:mm:ss.ffff", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out dateTime);
            return dateTime;
        }

        private static Guid TryParseGuid(string value)
        {
            return string.IsNullOrEmpty(value) ? Guid.Empty : new Guid(value);
        }

        private static int TryParseInt(string value)
        {
            return string.IsNullOrEmpty(value) ? 0 : int.Parse(value);
        }

        private string GetHeader(string name)
        {
            string value;
            switch (name)
            {
                case "MessageId":
                    return this.MessageId.ToString("D");
                case "CorellationId":
                    return this.CorellationId.ToString("D");
                case "CreatedOn":
                    return this.CreatedOn.ToString("yyyy-MM-dd HH:mm:ss.ffff");
                case "DeliverOn":
                    return this.DeliverOn.ToString("yyyy-MM-dd HH:mm:ss.ffff");
                case "ValidUntil":
                    return this.ValidUntil.ToString("yyyy-MM-dd HH:mm:ss.ffff");
                case "RetryNumber":
                    return this.RetryNumber.ToString();
                case "ResourcePath":
                    return this.ResourcePath;
                case "TypePath":
                    return this.TypePath;
                case "PayloadFormat":
                    return this.PayloadFormat;
                case "PayloadLength":
                    return this.PayloadLength.ToString();
                case "PayloadType":
                    return this.PayloadType;
                default:
                    _headers.TryGetValue(name, out value);
                    return value;
            }
        }

        public IEnumerator<MessageHeader> GetEnumerator()
        {
            var list = new List<MessageHeader>();
            AddNamedToList(list, MessageId != Guid.Empty, "MessageId", false);
            AddNamedToList(list, CorellationId != Guid.Empty, "CorellationId", true);
            AddNamedToList(list, CreatedOn != DateTime.MinValue, "CreatedOn", false);
            AddNamedToList(list, DeliverOn != DateTime.MinValue, "DeliverOn", false);
            AddNamedToList(list, ValidUntil != DateTime.MinValue, "ValidUntil", false);
            AddNamedToList(list, RetryNumber != 0, "RetryNumber", false);
            AddNamedToList(list, true, "ResourcePath", false);
            AddNamedToList(list, true, "TypePath", false);
            AddNamedToList(list, true, "PayloadFormat", false);
            AddNamedToList(list, PayloadLength != 0, "PayloadLength", false);
            AddNamedToList(list, true, "PayloadType", false);
            foreach (var item in _headers)
                if (!string.IsNullOrEmpty(item.Value))
                    list.Add(new MessageHeader(item.Key, item.Value, true));
            return list.GetEnumerator();
        }

        private void AddNamedToList(List<MessageHeader> list, bool allowAdd, string name, bool copy)
        {
            if (!allowAdd)
                return;
            var value = GetHeader(name);
            if (string.IsNullOrEmpty(value))
                return;
            list.Add(new MessageHeader(name, value, copy));
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void CopyFrom(MessageHeaders headers)
        {
            MessageId = headers.MessageId;
            CorellationId = headers.CorellationId;
            CreatedOn = headers.CreatedOn;
            DeliverOn = headers.DeliverOn;
            ValidUntil = headers.ValidUntil;
            RetryNumber = headers.RetryNumber;
            ResourcePath = headers.ResourcePath;
            TypePath = headers.TypePath;
            PayloadFormat = headers.PayloadFormat;
            PayloadLength = headers.PayloadLength;
            PayloadType = headers.PayloadType;
            foreach (var header in headers._headers)
                _headers[header.Key] = header.Value;
        }

    }

    public class MessageHeader
    {
        public readonly string Name;
        public readonly string Value;
        public readonly bool CopyToEvent;

        public MessageHeader(string name, string value, bool copy)
        {
            Name = name;
            Value = value;
            CopyToEvent = copy;
        }
    }
}
