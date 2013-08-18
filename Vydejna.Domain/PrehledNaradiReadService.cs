using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Messaging;
using Vydejna.Contracts;
using CqrsFramework.KeyValueStore;
using CqrsFramework.Serialization;
using System.Runtime.Serialization;
using CqrsFramework.Domain;

namespace Vydejna.Domain
{
    [DataContract(Namespace = Serialization.Namespace)]
    public class PrehledNaradiView
    {
        [DataMember(Order = 0)]
        public byte[] ProjectionHash { get; set; }
        [DataMember(Order = 1)]
        public long ProjectionClock { get; set; }
        [DataMember(Order = 2)]
        public List<PouzivaneNaradiDto> SeznamNaradi { get; set; }
    }

    public class PrehledNaradiReadService : IPrehledNaradiReadService, IProjectionDispatcher
    {
        private static readonly byte[] _projectionHash = new byte[] { 0x00, 0x00, 0x00, 0x00 };
        private IKeyValueStore _store;
        private IMessageBodySerializer _serializer;
        private PrehledNaradiView _view;
        private bool _bulkUpdate = false;
        private Dictionary<Guid, PouzivaneNaradiDto> _indexPodleId;
        private MessageDispatcher _dispatcher;

        public PrehledNaradiReadService(IKeyValueStore store, IMessageBodySerializer serializer)
        {
            _store = store;
            _serializer = serializer;

            var doc = _store.Get("prehlednaradi");
            var headers = new MessageHeaders();
            headers.PayloadType = "PrehledNaradiDto";
            _view = _serializer.Deserialize(doc.Data, headers) as PrehledNaradiView;

            _indexPodleId = _view.SeznamNaradi.ToDictionary(n => n.Id);

            _dispatcher = new MessageDispatcher();
            _dispatcher.ThrowOnUnknownHandler = false;
            _dispatcher.Register<DefinovanoPouzivaneNaradiEvent>(Handle);
            _dispatcher.Register<UpravenPocetNaradiNaSkladeEvent>(Handle);
        }

        public SeznamPouzivanehoNaradiDto ZiskatSeznam(int offset, int pocet)
        {
            var dto = new SeznamPouzivanehoNaradiDto();
            dto.SeznamNaradi = new List<PouzivaneNaradiDto>(_view.SeznamNaradi.Skip(offset).Take(pocet));
            dto.OffsetPrvnihoPrvku = offset;
            dto.PocetVsechPrvku = _view.SeznamNaradi.Count;
            return dto;
        }

        public void BeginUpdate()
        {
            _bulkUpdate = true;
        }

        public void EndUpdate()
        {
            _bulkUpdate = false;
            SaveView();
        }

        public void Reset()
        {
            _view.SeznamNaradi.Clear();
            _view.ProjectionClock = 0;
            _view.ProjectionHash = _projectionHash;
            if (!_bulkUpdate)
                SaveView();
        }

        private void SaveView()
        {
            _store.Set("prehlednaradi", -1, _serializer.Serialize(_view, new MessageHeaders()));
        }

        public bool NeedsRebuild()
        {
            return !Enumerable.SequenceEqual(_view.ProjectionHash, _projectionHash);
        }

        public long GetClockToHandle()
        {
            return _view.ProjectionClock;
        }

        public void Dispatch(Message message)
        {
            _dispatcher.Dispatch(message);
            _view.ProjectionClock = message.Headers.EventClock;
            if (!_bulkUpdate)
                SaveView();
        }

        private void Handle(DefinovanoPouzivaneNaradiEvent ev)
        {
            if (_indexPodleId.ContainsKey(ev.Id))
                return;
            var dto = new PouzivaneNaradiDto()
            {
                Id = ev.Id,
                Vykres = ev.Vykres,
                Rozmer = ev.Rozmer,
                Druh = ev.Druh
            };
            _indexPodleId[ev.Id] = dto;
            int pozice = 0;
            for (int i = 0; i < _view.SeznamNaradi.Count; i++)
            {
                var comparison = Compare(dto, _view.SeznamNaradi[i]);
                if (comparison < 0)
                {
                    pozice = i;
                    break;
                }
            }
            _view.SeznamNaradi.Insert(pozice, dto);
        }

        private int Compare(PouzivaneNaradiDto a, PouzivaneNaradiDto b)
        {
            if (a == b)
                return 0;
            var compareVykres = string.CompareOrdinal(a.Vykres, b.Vykres);
            var compareRozmer = string.CompareOrdinal(a.Rozmer, b.Rozmer);
            if (compareVykres != 0)
                return compareVykres;
            if (compareRozmer != 0)
                return compareRozmer;
            return 0;
        }

        private void Handle(UpravenPocetNaradiNaSkladeEvent ev)
        {
            PouzivaneNaradiDto dto;
            if (!_indexPodleId.TryGetValue(ev.Id, out dto))
                return;
            dto.PocetNaSklade = ev.NoveMnozstvi;
        }
    }
}
