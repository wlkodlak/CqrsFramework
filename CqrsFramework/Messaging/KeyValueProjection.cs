using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Threading.Tasks;
using CqrsFramework.KeyValueStore;
using CqrsFramework.Serialization;
using System.Linq.Expressions;

namespace CqrsFramework.Messaging
{
    public class KeyValueProjection<TView> : IProjectionDispatcher where TView : new()
    {
        private IKeyValueStore _store;
        private IKeyValueProjectionStrategy _strategy;
        private string _typename;
        private string _metadataPrefix;
        private byte[] _rebuildHash;
        private Dictionary<Type, Registration> _registrations;

        private bool _bulkMode, _purgeBeforeFlush;
        private long _bulkClock = 0;
        private Dictionary<string, CacheItem> _cache;
        private bool _needsRebuild = false;
        private bool _metadataLoaded;

        private class Registration
        {
            public Type Type;
            public Func<object, string> GetKey;
            public Func<object, MessageHeaders, TView> AddFunc;
            public Func<object, MessageHeaders, TView, TView> UpdateFunc;
        }

        private class CacheItem
        {
            public string Key;
            public int OriginalVersion;
            public TView View;
            public bool Exists;
        }

        public KeyValueProjection(IKeyValueStore store, IKeyValueProjectionStrategy strategy, string metadataPrefix, byte[] rebuildHash)
        {
            _store = store;
            _strategy = strategy;
            _typename = strategy.GetTypename(typeof(TView));
            _metadataPrefix = metadataPrefix;
            _rebuildHash = rebuildHash;
            _registrations = new Dictionary<Type, Registration>();
            _cache = new Dictionary<string, CacheItem>(StringComparer.Ordinal);
        }

        public void BeginUpdate()
        {
            LoadMetadata();
            _bulkMode = true;
        }

        public void EndUpdate()
        {
            _bulkMode = false;
            if (_purgeBeforeFlush)
                _store.Purge();
            foreach (var cacheItem in _cache.Values)
                _store.Set(cacheItem.Key, -1, _strategy.SerializeView(typeof(TView), cacheItem.View));
            SaveClock(_bulkClock);
            if (_purgeBeforeFlush)
                _store.Set(string.Format("{0}__Hash", _metadataPrefix), -1, _rebuildHash);
        }

        public void Reset()
        {
            if (_bulkMode)
                _purgeBeforeFlush = true;
            else
            {
                _store.Purge();
                _cache.Clear();
            }
        }

        public bool NeedsRebuild()
        {
            LoadMetadata();
            return _needsRebuild;
        }

        private void LoadMetadata()
        {
            if (_metadataLoaded)
                return;
            _needsRebuild = LoadNeedsRebuild();
            if (_needsRebuild)
                _bulkClock = 0;
            else
                _bulkClock = LoadClock();
            _metadataLoaded = true;
        }

        public long GetClockToHandle()
        {
            LoadMetadata();
            return _bulkClock;
        }

        private bool LoadNeedsRebuild()
        {
            var doc = _store.Get(string.Format("{0}__Hash", _metadataPrefix));
            if (doc == null)
                return false;
            return !Enumerable.SequenceEqual(_rebuildHash, doc.Data);
        }

        private long LoadClock()
        {
            var doc = _store.Get(string.Format("{0}__Clock", _metadataPrefix));
            if (doc == null || doc.Data == null)
                return 0;
            return _strategy.DeserializeClock(doc.Data);
        }

        public void Dispatch(Message message)
        {
            Registration registration;
            if (!_registrations.TryGetValue(message.Payload.GetType(), out registration))
                return;
            var key = registration.GetKey(message.Payload);
            if (_bulkMode)
            {
                CacheItem cacheItem;
                if (!_cache.TryGetValue(key, out cacheItem))
                {
                    _cache[key] = cacheItem = new CacheItem();
                    cacheItem.Key = key;
                    var doc = _purgeBeforeFlush ? null : _store.Get(key);
                    if (doc != null)
                    {
                        cacheItem.Exists = true;
                        cacheItem.OriginalVersion = doc.Version;
                        cacheItem.View = doc.Data == null ? default(TView) : (TView)_strategy.DeserializeView(typeof(TView), doc.Data);
                    }
                }

                if (!cacheItem.Exists)
                {
                    if (registration.AddFunc == null)
                        throw HandlerNotFound(message.Payload, true);
                    cacheItem.View = registration.AddFunc(message.Payload, message.Headers);
                    cacheItem.Exists = true;
                    _bulkClock = message.Headers.EventClock + 1;
                }
                else
                {
                    if (registration.UpdateFunc == null)
                        throw HandlerNotFound(message.Payload, false);
                    cacheItem.View = registration.UpdateFunc(message.Payload, message.Headers, cacheItem.View);
                    _bulkClock = message.Headers.EventClock + 1;
                }
            }
            else
            {
                var oldDoc = _store.Get(key);
                if (oldDoc == null)
                {
                    if (registration.AddFunc == null)
                        throw HandlerNotFound(message.Payload, true);
                    var view = registration.AddFunc(message.Payload, message.Headers);
                    _store.Set(key, 0, _strategy.SerializeView(typeof(TView), view));
                    SaveClock(message.Headers.EventClock + 1);
                }
                else
                {
                    if (registration.UpdateFunc == null)
                        throw HandlerNotFound(message.Payload, false);
                    var oldView = (TView)_strategy.DeserializeView(typeof(TView), oldDoc.Data);
                    var newView = registration.UpdateFunc(message.Payload, message.Headers, oldView);
                    _store.Set(key, oldDoc.Version, _strategy.SerializeView(typeof(TView), newView));
                    SaveClock(message.Headers.EventClock + 1);
                }
            }
        }

        private Exception HandlerNotFound(object payload, bool isAdd)
        {
            return new InvalidOperationException(string.Format(
                "Event {0} has no {1} handler in {2} projection",
                payload.GetType().Name,
                isAdd ? "add" : "update",
                GetType().Name));
        }

        private void SaveClock(long clock)
        {
            _store.Set(string.Format("{0}__Clock", _metadataPrefix), -1, _strategy.SerializeClock(clock));
        }

        public void RegisterRaw(Type type, Func<object, string> key, bool hasAdd, bool hasUpdate, Func<object, MessageHeaders, TView> add, Func<object, MessageHeaders, TView, TView> update)
        {
            var registration = new Registration();
            registration.Type = type;
            registration.GetKey = key;
            registration.AddFunc = hasAdd ? add : null;
            registration.UpdateFunc = hasUpdate ? update : null;
            _registrations[registration.Type] = registration;
        }

        [System.Diagnostics.DebuggerNonUserCode]
        public void Register<TEvent>(Func<TEvent, string> key, Func<TEvent, MessageHeaders, TView> add, Func<TEvent, MessageHeaders, TView, TView> update)
        {
            RegisterRaw(typeof(TEvent),
                e => key((TEvent)e),
                add != null,
                update != null,
                (e, h) => add((TEvent)e, h),
                (e, h, v) => update((TEvent)e, h, v));
        }

        [System.Diagnostics.DebuggerNonUserCode]
        public void Register<TEvent>(Func<TEvent, string> key, Func<TEvent, TView> add, Func<TEvent, TView, TView> update)
        {
            RegisterRaw(typeof(TEvent),
                e => key((TEvent)e),
                add != null,
                update != null,
                (e, h) => add((TEvent)e),
                (e, h, v) => update((TEvent)e, v));
        }

        [System.Diagnostics.DebuggerNonUserCode]
        public void Register<TEvent>(Func<TEvent, string> key, Action<TEvent, TView> addOrUpdate)
        {
            RegisterRaw(typeof(TEvent),
                e => key((TEvent)e),
                addOrUpdate != null,
                addOrUpdate != null,
                (e, h) => { var v = new TView(); addOrUpdate((TEvent)e, v); return v; },
                (e, h, v) => { addOrUpdate((TEvent)e, v); return v; });
        }

        public void AutoRegister(object dispatcher)
        {
            var registrator = new KeyValueProjectionAutoRegister<TView>(dispatcher);
            var foundItems = registrator.FindMethods();
            foreach (var item in foundItems)
            {
                var add = registrator.MakeAdd(item);
                var update = registrator.MakeUpdate(item);
                RegisterRaw(
                    item.Type,
                    registrator.MakeGetKey(item),
                    add != null, update != null, 
                    add, update
                    );
            }
        }

    }
}
