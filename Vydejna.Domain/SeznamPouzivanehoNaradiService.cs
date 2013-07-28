using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Contracts;
using CqrsFramework.Messaging;
using System.Threading;

namespace Vydejna.Domain
{
    public class SeznamPouzivanehoNaradiService : ISeznamPouzivanehoNaradiService
    {
        private IKeyValueProjectionReader<SeznamPouzivanehoNaradi> _store;

        public SeznamPouzivanehoNaradiService(IKeyValueProjectionReader<SeznamPouzivanehoNaradi> store)
        {
            this._store = store;
        }
        public Task<SeznamPouzivanehoNaradi> ZiskatSeznam()
        {
            return Task.Factory.StartNew(() => _store.Get("naradi-pouzivane"));
        }
    }
}
