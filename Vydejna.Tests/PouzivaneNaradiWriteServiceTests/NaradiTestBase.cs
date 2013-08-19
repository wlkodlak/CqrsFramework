using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Domain;
using Vydejna.Contracts;
using CqrsFramework.Domain;
using Moq;

namespace Vydejna.Tests.PouzivaneNaradiWriteServiceTests
{
    public abstract class NaradiTestBase
    {
        protected INaradiWriteService Service;
        protected List<IEvent> UlozeneUdalosti;

        protected void PripravitService(IEnumerable<IEvent> events)
        {
            Service = new PouzivaneNaradiWriteService(new TestNaradiRepository(this, events ?? new IEvent[0]));
            UlozeneUdalosti = new List<IEvent>();
        }

        private class TestNaradiRepository : IRepository<Guid, Naradi>
        {
            private NaradiTestBase _parent;
            private List<IEvent> _events;

            public TestNaradiRepository(NaradiTestBase parent, IEnumerable<IEvent> events)
            {
                _parent = parent;
                _events = events.ToList();
            }

            public Naradi Get(Guid key)
            {
                var naradi = new Naradi();
                var vyberUdalosti = _events.Where(e => ShodaId(key, e)).ToList();
                (naradi as IAggregate).LoadFromHistory(null, vyberUdalosti);
                return naradi;
            }

            private bool ShodaId(Guid key, IEvent e)
            {
                return true;
            }

            public void Save(Naradi aggregate, object context, RepositorySaveFlags repositorySaveFlags)
            {
                var newEvents = (aggregate as IAggregate).GetEvents().ToList();
                (aggregate as IAggregate).Commit();
                _events.AddRange(newEvents);
                _parent.UlozeneUdalosti.AddRange(newEvents);
            }
        }
    }
}
