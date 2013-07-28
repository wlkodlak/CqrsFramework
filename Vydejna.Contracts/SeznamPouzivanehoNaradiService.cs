using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;

namespace Vydejna.Contracts
{
    public interface ISeznamPouzivanehoNaradiService
    {
        Task<SeznamPouzivanehoNaradi> ZiskatSeznam();
    }

    [DataContract]
    public class SeznamPouzivanehoNaradi
    {
        [DataMember(Order = 0, IsRequired = true)]
        public List<PolozkaSeznamuPouzivanehoNaradi> Seznam { get; set; }
    }

    [DataContract]
    public class PolozkaSeznamuPouzivanehoNaradi
    {
        [DataMember(Order=0)]
        public Guid Id { get; set; }
        [DataMember(Order = 1)]
        public string Vykres { get; set; }
        [DataMember(Order = 2)]
        public string Rozmer { get; set; }
        [DataMember(Order = 3)]
        public string Druh { get; set; }
    }
}
