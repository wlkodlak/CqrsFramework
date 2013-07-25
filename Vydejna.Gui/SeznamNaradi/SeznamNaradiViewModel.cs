using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.ObjectModel;
using System.ComponentModel;

namespace Vydejna.Gui.SeznamNaradi
{
    public class SeznamNaradiViewModel
    {
        private ISeznamNaradiService _service;
        private ObservableCollection<PolozkaSeznamuNaradiViewModel> _seznam;

        private SeznamNaradiViewModel()
        {
            _seznam = new ObservableCollection<PolozkaSeznamuNaradiViewModel>();
            _seznam.Add(new PolozkaSeznamuNaradiViewModel(Guid.Empty, "5847-5877", "80x40x20", "Druh naradi"));
            _seznam.Add(new PolozkaSeznamuNaradiViewModel(Guid.Empty, "2144-5345", "120x50x20", "Klestina"));
            _seznam.Add(new PolozkaSeznamuNaradiViewModel(Guid.Empty, "3481-7484", "800x20", "Brusny papir"));
            _seznam.Add(new PolozkaSeznamuNaradiViewModel(Guid.Empty, "22-254-51", "175x35x5", "Datelna"));
            _seznam.Add(new PolozkaSeznamuNaradiViewModel(Guid.Empty, "2487-5448", "defektni", "Spacek"));
        }

        public SeznamNaradiViewModel(ISeznamNaradiService service)
        {
            _seznam = new ObservableCollection<PolozkaSeznamuNaradiViewModel>();
            _service = service;
        }

        public static SeznamNaradiViewModel DesignData { get { return new SeznamNaradiViewModel(); } }

        public IEnumerable<PolozkaSeznamuNaradiViewModel> SeznamNaradi { get { return _seznam; } }
    }

    public class PolozkaSeznamuNaradiViewModel
    {
        private Guid _id;
        private string _vykres, _rozmer, _druh;

        public Guid Id { get { return _id; } }
        public string Vykres { get { return _vykres; } }
        public string Rozmer { get { return _rozmer; } }
        public string Druh { get { return _druh; } }

        public PolozkaSeznamuNaradiViewModel(Guid id, string vykres, string rozmer, string druh)
        {
            _id = id;
            _vykres = vykres;
            _rozmer = rozmer;
            _druh = druh;
        }

    }

    public interface ISeznamNaradiService
    {

    }
        
}
