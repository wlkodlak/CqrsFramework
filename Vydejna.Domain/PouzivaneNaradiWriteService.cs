using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Domain;
using Vydejna.Contracts;

namespace Vydejna.Domain
{
    public class PouzivaneNaradiWriteService : INaradiWriteService
    {
        private IRepository<Guid, Naradi> _repo;

        public PouzivaneNaradiWriteService(IRepository<Guid, Naradi> repository)
        {
            _repo = repository;
        }

        public void DefinovatPouzivaneNaradi(DefinovatPouzivaneNaradiCommand cmd)
        {
            ValidationErrorException.ConditionalThrow(new DefinovatPouzivaneNaradiCommandValidator().Validate(cmd));
            var naradi = new Naradi();
            naradi.Definovat(cmd.Id, cmd.Vykres, cmd.Rozmer, cmd.Druh);
            _repo.Save(naradi, null, RepositorySaveFlags.Create);
        }

        public void UpravitPocetNaradiNaSklade(UpravitPocetNaradiNaSkladeCommand cmd)
        {
            ValidationErrorException.ConditionalThrow(new UpravitPocetNaradiNaSkladeCommandValidator().Validate(cmd));
            var naradi = _repo.Get(cmd.Id);
            naradi.UpravitPocetNaSklade(cmd.TypUpravy, cmd.ZmenaMnozstvi);
            _repo.Save(naradi, null, RepositorySaveFlags.Append);
        }

        public void PrijmoutNaradiZeSkladu(PrijmoutNaradiZeSkladuCommand cmd)
        {
            ValidationErrorException.ConditionalThrow(new PrijmoutNaradiZeSkladuCommandValidator().Validate(cmd));
            var naradi = _repo.Get(cmd.Id);
            naradi.PrijmoutNaradiZeSkladu(cmd.Mnozstvi, cmd.Dodavatel, cmd.Cena);
            _repo.Save(naradi, null, RepositorySaveFlags.Append);
        }
    }
}
