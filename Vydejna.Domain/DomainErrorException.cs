using CqrsFramework.Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Contracts;

namespace Vydejna.Domain
{
    public class DomainErrorException : ApplicationException
    {
        public DomainErrorException(string message)
            : base(message)
        {
        }

        public static void ThrowFromValidation(IValidationResult<DefinovatPouzivaneNaradiCommand> validation)
        {
            if (validation.Severity <= ValidationRuleSeverity.Warning)
                return;
            var sb = new StringBuilder();
            sb.Append("Validation rules were broken: ");
            bool isFirst = true;
            foreach (var rule in validation.BrokenRules)
            {
                if (isFirst)
                    isFirst = false;
                else
                    sb.Append(", ");
                sb.Append(rule.ErrorCode);
            }
            throw new DomainErrorException(sb.ToString());
        }
    }
}
