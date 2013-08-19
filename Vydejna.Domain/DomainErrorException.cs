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
        public string ErrorCode { get; private set; }
        private string _message;

        public DomainErrorException(string errorCode, string message)
        {
            this.ErrorCode = errorCode;
            this._message = message;
        }

        public override string Message
        {
            get { return _message; }
        }
    }

    public class ValidationErrorException : ApplicationException
    {
        public static void ConditionalThrow<TCommand>(IValidationResult<TCommand> validation)
        {
            if (validation.Severity > ValidationRuleSeverity.Warning)
                throw new ValidationErrorException();
        }
    }
}
