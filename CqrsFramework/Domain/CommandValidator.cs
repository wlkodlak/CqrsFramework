using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Domain
{
    public interface IValidationRule<TCommand>
    {
        ValidationRuleSeverity Severity { get; }
        string ErrorCode { get; }
        string Message { get; }
        bool IsValid(TCommand cmd);
    }

    public interface IValidationResult<TCommand>
    {
        ValidationRuleSeverity Severity { get; }
        IList<IValidationRule<TCommand>> BrokenRules { get; }
    }

    public interface ICommandValidator<TCommand>
    {
        IValidationResult<TCommand> Validate(TCommand cmd);
    }

    public enum ValidationRuleSeverity
    {
        NoError,
        Warning,
        Required,
        Error
    }

    public abstract class CommandValidator<TCommand> : ICommandValidator<TCommand>
    {
        private List<IValidationRule<TCommand>> _allRules;

        protected CommandValidator()
        {
            _allRules = new List<IValidationRule<TCommand>>();
        }

        protected CommandValidator(IEnumerable<IValidationRule<TCommand>> rules)
        {
            _allRules = new List<IValidationRule<TCommand>>(rules);
        }

        protected void AddRule(IValidationRule<TCommand> rule)
        {
            _allRules.Add(rule);
        }

        public IValidationResult<TCommand> Validate(TCommand cmd)
        {
            var result = new ValidationResult();
            foreach (var rule in _allRules)
            {
                if (!rule.IsValid(cmd))
                    result.AddBrokenRule(rule);
            }
            return result;
        }

        protected IValidationRule<TCommand> AddRule(ValidationRuleSeverity severity, string code, string message, Predicate<TCommand> predicate)
        {
            var rule = new CommandValidationRule<TCommand>(severity, code, message, predicate);
            _allRules.Add(rule);
            return rule;
        }

        private class ValidationResult : IValidationResult<TCommand>
        {
            private ValidationRuleSeverity _severity = ValidationRuleSeverity.NoError;
            private List<IValidationRule<TCommand>> _brokenRules = new List<IValidationRule<TCommand>>();

            public ValidationRuleSeverity Severity
            {
                get { return _severity; }
            }

            public IList<IValidationRule<TCommand>> BrokenRules
            {
                get { return _brokenRules; }
            }

            public void AddBrokenRule(IValidationRule<TCommand> rule)
            {
                _brokenRules.Add(rule);
                if (_severity < rule.Severity)
                    _severity = rule.Severity;
            }
        }
    }

    public class CommandValidationRule<TCommand> : IValidationRule<TCommand>
    {
        private Predicate<TCommand> _predicate;

        public CommandValidationRule(ValidationRuleSeverity severity, string code, string message, Predicate<TCommand> isValid)
        {
            this.Severity = severity;
            this.ErrorCode = code;
            this.Message = message;
            this._predicate = isValid;
        }

        public ValidationRuleSeverity Severity { get; protected set; }
        public string ErrorCode { get; protected set; }
        public string Message { get; protected set; }
        public bool IsValid(TCommand cmd)
        {
            return _predicate(cmd);
        }
    }
}
