using System;

namespace Raven.Server.SchemaValidation.Number;

//TODO Should we support number value for exclusiveMinimum?
[SchemaRule("minimum", "exclusiveMinimum")]
public class MinimumSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _minimum;
    private readonly Action<decimal, IErrorBuilder> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MinimumSchemaRuleValidator(string path, decimal minimum) : this(path, minimum, false)
    {
    }
    
    public MinimumSchemaRuleValidator(string path, decimal minimum, bool exclusiveMinimum) : base(path)
    {
        _minimum = minimum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidate : NonExclusiveValidate;
    }

    protected override void ValidateInternal(decimal value, IErrorBuilder errorBuilder) => _validatePredicate(value, errorBuilder);

    private void NonExclusiveValidate(decimal value, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_minimum) < 0)
            errorBuilder.AddError($"The value '{value}' at '{Path}' should be greater than or equal to {_minimum}.");
    }
    private void ExclusiveValidate(decimal value, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_minimum) <= 0)
            errorBuilder.AddError($"The value '{value}' at '{Path}' should be greater than {_minimum}.");
    }
}
