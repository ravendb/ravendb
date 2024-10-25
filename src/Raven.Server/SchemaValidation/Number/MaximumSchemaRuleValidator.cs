using System;

namespace Raven.Server.SchemaValidation.Number;

//TODO Should we support number value for exclusiveMaximum?
[SchemaRule("maximum", "exclusiveMaximum")]
public class MaximumSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _maximum;
    private readonly Action<decimal, IErrorBuilder> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MaximumSchemaRuleValidator(string path, decimal maximum) : this(path, maximum, false)
    {
    }

    public MaximumSchemaRuleValidator(string path, decimal maximum, bool exclusiveMinimum) : base(path)
    {
        _maximum = maximum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidate : NonExclusiveValidate;
    }

    protected override void ValidateInternal(decimal value, IErrorBuilder errorBuilder)
    {
        _validatePredicate(value, errorBuilder);
    }

    private void NonExclusiveValidate(decimal value, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_maximum) > 0) 
            errorBuilder.AddError($"The value '{value}' at '{Path}' should be less than or equal to {_maximum}.");
    }
    private void ExclusiveValidate(decimal value, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_maximum) >= 0)
            errorBuilder.AddError($"The value '{value}' at '{Path}' should be less than {_maximum}.");
    }
}
