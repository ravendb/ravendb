using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Number;

[SchemaRule("minimum")]
public class MinimumSchemaRuleValidator : NumberSchemaRuleValidator
{
    public const string RuleName = "minimum";
    private const string ExclusiveMinimumRuleName = "exclusiveMinimum";
    
    private readonly decimal _minimum;
    private readonly Action<decimal, SchemaValidatorPath, IErrorBuilder> _validatePredicate;

    public static MinimumSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition)
    {
        if (schemaDefinition.TryGet(RuleName, out decimal maximum) == false)
            //TODO Should not happen. Also maybe collect all error to return full error report
            return null;

        schemaDefinition.TryGet(ExclusiveMinimumRuleName, out bool exclusiveMaximum);
        //TODO Maybe also to handle old version of exclusiveMinimum
        
        return new MinimumSchemaRuleValidator(maximum, exclusiveMaximum);
    }
    
    // ReSharper disable once IntroduceOptionalParameters.Global
    private MinimumSchemaRuleValidator(decimal minimum, bool exclusiveMinimum)
    {
        _minimum = minimum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidate : NonExclusiveValidate;
    }

    protected override void ValidateInternal(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder) => _validatePredicate(value, path, errorBuilder);

    private void NonExclusiveValidate(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_minimum) < 0)
            errorBuilder.AddError($"The value '{value}' at '{path}' should be greater than or equal to {_minimum}.");
    }
    private void ExclusiveValidate(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_minimum) <= 0)
            errorBuilder.AddError($"The value '{value}' at '{path}' should be greater than {_minimum}.");
    }
}
