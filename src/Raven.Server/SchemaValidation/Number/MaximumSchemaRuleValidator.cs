using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Number;

[SchemaRule("maximum")]
public class MaximumSchemaRuleValidator : NumberSchemaRuleValidator
{
    public const string RuleName = "maximum";
    private const string ExclusiveMaximumRuleName = "exclusiveMaximum";
    
    private readonly decimal _maximum;
    private readonly Action<decimal, SchemaValidatorPath, IErrorBuilder> _validatePredicate;

    public static MaximumSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition)
    {
        if (schemaDefinition.TryGet(RuleName, out decimal maximum) == false)
            //TODO Should not happen. Also maybe collect all error to return full error report
            return null;

        schemaDefinition.TryGet(ExclusiveMaximumRuleName, out bool exclusiveMaximum);
        //TODO Maybe also to handle old version of exclusiveMaximum
        
        return new MaximumSchemaRuleValidator(maximum, exclusiveMaximum);
    }
    
    // ReSharper disable once IntroduceOptionalParameters.Global
    private MaximumSchemaRuleValidator(decimal maximum, bool exclusiveMinimum)
    {
        _maximum = maximum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidate : NonExclusiveValidate;
    }

    protected override void ValidateInternal(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        _validatePredicate(value, path, errorBuilder);
    }

    private void NonExclusiveValidate(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_maximum) > 0) 
            errorBuilder.AddError($"The value '{value}' at '{path}' should be less than or equal to {_maximum}.");
    }
    private void ExclusiveValidate(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_maximum) >= 0)
            errorBuilder.AddError($"The value '{value}' at '{path}' should be less than {_maximum}.");
    }
}
