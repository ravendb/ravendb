using System;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

[SchemaRule(SchemaValidatorConstants.maximum)]
public class MaximumSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _maximum;
    private readonly Func<decimal, IErrorBuilder, bool> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MaximumSchemaRuleValidator(decimal maximum, bool exclusiveMinimum)
    {
        _maximum = maximum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidate : NonExclusiveValidate;
    }

    protected override bool ValidateInternal(decimal value, IErrorBuilder errorBuilder)
    {
        return _validatePredicate(value, errorBuilder);
    }

    private bool NonExclusiveValidate(decimal value, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_maximum) <= 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{errorBuilder.Path}' should be less than or equal to {_maximum}.");
        return false;
    }

    private bool ExclusiveValidate(decimal value, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_maximum) < 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{errorBuilder.Path}' should be less than {_maximum}.");
        return false;
    }
}

// ReSharper disable once UnusedType.Global
public class MaximumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MaximumSchemaRuleValidator>
{
    public override MaximumSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(SchemaValidationHelper.TryGetNumber(schemaDefinition, Rule, schemaPath, out var maximum) == false)
            return null;

        SchemaValidationHelper.TryGetBoolean(schemaDefinition, SchemaValidatorConstants.exclusiveMaximum, schemaPath, out bool exclusiveMaximum);
        
        return new MaximumSchemaRuleValidator(maximum, exclusiveMaximum);
    }
}
