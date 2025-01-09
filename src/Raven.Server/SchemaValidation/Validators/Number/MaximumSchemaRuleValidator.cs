using System;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

[SchemaRule(SchemaValidatorConstants.maximum)]
public class MaximumSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _maximum;
    private readonly Func<decimal, SchemaValidatorPath, IErrorBuilder, bool> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MaximumSchemaRuleValidator(decimal maximum, bool exclusiveMinimum)
    {
        _maximum = maximum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidate : NonExclusiveValidate;
    }

    protected override bool ValidateInternal(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        return _validatePredicate(value, path, errorBuilder);
    }

    private bool NonExclusiveValidate(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_maximum) <= 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{path}' should be less than or equal to {_maximum}.");
        return false;
    }

    private bool ExclusiveValidate(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_maximum) < 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{path}' should be less than {_maximum}.");
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
