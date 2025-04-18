using System;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

public class MaximumSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _maximum;
    private readonly Func<decimal, ErrorBuilder, bool> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MaximumSchemaRuleValidator(decimal maximum, bool exclusiveMinimum)
    {
        _maximum = maximum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidate : NonExclusiveValidate;
    }

    public override bool Validate(decimal value, ErrorBuilder errorBuilder)
    {
        return _validatePredicate(value, errorBuilder);
    }

    private bool NonExclusiveValidate(decimal value, ErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_maximum) <= 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{errorBuilder.Path}' should be less than or equal to {_maximum}.");
        return false;
    }

    private bool ExclusiveValidate(decimal value, ErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_maximum) < 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{errorBuilder.Path}' should be less than {_maximum}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.Maximum)]
// ReSharper disable once UnusedType.Global
public class MaximumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MaximumSchemaRuleValidator>
{
    public override MaximumSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        schemaPath += Rule;
        if(SchemaValidationHelper.TryGetNumber(schemaDefinition, Rule, schemaPath, out var maximum) == false)
            return null;

        SchemaValidationHelper.TryGetBoolean(schemaDefinition, SchemaValidatorConstants.ExclusiveMaximum, schemaPath, out bool exclusiveMaximum);
        
        return new MaximumSchemaRuleValidator(maximum, exclusiveMaximum);
    }
}
