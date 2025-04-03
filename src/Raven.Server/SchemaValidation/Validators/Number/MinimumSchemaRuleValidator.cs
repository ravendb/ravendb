using System;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

public class MinimumSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _minimum;
    private readonly Func<decimal, ErrorBuilder, bool> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MinimumSchemaRuleValidator(decimal minimum, bool exclusiveMinimum)
    {
        _minimum = minimum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidate : NonExclusiveValidate;
    }

    public override bool Validate(decimal value, ErrorBuilder errorBuilder) => _validatePredicate(value, errorBuilder);

    private bool NonExclusiveValidate(decimal value, ErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_minimum) >= 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{errorBuilder.Path}' should be greater than or equal to {_minimum}.");
        return false;
    }

    private bool ExclusiveValidate(decimal value, ErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_minimum) > 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{errorBuilder.Path}' should be greater than {_minimum}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.Minimum)]
// ReSharper disable once UnusedType.Global
public class MinimumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MinimumSchemaRuleValidator>
{
    public override MinimumSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        if(SchemaValidationHelper.TryGetNumber(schemaDefinition, Rule, schemaPath.FullPath, out var minimum) == false)
            return null;
        
        SchemaValidationHelper.TryGetBoolean(schemaDefinition, SchemaValidatorConstants.ExclusiveMinimum, schemaPath.FullPath, out bool exclusiveMinimum);

        return new MinimumSchemaRuleValidator(minimum, exclusiveMinimum);
    }
}
