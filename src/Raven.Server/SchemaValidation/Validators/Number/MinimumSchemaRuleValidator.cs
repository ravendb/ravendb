using System;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

public class MinimumSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _minimum;
    private readonly Func<decimal, IErrorBuilder, bool> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MinimumSchemaRuleValidator(decimal minimum, bool exclusiveMinimum)
    {
        _minimum = minimum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidate : NonExclusiveValidate;
    }

    protected override bool ValidateInternal(decimal value, IErrorBuilder errorBuilder) => _validatePredicate(value, errorBuilder);

    private bool NonExclusiveValidate(decimal value, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_minimum) >= 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{errorBuilder.Path}' should be greater than or equal to {_minimum}.");
        return false;
    }

    private bool ExclusiveValidate(decimal value, IErrorBuilder errorBuilder)
    {
        if (value.CompareTo(_minimum) > 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{errorBuilder.Path}' should be greater than {_minimum}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.minimum)]
public class MinimumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MinimumSchemaRuleValidator>
{
    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        if(SchemaValidationHelper.TryGetNumber(schemaDefinition, Rule, schemaPath.FullPath, out var minimum) == false)
            return null;
        
        SchemaValidationHelper.TryGetBoolean(schemaDefinition, SchemaValidatorConstants.exclusiveMinimum, schemaPath.FullPath, out bool exclusiveMinimum);

        return new MinimumSchemaRuleValidator(minimum, exclusiveMinimum);
    }
}
