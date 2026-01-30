using System;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Number;

public class MinimumSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _minimum;
    private readonly Func<SchemaValidationContext, decimal, bool> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MinimumSchemaRuleValidator(decimal minimum, bool exclusiveMinimum)
    {
        _minimum = minimum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidate : NonExclusiveValidate;
    }

    public override bool Validate(SchemaValidationContext context, decimal value) => _validatePredicate(context, value);

    private bool NonExclusiveValidate(SchemaValidationContext context, decimal value)
    {
        if (value.CompareTo(_minimum) >= 0) 
            return true;
        
        context.ErrorBuilder?.AddError($"The value '{value}' at '{context.ErrorBuilder.Path}' should be greater than or equal to {_minimum}.");
        return false;
    }

    private bool ExclusiveValidate(SchemaValidationContext context, decimal value)
    {
        if (value.CompareTo(_minimum) > 0) 
            return true;
        
        context.ErrorBuilder?.AddError($"The value '{value}' at '{context.ErrorBuilder.Path}' should be greater than {_minimum}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.Minimum)]
// ReSharper disable once UnusedType.Global
public class MinimumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MinimumSchemaRuleValidator>
{
    public override MinimumSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        schemaPath += Rule;
        if (SchemaValidationHelper.TryGetNumber(schemaDefinition, Rule, schemaPath, out var minimum) == false)
            return null;
        
        SchemaValidationHelper.TryGetBoolean(schemaDefinition, SchemaValidatorConstants.ExclusiveMinimum, schemaPath, out bool exclusiveMinimum);

        return new MinimumSchemaRuleValidator(minimum, exclusiveMinimum);
    }
}
