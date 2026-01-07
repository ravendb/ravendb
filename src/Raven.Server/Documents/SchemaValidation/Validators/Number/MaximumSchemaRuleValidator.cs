using System;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Number;

public class MaximumSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _maximum;
    private readonly Func<SchemaValidationContext, decimal, bool> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MaximumSchemaRuleValidator(decimal maximum, bool exclusiveMinimum)
    {
        _maximum = maximum;
        _validatePredicate = exclusiveMinimum ? ExclusiveValidate : NonExclusiveValidate;
    }

    public override bool Validate(SchemaValidationContext context, decimal value)
    {
        return _validatePredicate(context, value);
    }

    private bool NonExclusiveValidate(SchemaValidationContext context, decimal value)
    {
        if (value.CompareTo(_maximum) <= 0) 
            return true;
        
        context.ErrorBuilder?.AddError($"The value '{value}' at '{context.ErrorBuilder.Path}' should be less than or equal to {_maximum}.");
        return false;
    }

    private bool ExclusiveValidate(SchemaValidationContext context, decimal value)
    {
        if (value.CompareTo(_maximum) < 0) 
            return true;
        
        context.ErrorBuilder?.AddError($"The value '{value}' at '{context.ErrorBuilder.Path}' should be less than {_maximum}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.Maximum)]
// ReSharper disable once UnusedType.Global
public class MaximumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MaximumSchemaRuleValidator>
{
    public override MaximumSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        schemaPath += Rule;
        if (SchemaValidationHelper.TryGetNumber(schemaDefinition, Rule, schemaPath, out var maximum) == false)
            return null;

        SchemaValidationHelper.TryGetBoolean(schemaDefinition, SchemaValidatorConstants.ExclusiveMaximum, schemaPath, out bool exclusiveMaximum);

        return new MaximumSchemaRuleValidator(maximum, exclusiveMaximum);
    }
}
