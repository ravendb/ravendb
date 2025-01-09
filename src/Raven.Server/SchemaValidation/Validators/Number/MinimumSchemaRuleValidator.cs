using System;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

[SchemaRule(SchemaValidatorConstants.minimum)]
public class MinimumSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _minimum;
    private readonly Action<decimal, SchemaValidatorPath, IErrorBuilder> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MinimumSchemaRuleValidator(decimal minimum, bool exclusiveMinimum)
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

// ReSharper disable once UnusedType.Global
public class MinimumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MinimumSchemaRuleValidator>
{
    public override MinimumSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(SchemaValidationHelper.TryGetNumber(schemaDefinition, Rule, schemaPath, out var minimum) == false)
            return null;
        
        SchemaValidationHelper.TryGetBoolean(schemaDefinition, SchemaValidatorConstants.exclusiveMinimum, schemaPath, out bool exclusiveMinimum);

        return new MinimumSchemaRuleValidator(minimum, exclusiveMinimum);
    }
}
