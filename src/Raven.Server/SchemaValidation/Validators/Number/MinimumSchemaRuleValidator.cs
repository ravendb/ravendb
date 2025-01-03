using System;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

[SchemaRule("minimum")]
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
    public const string ExclusiveMinimumRuleName = "exclusiveMinimum";

    public override MinimumSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        if (NumberTypes.Contains(type) == false)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], NumberTypes, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out decimal minimum) == false)
            throw new InvalidOperationException($"'{Rule}' must to convertable to decimal here. Should not happen");

        if (TryGetPropertyType(schemaDefinition, ExclusiveMinimumRuleName, out type))
        {
            if (type != BlittableJsonToken.Boolean)
                TrowRuleTypeError(ExclusiveMinimumRuleName, schemaDefinition[ExclusiveMinimumRuleName], BlittableJsonToken.Boolean, type, schemaPath);
            //TODO Maybe also to handle old version of exclusiveMaximum
        }
        schemaDefinition.TryGet(ExclusiveMinimumRuleName, out bool exclusiveMaximum);
        
        return new MinimumSchemaRuleValidator(minimum, exclusiveMaximum);
    }
}
