using System;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

[SchemaRule("maximum")]
public class MaximumSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _maximum;
    private readonly Action<decimal, SchemaValidatorPath, IErrorBuilder> _validatePredicate;

    // ReSharper disable once IntroduceOptionalParameters.Global
    public MaximumSchemaRuleValidator(decimal maximum, bool exclusiveMinimum)
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

// ReSharper disable once UnusedType.Global
public class MaximumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MaximumSchemaRuleValidator>
{
    public const string ExclusiveMaximumRuleName = "exclusiveMaximum";

    public override MaximumSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        if (NumberTypes.Contains(type) == false)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], NumberTypes, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out decimal maximum) == false)
            throw new InvalidOperationException($"'{Rule}' must to convertable to decimal here. Should not happen");

        if (TryGetPropertyType(schemaDefinition, ExclusiveMaximumRuleName, out type))
        {
            if (type != BlittableJsonToken.Boolean)
                TrowRuleTypeError(ExclusiveMaximumRuleName, schemaDefinition[ExclusiveMaximumRuleName], BlittableJsonToken.Boolean, type, schemaPath);
            //TODO Maybe also to handle old version of exclusiveMaximum and add test
        }
        schemaDefinition.TryGet(ExclusiveMaximumRuleName, out bool exclusiveMaximum);
        
        return new MaximumSchemaRuleValidator(maximum, exclusiveMaximum);
    }
}
