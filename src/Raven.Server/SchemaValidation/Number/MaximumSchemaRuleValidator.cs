using System;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Number;

public class MaximumSchemaRuleValidator : NumberSchemaRuleValidator
{
    public const string RuleName = "maximum";
    public const string ExclusiveMaximumRuleName = "exclusiveMaximum";
    
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

public class MaximumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory
{
    protected override string Rule => MaximumSchemaRuleValidator.RuleName;

    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        if (NumberTypes.Contains(type) == false)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], NumberTypes, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out decimal maximum) == false)
            throw new InvalidOperationException($"'{Rule}' must to convertable to decimal here. Should not happen");
        
        const string exclusive = MaximumSchemaRuleValidator.ExclusiveMaximumRuleName;
        if (TryGetPropertyType(schemaDefinition, exclusive, out type))
        {
            if (type != BlittableJsonToken.Boolean)
                TrowRuleTypeError(exclusive, schemaDefinition[exclusive], BlittableJsonToken.Boolean, type, schemaPath);
            //TODO Maybe also to handle old version of exclusiveMaximum and add test
        }
        schemaDefinition.TryGet(exclusive, out bool exclusiveMaximum);
        
        return new MaximumSchemaRuleValidator(maximum, exclusiveMaximum);
    }
}
