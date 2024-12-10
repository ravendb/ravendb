using System;
using Sparrow.Json;
using System.Linq;

namespace Raven.Server.SchemaValidation.Number;

public class MinimumSchemaRuleValidator : NumberSchemaRuleValidator
{
    public const string RuleName = "minimum";
    public const string ExclusiveMinimumRuleName = "exclusiveMinimum";
    
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

public class MinimumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory
{
    protected override string Rule => MinimumSchemaRuleValidator.RuleName;

    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        if (NumberTypes.Contains(type) == false)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], NumberTypes, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out decimal minimum) == false)
            throw new InvalidOperationException($"'{Rule}' must to convertable to decimal here. Should not happen");
        
        const string exclusive = MinimumSchemaRuleValidator.ExclusiveMinimumRuleName;
        if (TryGetPropertyType(schemaDefinition, exclusive, out type))
        {
            if (type != BlittableJsonToken.Boolean)
                TrowRuleTypeError(exclusive, schemaDefinition[exclusive], BlittableJsonToken.Boolean, type, schemaPath);
            //TODO Maybe also to handle old version of exclusiveMaximum
        }
        schemaDefinition.TryGet(exclusive, out bool exclusiveMaximum);
        
        return new MinimumSchemaRuleValidator(minimum, exclusiveMaximum);
    }
}
