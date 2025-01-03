using System;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

[SchemaRule("multipleOf")]
public class MultipleOfSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _multipleOf;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MultipleOfSchemaRuleValidator(decimal multipleOf)
    {
        _multipleOf = multipleOf;
    }

    protected override void ValidateInternal(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(value % _multipleOf != 0)
            errorBuilder.AddError($"The value '{value}' at '{path}' should be a multiple of {_multipleOf}.");
    }
}

// ReSharper disable once UnusedType.Global
public class MultipleOfSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MultipleOfSchemaRuleValidator>
{
    public override MultipleOfSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        if (NumberTypes.Contains(type) == false)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], NumberTypes, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out decimal multipleOf) == false)
            throw new InvalidOperationException($"'{Rule}' must to convertable to decimal here. Should not happen");
        
        return new MultipleOfSchemaRuleValidator(multipleOf);
    }
}
