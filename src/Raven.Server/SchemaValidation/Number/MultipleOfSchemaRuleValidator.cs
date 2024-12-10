using System;
using Sparrow.Json;
using System.Linq;

namespace Raven.Server.SchemaValidation.Number;

public class MultipleOfSchemaRuleValidator : NumberSchemaRuleValidator
{
    public const string RuleName = "multipleOf";

    private readonly decimal _multipleOf;

    public static ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string s)
    {
        if (schemaDefinition.TryGet(RuleName, out decimal multipleOf) == false)
            //TODO Should not happen. Also maybe collect all error to return full error report
            return null;

        return new MultipleOfSchemaRuleValidator(multipleOf);
    }
    
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

public class MultipleOfSchemaRuleValidatorFactory : SchemaRuleValidatorFactory
{
    protected override string Rule => MultipleOfSchemaRuleValidator.RuleName;

    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
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
