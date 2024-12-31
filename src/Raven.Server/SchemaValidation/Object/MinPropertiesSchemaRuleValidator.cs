using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

public class MinPropertiesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    public const string RuleName = "minProperties";
    
    private readonly int _minProperties;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MinPropertiesSchemaRuleValidator(int minProperties)
    {
        _minProperties = minProperties;
    }
    protected override void ValidateInternal(BlittableJsonReaderObject value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(value.Count >= _minProperties)
            return;
        errorBuilder.AddError($"The object at '{path}' must have at least {_minProperties} properties, but it has only {value.Count}.");
    }
}

public class MinPropertiesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory
{
    protected override string Rule => MinPropertiesSchemaRuleValidator.RuleName;

    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        const BlittableJsonToken expectedType = BlittableJsonToken.Integer;
        if (type != expectedType)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], expectedType, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out int maximinProperties) == false)
            throw new InvalidOperationException($"'{Rule}' must to be convertable to decimal here. Should not happen");

        return new MinPropertiesSchemaRuleValidator(maximinProperties);
    }
}
