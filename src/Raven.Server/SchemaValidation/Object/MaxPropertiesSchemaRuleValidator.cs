using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

public class MaxPropertiesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    public const string RuleName = "maxProperties";
    
    private readonly int _maxProperties;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MaxPropertiesSchemaRuleValidator(int maxProperties)
    {
        _maxProperties = maxProperties;
    }
    protected override void ValidateInternal(BlittableJsonReaderObject value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(value.Count <= _maxProperties)
            return;
        errorBuilder.AddError($"The object at '{path}' must have no more than {_maxProperties} properties, but it has {value.Count}.");
    }
}

public class MaxPropertiesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory
{
    protected override string Rule => MaxPropertiesSchemaRuleValidator.RuleName;

    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        const BlittableJsonToken expectedType = BlittableJsonToken.Integer;
        if (type != expectedType)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], expectedType, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out int maxProperties) == false)
            throw new InvalidOperationException($"'{Rule}' must to be convertable to decimal here. Should not happen");

        return new MaxPropertiesSchemaRuleValidator(maxProperties);
    }
}
