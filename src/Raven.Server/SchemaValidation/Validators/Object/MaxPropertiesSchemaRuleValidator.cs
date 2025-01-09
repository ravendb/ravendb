using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[SchemaRule(SchemaValidatorConstants.maxProperties)]
public class MaxPropertiesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly long _maxProperties;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MaxPropertiesSchemaRuleValidator(long maxProperties)
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

// ReSharper disable once UnusedType.Global
public class MaxPropertiesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MaxPropertiesSchemaRuleValidator>
{
    public override MaxPropertiesSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath, out var maxProperties) 
            ? new MaxPropertiesSchemaRuleValidator(maxProperties)
            : null;
    }
}
