using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

public class MaxPropertiesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly long _maxProperties;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MaxPropertiesSchemaRuleValidator(long maxProperties)
    {
        _maxProperties = maxProperties;
    }
    protected override bool ValidateInternal(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
    {
        if(value.Count <= _maxProperties)
            return true;
        errorBuilder?.AddError($"The object at '{errorBuilder.Path}' must have no more than {_maxProperties} properties, but it has {value.Count}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.MaxProperties)]
// ReSharper disable once UnusedType.Global
public class MaxPropertiesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MaxPropertiesSchemaRuleValidator>
{
    public override MaxPropertiesSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath.FullPath, out var maxProperties) 
            ? new MaxPropertiesSchemaRuleValidator(maxProperties)
            : null;
    }
}
