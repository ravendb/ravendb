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
    protected override bool ValidateInternal(BlittableJsonReaderObject value, IErrorBuilder errorBuilder)
    {
        if(value.Count <= _maxProperties)
            return true;
        errorBuilder?.AddError($"The object at '{errorBuilder.Path}' must have no more than {_maxProperties} properties, but it has {value.Count}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.maxProperties)]
public class MaxPropertiesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MaxPropertiesSchemaRuleValidator>
{
    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath.FullPath, out var maxProperties) 
            ? new MaxPropertiesSchemaRuleValidator(maxProperties)
            : null;
    }
}
