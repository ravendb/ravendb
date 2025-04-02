using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

public class MinPropertiesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly long _minProperties;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MinPropertiesSchemaRuleValidator(long minProperties)
    {
        _minProperties = minProperties;
    }
    protected override bool ValidateInternal(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
    {
        if(value.Count >= _minProperties)
            return true;
        errorBuilder?.AddError($"The object at '{errorBuilder.Path}' must have at least {_minProperties} {(_minProperties == 1 ? "property": "properties")}, but it has only {value.Count}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.MinProperties)]
// ReSharper disable once UnusedType.Global
public class MinPropertiesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MinPropertiesSchemaRuleValidator>
{
    public override MinPropertiesSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath.FullPath, out var minProperties) 
            ? new MinPropertiesSchemaRuleValidator(minProperties)
            : null;
    }
}
