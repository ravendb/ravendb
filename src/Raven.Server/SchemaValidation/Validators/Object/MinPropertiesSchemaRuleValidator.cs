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
        errorBuilder?.AddError($"The object at '{errorBuilder.Path}' must have at least {_minProperties} properties, but it has only {value.Count}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.minProperties)]
public class MinPropertiesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MinPropertiesSchemaRuleValidator>
{
    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath.FullPath, out var minProperties) 
            ? new MinPropertiesSchemaRuleValidator(minProperties)
            : null;
    }
}
