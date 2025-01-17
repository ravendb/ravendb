using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[SchemaRule(SchemaValidatorConstants.minProperties)]
public class MinPropertiesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly long _minProperties;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MinPropertiesSchemaRuleValidator(long minProperties)
    {
        _minProperties = minProperties;
    }
    protected override bool ValidateInternal(BlittableJsonReaderObject value, IErrorBuilder errorBuilder)
    {
        if(value.Count >= _minProperties)
            return true;
        errorBuilder?.AddError($"The object at '{errorBuilder.Path}' must have at least {_minProperties} properties, but it has only {value.Count}.");
        return false;
    }
}

// ReSharper disable once UnusedType.Global
public class MinPropertiesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MinPropertiesSchemaRuleValidator>
{
    public override MinPropertiesSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath, out var minProperties) 
            ? new MinPropertiesSchemaRuleValidator(minProperties)
            : null;
    }
}
