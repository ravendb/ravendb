using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Object;

public class MinPropertiesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly long _minProperties;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MinPropertiesSchemaRuleValidator(long minProperties)
    {
        _minProperties = minProperties;
    }
    public override bool Validate(SchemaValidationContext context, BlittableJsonReaderObject value)
    {
        if(value.Count >= _minProperties)
            return true;
        context.ErrorBuilder?.AddError($"The object at '{context.ErrorBuilder.Path}' must have at least {_minProperties} {(_minProperties == 1 ? "property": "properties")}, but it has only {value.Count}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.MinProperties)]
// ReSharper disable once UnusedType.Global
public class MinPropertiesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MinPropertiesSchemaRuleValidator>
{
    public override MinPropertiesSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath + Rule, out var minProperties) 
            ? new MinPropertiesSchemaRuleValidator(minProperties)
            : null;
    }
}
