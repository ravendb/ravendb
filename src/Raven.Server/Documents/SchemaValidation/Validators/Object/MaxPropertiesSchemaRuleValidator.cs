using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Object;

public class MaxPropertiesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly long _maxProperties;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MaxPropertiesSchemaRuleValidator(long maxProperties)
    {
        _maxProperties = maxProperties;
    }
    public override bool Validate(SchemaValidationContext context, BlittableJsonReaderObject value)
    {
        if(value.Count <= _maxProperties)
            return true;
        context.ErrorBuilder?.AddError($"The object at '{context.ErrorBuilder.Path}' must have no more than {_maxProperties} properties, but it has {value.Count}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.MaxProperties)]
// ReSharper disable once UnusedType.Global
public class MaxPropertiesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MaxPropertiesSchemaRuleValidator>
{
    public override MaxPropertiesSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath + Rule, out var maxProperties) 
            ? new MaxPropertiesSchemaRuleValidator(maxProperties)
            : null;
    }
}
