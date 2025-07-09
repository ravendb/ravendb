using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Object;

public class RequiredSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly LazyStringValue[] _requiredProperties;

    public RequiredSchemaRuleValidator(LazyStringValue[] required)
    {
        _requiredProperties = required;
    }
    
    public RequiredSchemaRuleValidator(LazyStringValue required)
    {
        _requiredProperties = [required];
    }
    
    public override bool Validate(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
    {
        var isValid = true;
        foreach (var required in _requiredProperties)
        {
            if(value.Contains(required))
                continue;
            errorBuilder?.AddError($"The required property '{required}' is missing at '{errorBuilder.Path}'.");
            isValid = false;
        }
        return isValid;
    }
}

[SchemaRule(SchemaValidatorConstants.Required)]
// ReSharper disable once UnusedType.Global
public class RequiredSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<RequiredSchemaRuleValidator>
{
    public override RequiredSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        if (SchemaValidationHelper.TryGetArray(schemaDefinition, Rule, schemaPath + Rule, out var blittableRequired) == false)
            return null;

        var required = SchemaValidationHelper.CheckBlittableArrayElementTypesAndThrow<LazyStringValue>(blittableRequired, schemaPath);
        return required != null
            ? new RequiredSchemaRuleValidator(required)
            : null;
    }
}
