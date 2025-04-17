using System.Collections.Generic;
using Raven.Server.SchemaValidation.ErrorMessage;
using Raven.Server.SchemaValidation.Validators.String;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

public class PropertyNamesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly SchemaRuleValidator<LazyStringValue>[] _propertyNameValidators;

    // ReSharper disable once ConvertToPrimaryConstructor
    public PropertyNamesSchemaRuleValidator(SchemaRuleValidator<LazyStringValue>[] propertyNameValidators)
    {
        _propertyNameValidators = propertyNameValidators;
    }

    public override bool Validate(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
    {
        if (_propertyNameValidators == null)
            return true;

        var isValid = true;
        for (int i = 0; i < value.Count; i++)
        {
            var propName = value.GetPropertyNameByIndex(i);
            foreach (var validator in _propertyNameValidators)
            {
                isValid &= validator.Validate(propName, errorBuilder);
                if (errorBuilder == null && isValid == false)
                    return false;
            }
        }

        return isValid;
    }
}

[SchemaRule(SchemaValidatorConstants.PropertyNames)]
// ReSharper disable once UnusedType.Global
public class PropertyNamesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<PropertyNamesSchemaRuleValidator>
{
    public override PropertyNamesSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        if(SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath.FullPath, out var propertyNames) == false)
            return null;
        schemaPath += Rule;
        List<SchemaRuleValidator<LazyStringValue>> propertyNameValidators = null;
        for (int i = 0; i < propertyNames.Count; i++)
        {
            var propName = propertyNames.GetPropertyNameByIndex(i);
            if(SchemaRuleValidatorFactoryHelper.TryCreateValidator(propName, propertyNames, schemaPath, refSchemas, out var ruleValidator) == false)
                continue;
            var ruleSchemaPath = schemaPath + propName;
            if (ruleValidator is not StringSchemaRuleValidator stringValidator)
                throw new InvalidSchemaValidationDefinitionException(
                    $"The rule '{propName}' defined in '{Rule}' at '{ruleSchemaPath}' is invalid because it includes constraints that are not applicable to strings.");

            stringValidator.FocusOnPropertyName();
            (propertyNameValidators ??= []).Add(stringValidator);
        }

        if (propertyNameValidators == null)
            return null;
        
        return new PropertyNamesSchemaRuleValidator(propertyNameValidators.ToArray());
    }
}
