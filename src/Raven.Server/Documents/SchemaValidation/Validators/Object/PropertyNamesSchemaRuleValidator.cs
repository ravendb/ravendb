using System.Collections.Generic;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Raven.Server.Documents.SchemaValidation.Validators.String;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Object;

public class PropertyNamesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly SchemaRuleValidator<LazyStringValue>[] _propertyNameValidators;

    // ReSharper disable once ConvertToPrimaryConstructor
    public PropertyNamesSchemaRuleValidator(SchemaRuleValidator<LazyStringValue>[] propertyNameValidators)
    {
        _propertyNameValidators = propertyNameValidators;
    }

    public override bool Validate(SchemaValidationContext context, BlittableJsonReaderObject value)
    {
        if (_propertyNameValidators == null)
            return true;

        var isValid = true;
        for (int i = 0; i < value.Count; i++)
        {
            var propName = value.GetPropertyNameByIndex(i);
            foreach (var validator in _propertyNameValidators)
            {
                isValid &= validator.Validate(context, propName);
                if (context.ErrorBuilder == null && isValid == false)
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
    public override PropertyNamesSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        schemaPath += Rule;
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath, out var propertyNames) == false)
            return null;

        List<SchemaRuleValidator<LazyStringValue>> propertyNameValidators = null;
        for (int i = 0; i < propertyNames.Count; i++)
        {
            var propName = propertyNames.GetPropertyNameByIndex(i);
            if (SchemaRuleValidatorFactoryHelper.TryCreateValidator(context, propName, propertyNames, schemaPath, out var ruleValidator) == false)
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
