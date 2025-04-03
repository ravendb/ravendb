using System.Collections.Generic;
using Raven.Server.SchemaValidation.Validators.String;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

public class PropertyNamesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly SchemaRuleValidator<string>[] _propertyNameValidators;

    // ReSharper disable once ConvertToPrimaryConstructor
    public PropertyNamesSchemaRuleValidator(SchemaRuleValidator<string>[] propertyNameValidators)
    {
        _propertyNameValidators = propertyNameValidators;
    }

    // ReSharper disable once ConvertToPrimaryConstructor
    public override bool Validate(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
    {
        if (_propertyNameValidators == null)
            return true;

        var isValid = true;
        foreach (var propertyName in value.GetPropertyNames())
        {
            foreach (var validator in _propertyNameValidators)
            {
                isValid &= validator.Validate(propertyName, errorBuilder);
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
        List<SchemaRuleValidator<string>> propertyNameValidators = null;
        foreach (var rule in propertyNames.GetPropertyNames())
        {
            if(SchemaRuleValidatorFactoryHelper.TryCreateValidator(rule, propertyNames, schemaPath, refSchemas, out var ruleValidator) == false)
                continue;
            var ruleSchemaPath = schemaPath + rule;
            if (ruleValidator is not StringSchemaRuleValidator stringValidator)
                throw new InvalidSchemaValidationDefinitionException(
                    $"The rule '{rule}' defined in '{Rule}' at '{ruleSchemaPath}' is invalid because it includes constraints that are not applicable to strings.");

            stringValidator.FocusOnPropertyName();
            (propertyNameValidators ??= []).Add(stringValidator);
        }

        if (propertyNameValidators == null)
            return null;
        
        return new PropertyNamesSchemaRuleValidator(propertyNameValidators.ToArray());
    }
}
