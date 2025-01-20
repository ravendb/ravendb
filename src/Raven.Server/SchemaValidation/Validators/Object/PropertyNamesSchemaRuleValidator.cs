using System.Collections.Generic;
using Raven.Server.SchemaValidation.Validators.String;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

public class PropertyNamesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    SchemaRuleValidator<string>[] _propertyNameValidators;
    private readonly string _schemaPath;

    // ReSharper disable once ConvertToPrimaryConstructor
    public PropertyNamesSchemaRuleValidator(string schemaPath)
    {
        _schemaPath = schemaPath;
    }

    public void Init(BlittableJsonReaderObject propertyNamesSchemaDefinition)
    {
        List<SchemaRuleValidator<string>> propertyNameValidators = null;
        foreach (var rule in propertyNamesSchemaDefinition.GetPropertyNames())
        {
            if(SchemaRuleValidatorFactoryHelper.TryCreateValidator(rule, propertyNamesSchemaDefinition, _schemaPath, out var validator) == false)
                continue;

            if (validator is not StringSchemaRuleValidator stringValidator)
                throw new InvalidSchemaValidationDefinitionException(
                    $"The rule '{rule}' defined in 'propertyNames' at '{_schemaPath}' is invalid because it includes constraints that are not applicable to strings.");

            //TODO Maybe find more elegant way
            stringValidator.FocusOnPropertyName();
            (propertyNameValidators ??= new List<SchemaRuleValidator<string>>()).Add(stringValidator);
        }

        if (propertyNameValidators != null)
            _propertyNameValidators = propertyNameValidators.ToArray();
    }
    // ReSharper disable once ConvertToPrimaryConstructor
    protected override bool ValidateInternal(BlittableJsonReaderObject value, IErrorBuilder errorBuilder)
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

[SchemaRule(SchemaValidatorConstants.propertyNames)]
public class PropertyNamesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<PropertyNamesSchemaRuleValidator>
{
    public override PropertyNamesSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath, out var propertyNames) == false)
            return null;
        
        var validator = new PropertyNamesSchemaRuleValidator(schemaPath);
        validator.Init(propertyNames);
        return validator;
    }
}
