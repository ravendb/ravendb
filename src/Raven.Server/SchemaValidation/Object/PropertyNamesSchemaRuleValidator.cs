using System.Collections.Generic;
using Raven.Server.SchemaValidation.String;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

public class PropertyNamesSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    public const string RuleName = "propertyNames";
    
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
            if(SchemaRuleValidatorFactory.TryCreateValidator(rule, propertyNamesSchemaDefinition, _schemaPath, out var validator) == false)
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
    protected override void ValidateInternal(BlittableJsonReaderObject value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (_propertyNameValidators == null)
            return;

        foreach (var propertyName in value.GetPropertyNames())
        {
            foreach (var validator in _propertyNameValidators)
            {
                validator.Validate(propertyName, path, errorBuilder);
            }
        }
    }
}

public class PropertyNamesSchemaRuleValidatorFactory : SchemaRuleValidatorFactory
{
    protected override string Rule => PropertyNamesSchemaRuleValidator.RuleName;

    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var propertyNamesType) == false)
            return null;
        var propertyNames = GetObjOrThrow(Rule, schemaDefinition, schemaPath, propertyNamesType);
        
        var validator = new PropertyNamesSchemaRuleValidator(schemaPath);
        validator.Init(propertyNames);
        return validator;
    }
}
