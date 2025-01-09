using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[DebuggerDisplay("'{_schemaPath}' object validator")]
public class ObjectSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly string _schemaPath;
    private Dictionary<string, PropertySchemaRuleValidator> _namedPropertySchemaRuleValidators;
    private (Regex Regex, PropertySchemaRuleValidator Validator)[] _patternPropertiesSchemaRuleValidators;
    private (bool Allowed, PropertySchemaRuleValidator Validator) _additionalPropertiesSchemaRuleValidator;

    // ReSharper disable once ConvertToPrimaryConstructor
    public ObjectSchemaRuleValidator(string schemaPath)
    {
        _schemaPath = schemaPath;
    }
    
    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        //TODO To create an informative error when fails to read
        _namedPropertySchemaRuleValidators = ReadPropertyValidators(schemaDefinition, SchemaValidatorConstants.properties)?
            .ToDictionary(x => x.Accessor);
        _patternPropertiesSchemaRuleValidators = ReadPropertyValidators(schemaDefinition, SchemaValidatorConstants.patternProperties)?
            .Select(x => (new Regex(x.Accessor), x)).ToArray();

        ReadAdditionalProperties(schemaDefinition);
    }

    private void ReadAdditionalProperties(BlittableJsonReaderObject schemaDefinition)
    {
        //TODO Maybe change the schema path to full path
        const string propertySpecifier = "#/additionalProperties"; //Used for error messages

        if (schemaDefinition.TryGet(SchemaValidatorConstants.additionalProperties, out object additionalProperties) == false)
        {
            _additionalPropertiesSchemaRuleValidator = (true, null);
            return;
        }

        switch (additionalProperties)
        {
            case bool isAdditionalPropertiesAllowed:
                _additionalPropertiesSchemaRuleValidator = (isAdditionalPropertiesAllowed, null);
                break;
            case BlittableJsonReaderObject additionalPropertiesSchema:
            {
                var validator = new PropertySchemaRuleValidator(propertySpecifier, _schemaPath);
                validator.Init(additionalPropertiesSchema);
                _additionalPropertiesSchemaRuleValidator = (true, validator);
                break;
            }
            default:
                //TODO To improve error message
                throw new InvalidSchemaValidationDefinitionException(
                    $"The value of 'additionalProperties' at '{_schemaPath}' must be a boolean or an object, but received a value of type '{SchemaValidationHelper.GetPublicTypeOfObj(additionalProperties)}'."
                );
        }
    }

    private List<PropertySchemaRuleValidator> ReadPropertyValidators(BlittableJsonReaderObject schemaDefinition, string name)
    {
        if (schemaDefinition.TryGet(name, out object readPropertySpecifiers) == false) 
            return null;

        if (readPropertySpecifiers is BlittableJsonReaderObject propertySpecifiers == false)
            throw new InvalidSchemaValidationDefinitionException($"The value of '{name}' at '{_schemaPath}' must be an object, but received a value of type '{SchemaValidationHelper.GetPublicTypeOfObj(readPropertySpecifiers)}'.");
            
        List<PropertySchemaRuleValidator> validators = null;
        foreach (var propertySpecifier in propertySpecifiers.GetPropertyNames())
        {
            SchemaValidationHelper.TryGetObject(propertySpecifiers, propertySpecifier, _schemaPath, out var propertySchemaDefinition);

            var validator = new PropertySchemaRuleValidator(propertySpecifier, _schemaPath);
            validator.Init(propertySchemaDefinition);
            (validators ??= new List<PropertySchemaRuleValidator>()).Add(validator);
        }

        return validators;
    }

    protected override bool ValidateInternal(BlittableJsonReaderObject value, IErrorBuilder errorBuilder)
    {
        var isValid = true;
        if (_namedPropertySchemaRuleValidators != null)
        {
            foreach (var (prop, validator) in _namedPropertySchemaRuleValidators)
            {
                isValid&= ValidateProperty(validator, value, prop, errorBuilder);
            }
        }

        foreach (string prop in value.GetPropertyNames())
        {
            var hasValidator = _namedPropertySchemaRuleValidators != null && _namedPropertySchemaRuleValidators.ContainsKey(prop);
            if (_patternPropertiesSchemaRuleValidators != null)
            {
                foreach (var (regex, validator) in _patternPropertiesSchemaRuleValidators)
                {
                    if(regex.IsMatch(prop) == false)
                        continue;
                
                    hasValidator = true;
                    isValid &= ValidateProperty(validator,value, prop, errorBuilder);
                }
            }

            if (hasValidator) 
                continue;
            
            var (allowed, additionalPropertiesValidator) = _additionalPropertiesSchemaRuleValidator;
            if (allowed == false)
            {
                errorBuilder?.AddError($"The property '{prop}' at '{errorBuilder.Path}' is not defined and additional properties are not allowed.");
                isValid = false;
            }
            else if (additionalPropertiesValidator != null)
            {
                isValid &= ValidateProperty(additionalPropertiesValidator, value, prop, errorBuilder);
            }
        }
        return isValid;
    }

    private static bool ValidateProperty(PropertySchemaRuleValidator validator, BlittableJsonReaderObject value, string prop,
        IErrorBuilder errorBuilder)
    {
        
        errorBuilder?.Path.StepIn(prop);
        var isValid = validator.Validate(value, prop, errorBuilder);
        errorBuilder?.Path.StepOut();
        return isValid;
    }
}
