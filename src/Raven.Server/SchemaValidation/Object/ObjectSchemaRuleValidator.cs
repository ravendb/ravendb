using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

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
            .ToDictionary(x => x.PropertySpecifier);
        _patternPropertiesSchemaRuleValidators = ReadPropertyValidators(schemaDefinition, SchemaValidatorConstants.patternProperties)?
            .Select(x => (new Regex(x.PropertySpecifier), x)).ToArray();

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
            if (propertySpecifiers.TryGet(propertySpecifier, out BlittableJsonReaderObject propertySchemaDefinition) == false)
            {
                //TODO To put better message
                Debug.Assert(false, "Should not happen");
                // ReSharper disable once HeuristicUnreachableCode
                continue;
            }

            // var path = $"{Path}.{propertyName}"; TODO To add path for errors
            //TODO To remove isRequired if not needed
            var validator = new PropertySchemaRuleValidator(propertySpecifier, _schemaPath);
            validator.Init(propertySchemaDefinition);
            (validators ??= new List<PropertySchemaRuleValidator>()).Add(validator);
        }

        return validators;
    }

    protected override void ValidateInternal(BlittableJsonReaderObject value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (_namedPropertySchemaRuleValidators != null)
        {
            foreach (var (prop, validator) in _namedPropertySchemaRuleValidators)
            {
                ValidateProperty(validator, value, prop, path, errorBuilder);
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
                    ValidateProperty(validator,value, prop, path, errorBuilder);
                }
            }

            if (hasValidator) 
                continue;
            
            var (allowed, additionalPropertiesValidator) = _additionalPropertiesSchemaRuleValidator;
            if (allowed == false)
            {
                errorBuilder.AddError($"The property '{prop}' at '{path}' is not defined and additional properties are not allowed.");
            }
            else if (additionalPropertiesValidator != null)
            {
                ValidateProperty(additionalPropertiesValidator, value, prop, path, errorBuilder);
            }
        }
    }

    private static void ValidateProperty(PropertySchemaRuleValidator validator, BlittableJsonReaderObject value, string prop, SchemaValidatorPath path,
        IErrorBuilder errorBuilder)
    {
        path.StepIn(prop);
        validator.Validate(value, prop, path, errorBuilder);
        path.StepOut();
    }
}
