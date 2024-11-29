using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

public class ObjectSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private HashSet<string> _requiredHashSet;
    private Dictionary<string, SpecifiedPropertySchemaRuleValidator> _namedPropertySchemaRuleValidators;
    private (Regex Regex, SpecifiedPropertySchemaRuleValidator Validator)[] _patternPropertiesSchemaRuleValidators;
    private (bool Allowed, AdditionalPropertySchemaRuleValidator Validator) _additionalPropertiesSchemaRuleValidator;

    // ReSharper disable once ConvertToPrimaryConstructor
    protected ObjectSchemaRuleValidator(string path, string property, bool isRequired)
    {
    }

    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        _requiredHashSet = schemaDefinition.TryGet(SchemaValidatorConstants.required, out BlittableJsonReaderArray required)
            ? required.Select(x => x.ToString()).ToHashSet()
            : null;
        
        //TODO To create an informative error when fails to read
        _namedPropertySchemaRuleValidators = ReadPropertyValidators(schemaDefinition, SchemaValidatorConstants.properties)?
            .ToDictionary(x => x.PropertySpecifier);
        _patternPropertiesSchemaRuleValidators = ReadPropertyValidators(schemaDefinition, SchemaValidatorConstants.patternProperties)?
            .Select(x => (new Regex(x.PropertySpecifier), x)).ToArray();

        ReadAdditionalProperties(schemaDefinition);
    }

    private void ReadAdditionalProperties(BlittableJsonReaderObject schemaDefinition)
    {
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
                var validator = new AdditionalPropertySchemaRuleValidator();
                validator.Init(additionalPropertiesSchema);
                _additionalPropertiesSchemaRuleValidator = (true, validator);
                break;
            }
            default:
                //TODO To improve error message
                throw new InvalidSchemaValidationDefinitionException("The schema definition is invalid.");
        }
    }

    private static IEnumerable<SpecifiedPropertySchemaRuleValidator> ReadPropertyValidators(BlittableJsonReaderObject schemaDefinition, string name)
    {
        List<SpecifiedPropertySchemaRuleValidator> validators = null;

        if (schemaDefinition.TryGet(name, out BlittableJsonReaderObject properties))
        {
            foreach (var propertyName in properties.GetPropertyNames())
            {
                if (properties.TryGet(propertyName, out BlittableJsonReaderObject propertySchemaDefinition) == false)
                {
                    //TODO To put better message
                    Debug.Assert(false, "Should not happen");
                    continue;
                }

                // var path = $"{Path}.{propertyName}"; TODO To add path for errors
                //TODO To remove isRequired if not needed
                var validator = new SpecifiedPropertySchemaRuleValidator(propertyName);
                validator.Init(propertySchemaDefinition);
                (validators??=new List<SpecifiedPropertySchemaRuleValidator>()).Add(validator);
            }
        }
        
        return validators;
    }

    protected override void ValidateInternal(BlittableJsonReaderObject value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (_requiredHashSet != null)
        {
            foreach (var required in _requiredHashSet)
            {
                if(value.Contains(required))
                    continue;
                errorBuilder.AddError($"The required property '{required}' is missing at '{path}'.");
            }
        }

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
