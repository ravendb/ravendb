using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[DebuggerDisplay("'{_schemaPath}' object validator")]
public class ObjectSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly SchemaPath _schemaPath;
    private readonly Dictionary<string, PropertySchemaRuleValidator> _namedPropertyValidators;
    private readonly (Regex Regex, PropertySchemaRuleValidator Validator)[] _patternPropertiesValidators;
    private readonly (bool Allowed, PropertySchemaRuleValidator Validator) _additionalPropertiesValidator;

    // ReSharper disable once ConvertToPrimaryConstructor
    public ObjectSchemaRuleValidator(Dictionary<string, PropertySchemaRuleValidator> named, (Regex, PropertySchemaRuleValidator x)[] pattern, (bool IsAllowed, PropertySchemaRuleValidator Validator) additional, SchemaPath schemaPath)
    {
        _namedPropertyValidators = named;
        _patternPropertiesValidators = pattern;
        _additionalPropertiesValidator = additional;
        _schemaPath = schemaPath;
    }

    protected override bool ValidateInternal(BlittableJsonReaderObject value, IErrorBuilder errorBuilder)
    {
        var isValid = true;
        if (_namedPropertyValidators != null)
        {
            foreach (var (prop, validator) in _namedPropertyValidators)
            {
                isValid&= ValidateProperty(validator, value, prop, errorBuilder);
            }
        }

        foreach (string prop in value.GetPropertyNames())
        {
            var hasValidator = _namedPropertyValidators != null && _namedPropertyValidators.ContainsKey(prop);
            if (_patternPropertiesValidators != null)
            {
                foreach (var (regex, validator) in _patternPropertiesValidators)
                {
                    if(regex.IsMatch(prop) == false)
                        continue;
                
                    hasValidator = true;
                    isValid &= ValidateProperty(validator,value, prop, errorBuilder);
                }
            }

            if (hasValidator) 
                continue;
            
            var (allowed, additionalPropertiesValidator) = _additionalPropertiesValidator;
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

public class ObjectSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<ObjectSchemaRuleValidator>
{
    public override ObjectSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        //TODO To create an informative error when fails to read
        var named = ReadPropertyValidators(schemaDefinition, schemaPath + SchemaValidatorConstants.properties)?
            .ToDictionary(x => x.Property);
        var pattern = ReadPropertyValidators(schemaDefinition, schemaPath + SchemaValidatorConstants.patternProperties)?
            .Select(x => (new Regex(x.Property), x)).ToArray();

        var additional = ReadAdditionalProperties(schemaDefinition, schemaPath);

        if (named == null && pattern == null && additional is { IsAllowed: true, Validator: null })
            return null;
        
        var validator = new ObjectSchemaRuleValidator(named, pattern, additional, schemaPath);
        return validator;
    }
    
    private static (bool IsAllowed, PropertySchemaRuleValidator Validator) ReadAdditionalProperties(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        const string rule = SchemaValidatorConstants.additionalProperties;
        if (schemaDefinition.TryGet(rule, out object additionalProperties) == false)
        {
            return (true, null);
        }
        schemaPath += rule;
        
        switch (additionalProperties)
        {
            case bool isAdditionalPropertiesAllowed:
                return (isAdditionalPropertiesAllowed, null);
            case BlittableJsonReaderObject additionalPropertiesSchema:
            {
                var validator = ElementSchemaRuleValidatorFactory.CreatePropertySchemaRuleValidator(additionalPropertiesSchema, schemaPath);
                return (true, validator);
            }
            default:
                //TODO To improve error message
                throw new InvalidSchemaValidationDefinitionException(
                    $"The value of '{rule}' must be a 'boolean' or an 'object', but received a value of type '{SchemaValidationHelper.GetPublicTypeOfObj(additionalProperties)}'. Schema path '{schemaPath}'."
                );
        }
    }

    private static List<PropertySchemaRuleValidator> ReadPropertyValidators(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        if (schemaDefinition.TryGet(schemaPath.Property, out object readPropertySpecifiers) == false) 
            return null;

        if (readPropertySpecifiers is BlittableJsonReaderObject propertySpecifiers == false)
            throw new InvalidSchemaValidationDefinitionException($"The value of '{schemaPath.Property}' must be an 'object', but received a value of type '{SchemaValidationHelper.GetPublicTypeOfObj(readPropertySpecifiers)}'. Schema path '{schemaPath}'.");
            
        List<PropertySchemaRuleValidator> validators = null;
        foreach (var propertySpecifier in propertySpecifiers.GetPropertyNames())
        {
            var propertySchemaPath = schemaPath + propertySpecifier;
            SchemaValidationHelper.TryGetObject(propertySpecifiers, propertySpecifier, propertySchemaPath.FullPath, out var propertySchemaDefinition);

            var validator = ElementSchemaRuleValidatorFactory.CreatePropertySchemaRuleValidator(propertySchemaDefinition, propertySchemaPath);
            (validators ??= []).Add(validator);
        }

        return validators;
    }
}
