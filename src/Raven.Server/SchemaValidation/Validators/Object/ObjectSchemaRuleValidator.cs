using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Server.SchemaValidation.ErrorMessage;
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

    public override bool Validate(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
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
        ErrorBuilder errorBuilder)
    {
        
        errorBuilder?.Path.StepIn(prop);
        var isValid = validator.Validate(value, prop, errorBuilder);
        errorBuilder?.Path.StepOut();
        return isValid;
    }
}

public class ObjectSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<ObjectSchemaRuleValidator>
{
    public override ObjectSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        var named = ReadPropertyValidators(schemaDefinition, schemaPath + SchemaValidatorConstants.Properties, refSchemas)?
            .ToDictionary(x => x.Property);
        var pattern = ReadPropertyValidators(schemaDefinition, schemaPath + SchemaValidatorConstants.PatternProperties, refSchemas)?
            .Select(x => (new Regex(x.Property), x)).ToArray();

        var additional = ReadAdditionalProperties(schemaDefinition, schemaPath, refSchemas);

        if (named == null && pattern == null && additional is { IsAllowed: true, Validator: null })
            return null;
        
        return new ObjectSchemaRuleValidator(named, pattern, additional, schemaPath);
    }
    
    private static (bool IsAllowed, PropertySchemaRuleValidator Validator) ReadAdditionalProperties(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        RefSchemas refSchemas)
    {
        const string rule = SchemaValidatorConstants.AdditionalProperties;
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
                var validator = ElementSchemaRuleValidatorFactory.CreatePropertySchemaRuleValidator(additionalPropertiesSchema, schemaPath, refSchemas);
                return (true, validator);
            }
            default:
                SchemaValidationHelper.TrowRuleTypeError(
                    rule, additionalProperties, [BlittableJsonToken.Boolean, BlittableJsonToken.StartObject], SchemaValidationHelper.GetPublicTypeOfObj(additionalProperties), schemaPath.FullPath);
                return (false, null);
        }
    }

    private static List<PropertySchemaRuleValidator> ReadPropertyValidators(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        RefSchemas refSchemas)
    {
        if(SchemaValidationHelper.TryGetObject(schemaDefinition, schemaPath.Property, schemaPath.FullPath, out var propertySchema) == false)
            return null;

        List<PropertySchemaRuleValidator> validators = null;
        foreach (var propertySpecifier in propertySchema.GetPropertyNames())
        {
            var propertySchemaPath = schemaPath + propertySpecifier;
            SchemaValidationHelper.TryGetObject(propertySchema, propertySpecifier, propertySchemaPath.FullPath, out var propertySchemaDefinition);

            var validator = ElementSchemaRuleValidatorFactory.CreatePropertySchemaRuleValidator(propertySchemaDefinition, propertySchemaPath, refSchemas);
            if(validator != null)
                (validators ??= []).Add(validator);
        }

        return validators;
    }
}
