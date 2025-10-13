using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Object;

[DebuggerDisplay("'{_schemaPath}' object validator")]
public class ObjectSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly SchemaPath _schemaPath;
    private readonly LazyStringValue[] _exclude;
    private readonly Dictionary<LazyStringValue, ElementSchemaRuleValidator> _namedPropertyValidators;
    private readonly (Regex Regex, ElementSchemaRuleValidator Validator)[] _patternPropertiesValidators;
    private readonly (bool Allowed, ElementSchemaRuleValidator Validator) _additionalPropertiesValidator;

    // ReSharper disable once ConvertToPrimaryConstructor
    public ObjectSchemaRuleValidator(Dictionary<LazyStringValue, ElementSchemaRuleValidator> named, (Regex, ElementSchemaRuleValidator x)[] pattern, (bool IsAllowed, ElementSchemaRuleValidator Validator) additional, SchemaPath schemaPath, LazyStringValue[] exclude)
    {
        _namedPropertyValidators = named;
        _patternPropertiesValidators = pattern;
        _additionalPropertiesValidator = additional;
        _schemaPath = schemaPath;
        _exclude = exclude;

        if (exclude != null)
        {
            foreach (var lazyStringValue in exclude)
            {
                _namedPropertyValidators.Remove(lazyStringValue);
            }
        }
    }

    public override bool Validate(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
    {
        var isValid = true;
        if (_namedPropertyValidators != null)
        {
            foreach (var (prop, validator) in _namedPropertyValidators)
            {
                isValid &= ValidateProperty(validator, value, prop, errorBuilder);
                if (errorBuilder == null && isValid == false)
                    return false;
            }
        }

        var buffer = ArrayPool<char>.Shared.Rent(1024);
        try
        {
            for (int i = 0; i < value.Count; i++)
            {
                var propName = value.GetPropertyNameByIndex(i);
                if (_exclude?.Contains(propName) == true) 
                    continue;
                
                var hasValidator = _namedPropertyValidators != null && _namedPropertyValidators.ContainsKey(propName);
                if (_patternPropertiesValidators != null)
                {
                    foreach (var (regex, validator) in _patternPropertiesValidators)
                    {
                        if (propName.Length > buffer.Length)
                        {
                            ArrayPool<char>.Shared.Return(buffer);
                            buffer = ArrayPool<char>.Shared.Rent(propName.Length);
                        }
                        propName.TryCopyTo(buffer);
                        if(regex.IsMatch(buffer.AsSpan(0, propName.Length)) == false)
                            continue;
                
                        hasValidator = true;
                        var prop = default(BlittableJsonReaderObject.PropertyDetails);
                        value.GetPropertyByIndex(i, ref prop);
                        isValid &= ValidateProperty(validator, propName, prop.Value, errorBuilder);
                        if (errorBuilder == null && isValid == false)
                            return false;
                    }
                }

                if (hasValidator) 
                    continue;
            
                var (allowed, additionalPropertiesValidator) = _additionalPropertiesValidator;
                if (allowed == false)
                {
                    errorBuilder?.AddError($"The property '{propName}' at '{errorBuilder.Path}' is not defined and additional properties are not allowed.");
                    isValid = false;
                }
                else if (additionalPropertiesValidator != null)
                {
                    var prop = default(BlittableJsonReaderObject.PropertyDetails);
                    value.GetPropertyByIndex(i, ref prop);
                    isValid &= ValidateProperty(additionalPropertiesValidator, propName, prop.Value, errorBuilder);
                    if (errorBuilder == null && isValid == false)
                        return false;
                }
            }
        }
        finally
        {
            ArrayPool<char>.Shared.Return(buffer);
        }

        return isValid;
    }

    private bool ShouldExclude(LazyStringValue prop)
    {
        if(_exclude != null)
            return true;

        var ret = false;
        foreach (var ex in _exclude)
        {
            if(prop.Equals(ex))
            if (string.Equals(ex, prop, StringComparison.Ordinal))
            {
                ret = true;
                break;
            }
        }
        return false;
    }

    private static bool ValidateProperty(ElementSchemaRuleValidator validator, BlittableJsonReaderObject value, LazyStringValue prop,
        ErrorBuilder errorBuilder)
    {
        return value.TryGetMember((string)prop, out object propValue) == false || ValidateProperty(validator, prop, propValue, errorBuilder);
    }

    private static bool ValidateProperty(ElementSchemaRuleValidator validator, LazyStringValue prop, object propValue, ErrorBuilder errorBuilder)
    {
        errorBuilder?.Path.StepIn(prop);
        var isValid = validator.Validate(propValue, errorBuilder);
        errorBuilder?.Path.StepOut();
        return isValid;
    }
}

public class ObjectSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<ObjectSchemaRuleValidator>
{
    public override ObjectSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        var named = ReadPropertyValidators(schemaDefinition, schemaPath + SchemaValidatorConstants.Properties, refSchemas)?
            .ToDictionary(x => x.property, x => x.validator);
        var pattern = ReadPropertyValidators(schemaDefinition, schemaPath + SchemaValidatorConstants.PatternProperties, refSchemas)?
            .Select(x => (new Regex(x.property), x.validator)).ToArray();

        var additional = ReadAdditionalProperties(schemaDefinition, schemaPath, refSchemas);

        if (named == null && pattern == null && additional is { IsAllowed: true, Validator: null })
            return null;

        var exclude = ReadExcludeProperties(schemaDefinition, schemaPath);
        
        return new ObjectSchemaRuleValidator(named, pattern, additional, schemaPath, exclude);
    }
    
    private static (bool IsAllowed, ElementSchemaRuleValidator Validator) ReadAdditionalProperties(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
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
                var validator = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(additionalPropertiesSchema, schemaPath, refSchemas);
                return (true, validator);
            }
            default:
                var expectedTypes = new HashSet<Type> { typeof(bool), typeof(BlittableJsonReaderObject) };
                SchemaValidationHelper.ThrowRuleTypeError(additionalProperties, expectedTypes.ToHashSet(), schemaPath);
                return (false, null);
        }
    }
    
    private static LazyStringValue[] ReadExcludeProperties(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        const string rule = SchemaValidatorConstants.ExcludedProperties;
        schemaPath += rule;
        if(SchemaValidationHelper.TryGetArray(schemaDefinition, rule, schemaPath, out var excludeProperties) == false)
            return null;

        var excluded = new LazyStringValue[excludeProperties.Length];
        for (var i = 0; i < excludeProperties.Length; i++)
        {
            var item = excludeProperties[i];
            if (item is LazyStringValue str == false)
            {
                SchemaValidationHelper.ThrowRuleTypeError(item, typeof(LazyStringValue), schemaPath + $"[{i}]");
                return null; // never hit
            }
            
            excluded[i] = str;
        }

        return excluded;
    }

    private static List<(LazyStringValue property, ElementSchemaRuleValidator validator)> ReadPropertyValidators(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        RefSchemas refSchemas)
    {
        if(SchemaValidationHelper.TryGetObject(schemaDefinition, schemaPath.Property, schemaPath, out var propertySchema) == false)
            return null;

        List<(LazyStringValue property, ElementSchemaRuleValidator validator)> validators = null;
        var prop = default(BlittableJsonReaderObject.PropertyDetails);
        for (int i = 0; i < propertySchema.Count; i++)
        {
            propertySchema.GetPropertyByIndex(i, ref prop);
            var propertySchemaPath = schemaPath + prop.Name;

            var propertySchemaDefinition = SchemaValidationHelper.CheckTypeAndThrow<BlittableJsonReaderObject>(prop.Value, schemaPath);

            var validator = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(propertySchemaDefinition, propertySchemaPath, refSchemas);
            if(validator != null)
                (validators ??= []).Add((prop.Name, validator));
        }

        return validators;
    }
}
