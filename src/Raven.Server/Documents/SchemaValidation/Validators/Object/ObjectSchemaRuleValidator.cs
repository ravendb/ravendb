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
    private readonly HashSet<LazyStringValue> _exclude;
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
        _exclude = exclude?.ToHashSet();

        if (exclude != null && _namedPropertyValidators != null)
        {
            foreach (var lazyStringValue in exclude)
            {
                _namedPropertyValidators.Remove(lazyStringValue);
            }
        }
    }

    public override bool Validate(SchemaValidationContext context, BlittableJsonReaderObject value)
    {
        var isValid = true;
        if (_namedPropertyValidators != null)
        {
            foreach (var (prop, validator) in _namedPropertyValidators)
            {
                isValid &= ValidateProperty(context, validator, value, prop);
                if (context.ErrorBuilder == null && isValid == false)
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
                        if (regex.IsMatch(buffer.AsSpan(0, propName.Length)) == false)
                            continue;

                        hasValidator = true;
                        var prop = default(BlittableJsonReaderObject.PropertyDetails);
                        value.GetPropertyByIndex(i, ref prop);
                        isValid &= ValidateProperty(context, validator, propName, prop.Value);
                        if (context.ErrorBuilder == null && isValid == false)
                            return false;
                    }
                }

                if (hasValidator)
                    continue;

                var (allowed, additionalPropertiesValidator) = _additionalPropertiesValidator;
                if (allowed == false)
                {
                    if (context.ErrorBuilder != null)
                    {
                        var path = context.ErrorBuilder.Path;
                        path.StepIn(propName);
                        context.ErrorBuilder.AddError($"The property '{propName}' is not defined in the schema and additional properties are not allowed. Full path: '{path}'.");
                        path.StepOut();
                    }
                    isValid = false;
                }
                else if (additionalPropertiesValidator != null)
                {
                    var prop = default(BlittableJsonReaderObject.PropertyDetails);
                    value.GetPropertyByIndex(i, ref prop);
                    isValid &= ValidateProperty(context, additionalPropertiesValidator, propName, prop.Value);
                    if (context.ErrorBuilder == null && isValid == false)
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

    private static bool ValidateProperty(SchemaValidationContext context, ElementSchemaRuleValidator validator, BlittableJsonReaderObject value, LazyStringValue prop)
    {
        return value.TryGetMember((string)prop, out object propValue) == false || ValidateProperty(context, validator, prop, propValue);
    }

    private static bool ValidateProperty(SchemaValidationContext context, ElementSchemaRuleValidator validator, LazyStringValue prop, object propValue)
    {
        context.StepIn(prop);
        var isValid = validator.Validate(context, propValue);
        context.StepOut();
        return isValid;
    }
}

public class ObjectSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<ObjectSchemaRuleValidator>
{
    public override ObjectSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        var named = ReadPropertyValidators(context, schemaDefinition, schemaPath + SchemaValidatorConstants.Properties)?
            .ToDictionary(x => x.property, x => x.validator);
        var pattern = ReadPropertyValidators(context, schemaDefinition, schemaPath + SchemaValidatorConstants.PatternProperties)?
            .Select(x => (new Regex(x.property), x.validator)).ToArray();

        var additional = ReadAdditionalProperties(context, schemaDefinition, schemaPath);

        if (named == null && pattern == null && additional is { IsAllowed: true, Validator: null })
            return null;

        var exclude = ReadExcludeProperties(schemaDefinition, schemaPath);
        
        return new ObjectSchemaRuleValidator(named, pattern, additional, schemaPath, exclude);
    }
    
    private static (bool IsAllowed, ElementSchemaRuleValidator Validator) ReadAdditionalProperties(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
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
                var validator = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(context, additionalPropertiesSchema, schemaPath);
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
        if (SchemaValidationHelper.TryGetArray(schemaDefinition, rule, schemaPath, out var excludeProperties) == false)
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

    private static List<(LazyStringValue property, ElementSchemaRuleValidator validator)> ReadPropertyValidators(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, schemaPath.Property, schemaPath, out var propertySchema) == false)
            return null;

        List<(LazyStringValue property, ElementSchemaRuleValidator validator)> validators = null;
        var prop = default(BlittableJsonReaderObject.PropertyDetails);
        for (int i = 0; i < propertySchema.Count; i++)
        {
            propertySchema.GetPropertyByIndex(i, ref prop);
            var propertySchemaPath = schemaPath + prop.Name;

            var propertySchemaDefinition = SchemaValidationHelper.CheckTypeAndThrow<BlittableJsonReaderObject>(prop.Value, schemaPath);

            var validator = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(context, propertySchemaDefinition, propertySchemaPath);
            if (validator != null)
                (validators ??= []).Add((prop.Name, validator));
        }

        return validators;
    }
}
