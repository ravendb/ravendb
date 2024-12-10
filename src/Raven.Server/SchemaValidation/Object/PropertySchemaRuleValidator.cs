using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

[DebuggerDisplay("'{_schemaPath}' property validator")]
public class PropertySchemaRuleValidator
{
    private readonly string _schemaPath;
    private ISchemaRuleValidator[] _ruleValidators;
    private BlittableJsonToken[] _typesRestriction;

    public string PropertySpecifier { get; }
    
    // ReSharper disable once ConvertToPrimaryConstructor
    public PropertySchemaRuleValidator(string propertySpecifier, string schemaPath)
    {
        PropertySpecifier = propertySpecifier;
        _schemaPath = string.IsNullOrEmpty(schemaPath) ? propertySpecifier : $"{schemaPath}.{propertySpecifier}";
    }
    
    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        if(schemaDefinition == null)
            return;
        ReadTypeRestrictionsRule(schemaDefinition);
        ReadValueSchemaRuleValidators(schemaDefinition);
    }
    
    private void ReadTypeRestrictionsRule(BlittableJsonReaderObject schemaDefinition)
    {
        if (schemaDefinition.TryGet(SchemaValidatorConstants.type, out object typesRestriction) == false) 
            return;
        
        var allowedTypes = new List<BlittableJsonToken>();
        if (typesRestriction is BlittableJsonReaderArray types)
        {
            foreach (var type in types)
            {
                allowedTypes.AddRange(ConvertTypeToTokens(type));
            }
        }
        else 
        {
            allowedTypes.AddRange(ConvertTypeToTokens(typesRestriction));
        }

        _typesRestriction = allowedTypes.ToArray();
    }
    
    public void Validate(BlittableJsonReaderObject parent, string property, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (TryGetPropertyType(parent, property, out BlittableJsonToken token) == false)
        {
            //TODO Maybe to check _isRequired also here
            // if (_isRequired)
                // TODO To improve the error message
                // errorBuilder.AddError($"The required property '{property}' is missing at '{path}'.");

            return;
        }

        if (IsOfRequiredType(token) == false)
        {
            errorBuilder.AddError($"'{path}' should be of type '{string.Join("' or '", _typesRestriction.Select(SchemaValidationHelper.GetPublicType))}' but actual type is '{SchemaValidationHelper.GetPublicType(token)}'.");
            return;
        }
        
        var value = parent[property];
        if (_ruleValidators == null)
            return;
        
        //TODO Maybe to filter _ruleValidators by afgument type and avoid cast and checking inside ruleValidator.Validate
        foreach (var ruleValidator in _ruleValidators)
        {
            ruleValidator.Validate(value, path, errorBuilder);
        }
    }

    private static bool TryGetPropertyType(BlittableJsonReaderObject parent, string property, out BlittableJsonToken token)
    {
        if (parent.TryGetPropertyType(new StringSegment(property), out var internalToken))
        {
            token = internalToken & BlittableJsonReaderBase.TypesMask;
            return true;
        }

        token = 0;
        return false;
    }
    
    private bool IsOfRequiredType(BlittableJsonToken jsonToken) => _typesRestriction == null || _typesRestriction.Length == 0 || _typesRestriction.Contains(jsonToken);

    private void ReadValueSchemaRuleValidators(BlittableJsonReaderObject propertySchemaDefinition)
    {
        List<ISchemaRuleValidator> ruleValidators = null;
        var hasObjectRestrictions = false;
        foreach (var rule in propertySchemaDefinition.GetPropertyNames())
        {
            if (rule is SchemaValidatorConstants.type or SchemaValidatorConstants.description)
                continue;

            if (rule is SchemaValidatorConstants.properties or SchemaValidatorConstants.patternProperties or SchemaValidatorConstants.additionalProperties or SchemaValidatorConstants.required)
            {
                hasObjectRestrictions = true;
                continue;
            }
            
            if(SchemaRuleValidatorFactory.TryCreateValidator(rule, propertySchemaDefinition, _schemaPath, out var validator) == false)
            {
                continue;
            }
            //TODO To check if we want to collect all the errors and return a full report. Also some time we need to ignore a rule like if "maximum" defined "maximumExclusive" handled as part of it.
            (ruleValidators??=new List<ISchemaRuleValidator>()).Add(validator);
        }

        if (hasObjectRestrictions)
        {
            var objValidator = new ObjectSchemaRuleValidator(_schemaPath);
            objValidator.Init(propertySchemaDefinition);
            (ruleValidators??=new List<ISchemaRuleValidator>()).Add(objValidator);
        }
        _ruleValidators = ruleValidators?.ToArray();
    }

    private BlittableJsonToken[] ConvertTypeToTokens(object type)
    {
        var stringType = GetLazyString(type);
        if(SchemaValidationHelper.TryConvertTypeToTokens(stringType, out BlittableJsonToken[] tokens) == false)
        {
            throw new InvalidSchemaValidationDefinitionException(
                $"The '{SchemaValidatorConstants.type}' restriction must be one of the allowed types ({string.Join(", ", SchemaValidationHelper.PublicTypes)}), but found '{type}'. " +
                $"Path: '{_schemaPath}'.");
        }
        return tokens;
    }

    private LazyStringValue GetLazyString(object type)
    {
        return type switch
        {
            LazyStringValue lazyStringValue => lazyStringValue,
            LazyCompressedStringValue lazyCompressedStringValue => lazyCompressedStringValue.ToLazyStringValue(),
            _ => throw new InvalidSchemaValidationDefinitionException($"Expected a value of type 'string' for 'type', but received '{SchemaValidationHelper.GetPublicTypeOfObj(type)}' of type '{type}' at path '{_schemaPath}'.")
        };
    }
}
