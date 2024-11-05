using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

internal class PropertySchemaRuleValidator
{
    private readonly string _path;
    private readonly string _property;
    private readonly bool _isRequired;
    private BlittableJsonToken[] _typesRestriction;
    private SchemaRuleValidator[] _ruleValidators;

    // ReSharper disable once ConvertToPrimaryConstructor
    public PropertySchemaRuleValidator(string path, string property, bool isRequired) 
    {
        _path = path;
        _property = property;
        _isRequired = isRequired;
    }

    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
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
                allowedTypes.AddRange(ConvertTypeToToken(type));
            }
        }
        else 
        {
            allowedTypes.AddRange(ConvertTypeToToken(typesRestriction));
        }

        _typesRestriction = allowedTypes.ToArray();
    }

    private IEquatable<string> GetEquatable(object type)
    {
        return type switch
        {
            LazyStringValue lazyStringValue => lazyStringValue,
            LazyCompressedStringValue lazyCompressedStringValue => lazyCompressedStringValue.ToString(),
            _ => throw new InvalidSchemaValidationDefinitionException($"Expected array or string for 'type', got {GetPublicType(type)}. Path '{_path}'.")
        };
    }

    private IEnumerable<BlittableJsonToken> ConvertTypeToToken(object type)
    {
        var equatable = GetEquatable(type);
        //TODO To replace with constant
        if (equatable.Equals("string"))
        {
            yield return BlittableJsonToken.String;
            yield return BlittableJsonToken.CompressedString;
        }
        //TODO To replace with constant
        else if (equatable.Equals("integer"))
        {
            yield return BlittableJsonToken.Integer;
        }
        else if (equatable.Equals("object"))
        {
            yield return BlittableJsonToken.StartObject;
        }
        else if (equatable.Equals("boolean"))
        {
            yield return BlittableJsonToken.Boolean;
        }
        else if (equatable.Equals("null"))
        {
            yield return BlittableJsonToken.Null;
        }
    }

    private void ReadValueSchemaRuleValidators(BlittableJsonReaderObject propertySchemaDefinition)
    {
        var ruleValidators = new List<SchemaRuleValidator>();
        foreach (var rule in propertySchemaDefinition.GetPropertyNames())
        {
            if (rule is SchemaValidatorConstants.type or SchemaValidatorConstants.description)
                //TODO Check if there are more
                continue;

            if (propertySchemaDefinition.TryGet(rule, out object v) == false)
                //TODO Should not happen
                continue;

            var foundAdditionalInfoProps = new List<object> { v };

            if (ValueSchemaRuleValidatorFactory.TryGetValueSchemaRuleValidatorFactory(rule, out var validatorsInfo) == false)
                //Can happen if the restriction rule is an additional info for another rule
                continue;

            foreach (var validatorInfo in validatorsInfo)
            {
                foreach (string additionalInfoProp in validatorInfo.AdditionalInfoProps)
                {
                    if (propertySchemaDefinition.TryGet(additionalInfoProp, out object additionalInfoPropValue) == false)
                        continue;

                    foundAdditionalInfoProps.Add(additionalInfoPropValue);
                }

                if(validatorInfo.TryCreate(_path, foundAdditionalInfoProps.ToArray(), out var validator) == false)
                    continue;
                ruleValidators.Add(validator);
            }
        }

        _ruleValidators = ruleValidators.ToArray();
    }

    private string GetPublicType(object type)
    {
        throw new NotImplementedException();
    }

    public void Validate(BlittableJsonReaderObject parent, IErrorBuilder errorBuilder)
    {
        if (TryGetPropertyType(parent, out BlittableJsonToken token) == false)
        {
            if (_isRequired)
                //TODO To improve the error message
                errorBuilder.AddError($"{_path} is required");

            return;
        }

        if (IsOfRequiredType(token) == false)
        {
            errorBuilder.AddError($"{_path} should be of type {string.Join(",", _typesRestriction.Select(GetActualPublicTypeName))} but actual type is {GetActualPublicTypeName(token)}.");
            return;
        }

        var value = parent[_property];
        //TODO Maybe to filter _ruleValidators by afgument type and avoid cast and checking inside ruleValidator.Validate
        foreach (var ruleValidator in _ruleValidators)
        {
            ruleValidator.Validate(value, errorBuilder);
        }
    }

    private bool TryGetPropertyType(BlittableJsonReaderObject parent, out BlittableJsonToken token)
    {
        if (parent.TryGetPropertyType(new StringSegment(_property), out var internalToken))
        {
            token = internalToken & BlittableJsonReaderBase.TypesMask;
            return true;
        }

        token = 0;
        return false;
    }

    private string GetActualPublicTypeName(BlittableJsonToken token)
     {
         if (token == BlittableJsonToken.Integer)
             return "Integer";
         
         if(token is BlittableJsonToken.String or BlittableJsonToken.CompressedString)
             return "String";

         if(token == BlittableJsonToken.LazyNumber)
             return "Number";

         if (token == BlittableJsonToken.Boolean)
             return "Boolean";

         if (token == BlittableJsonToken.StartObject)
             return "Object";

         if(token == BlittableJsonToken.StartArray)
             return "Array";

         if(token == BlittableJsonToken.Null)
             return "Null";

         //TODO To think about the error message
         throw new InvalidOperationException("some error");
     }
    
    private bool IsOfRequiredType(BlittableJsonToken jsonToken) => _typesRestriction == null || _typesRestriction.Contains(jsonToken);
}
