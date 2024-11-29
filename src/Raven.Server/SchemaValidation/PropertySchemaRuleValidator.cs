using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public abstract class PropertySchemaRuleValidator
{
    private SchemaRuleValidator[] _ruleValidators;
    private BlittableJsonToken[] _typesRestriction;

    private readonly bool _isRequired;

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
                allowedTypes.AddRange(ConvertTypeToToken(type));
            }
        }
        else 
        {
            allowedTypes.AddRange(ConvertTypeToToken(typesRestriction));
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
            errorBuilder.AddError($"{path} should be of type {string.Join(",", _typesRestriction.Select(GetActualPublicTypeName))} but actual type is {GetActualPublicTypeName(token)}.");
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

    private bool TryGetPropertyType(BlittableJsonReaderObject parent, string property, out BlittableJsonToken token)
    {
        if (parent.TryGetPropertyType(new StringSegment(property), out var internalToken))
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

    private void ReadValueSchemaRuleValidators(BlittableJsonReaderObject propertySchemaDefinition)
    {
        List<SchemaRuleValidator> ruleValidators = null;
        foreach (var rule in propertySchemaDefinition.GetPropertyNames())
        {
            if (rule is SchemaValidatorConstants.type or SchemaValidatorConstants.description)
                continue;

            if(SchemaRuleValidatorFactory.TryCreateValidator(rule, propertySchemaDefinition, out SchemaRuleValidator validator) == false)
                //TODO To check if we want to collect all the errors and return a full report. Also some time we need to ignore a rule like if "maximum" defined "maximumExclusive" handled as part of it.
                continue;
            
            (ruleValidators??=new List<SchemaRuleValidator>()).Add(validator);
        }

        _ruleValidators = ruleValidators?.ToArray();
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
    
    private IEquatable<string> GetEquatable(object type)
    {
        return type switch
        {
            LazyStringValue lazyStringValue => lazyStringValue,
            LazyCompressedStringValue lazyCompressedStringValue => lazyCompressedStringValue.ToString(),
            // _ => throw new InvalidSchemaValidationDefinitionException($"Expected array or string for 'type', got {GetPublicType(type)}. Path '{_path}'.") TODO to add path for errors
            _ => throw new InvalidSchemaValidationDefinitionException($"Expected array or string for 'type', got {GetPublicType(type)}.")
        };
    }

    private string GetPublicType(object type)
    {
        throw new NotImplementedException();
    }
}
