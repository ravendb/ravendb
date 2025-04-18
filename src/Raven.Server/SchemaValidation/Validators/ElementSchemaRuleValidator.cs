using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

[DebuggerDisplay("'{SchemaPath}' property validator")]
public class ElementSchemaRuleValidator
{
    // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
    private readonly ISchemaRuleValidator[] _ruleValidators;
    private readonly Type[] _typesRestriction;
    private readonly string[] _publicTypesRestriction;
    protected readonly SchemaPath SchemaPath;
    
    public BlittableJsonReaderObject SchemaDefinition { get; init; }

    // ReSharper disable once ConvertToPrimaryConstructor
    public ElementSchemaRuleValidator(Type[] typesRestriction, ISchemaRuleValidator[] ruleValidators, SchemaPath schemaPath)
    {
        _typesRestriction = typesRestriction;
        _publicTypesRestriction = _typesRestriction?.Select(SchemaValidationHelper.GetPublicType).Distinct().ToArray();
        _ruleValidators = ruleValidators;
        SchemaPath = schemaPath;
    }
    
    public bool Validate(object value, ErrorBuilder errorBuilder)
    {
        if (IsOfRequiredType(value) == false)
        {
            errorBuilder?.AddError($"'{errorBuilder.Path}' should be of type '{_publicTypesRestriction:' or '}' but actual type is '{SchemaValidationHelper.GetPublicType(value.GetType())}'.");
            return false;
        }
        
        return CheckAllValidators(value, errorBuilder);
    }

    private bool CheckAllValidators(object value, ErrorBuilder errorBuilder)
    {
        if (_ruleValidators == null)
            return true;

        var isValid = true;
        foreach (var ruleValidator in _ruleValidators)
        {
            isValid &= ruleValidator.Validate(value, errorBuilder);
            if(errorBuilder == null && isValid == false)
                return false;
        }
        return isValid;
    }

    private bool IsOfRequiredType(object obj) => _typesRestriction == null || _typesRestriction.Length == 0 || _typesRestriction.Contains(obj?.GetType());
}

public static class ElementSchemaRuleValidatorFactory
{
    public static ElementSchemaRuleValidator CreateElementSchemaRuleValidator(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        RefSchemas refSchemas)
    {
        return TryReadSchema(schemaDefinition, schemaPath, refSchemas, out Type[] typesRestriction, out ISchemaRuleValidator[] ruleValidators)
            ? new ElementSchemaRuleValidator(typesRestriction, ruleValidators, schemaPath) {SchemaDefinition = schemaDefinition} : 
            null;
    }
    
    private static bool TryReadSchema(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        RefSchemas refSchemas,
        out Type[] typesRestriction, out ISchemaRuleValidator[] ruleValidators)
    {
        if (schemaDefinition == null)
        {
            typesRestriction = null;
            ruleValidators = null;
            return false;
        }
        typesRestriction = ReadTypeRestrictionsRule(schemaDefinition, schemaPath);
        ruleValidators = ReadValueSchemaRuleValidators(schemaDefinition, schemaPath, refSchemas);

        return true;
    }

    private static Type[] ReadTypeRestrictionsRule(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        if (schemaDefinition.TryGet(SchemaValidatorConstants.Type, out object typesRestrictionSchema) == false)
            return null;

        schemaPath += SchemaValidatorConstants.Type;
        var allowedTypes = new List<Type>();
        if (typesRestrictionSchema is BlittableJsonReaderArray types)
        {
            foreach (var type in types)
            {
                allowedTypes.AddRange(ConvertToTypes(type, schemaPath));
            }
        }
        else 
        {
            allowedTypes.AddRange(ConvertToTypes(typesRestrictionSchema, schemaPath));
        }

        return allowedTypes.ToArray();
    }
    
    private static Type[] ConvertToTypes(object type, SchemaPath schemaPath)
    {
        var stringType = GetLazyString(type, schemaPath);
        if(SchemaValidationHelper.TryGetTokensForType(stringType, out Type[] tokens) == false)
        {
            throw new InvalidSchemaValidationDefinitionException(
                $"The '{SchemaValidatorConstants.Type}' restriction must be one of the allowed types ('{string.Join("', '", SchemaValidationHelper.PublicTypes)}'), but found '{type}'. " +
                $"Schema path: '{schemaPath}'.");
        }
        return tokens;
    }
    
    private static LazyStringValue GetLazyString(object type, SchemaPath schemaPath)
    {
        return type switch
        {
            LazyStringValue lazyStringValue => lazyStringValue,
            LazyCompressedStringValue lazyCompressedStringValue => lazyCompressedStringValue.ToLazyStringValue(),
            _ => throw new InvalidSchemaValidationDefinitionException($"Expected a value of type 'string' for 'type', but received '{SchemaValidationHelper.GetPublicTypeOfObj(type)}' of type '{type}' at path '{schemaPath}'.")
        };
    }
    
    private static ISchemaRuleValidator[] ReadValueSchemaRuleValidators(BlittableJsonReaderObject propertySchemaDefinition, SchemaPath schemaPath,
        RefSchemas refSchemas)
    {
        List<ISchemaRuleValidator> ruleValidators = null;
        var alreadyHasObjectRestrictions = false;
        var alreadyHasArrayRestrictions = false;
        foreach (var rule in propertySchemaDefinition.GetPropertyNames())
        {
            if (rule is SchemaValidatorConstants.Type or SchemaValidatorConstants.Description)
                continue;

            ISchemaRuleValidator validator;
            switch (rule)
            {
                case SchemaValidatorConstants.Properties or SchemaValidatorConstants.PatternProperties or SchemaValidatorConstants.AdditionalProperties:
                {
                    if (alreadyHasObjectRestrictions)
                        continue;
                    validator = SchemaRuleValidatorFactoryHelper.CreateObjectValidator(propertySchemaDefinition, schemaPath, refSchemas);
                    alreadyHasObjectRestrictions = true;
                    break;
                }
                case SchemaValidatorConstants.PrefixItems or SchemaValidatorConstants.Items:
                {
                    if (alreadyHasArrayRestrictions)
                        continue;
                    validator = SchemaRuleValidatorFactoryHelper.CreateArrayValidator(propertySchemaDefinition, schemaPath, refSchemas);
                    alreadyHasArrayRestrictions = true;
                    break;
                }
                default:
                {
                    if (SchemaRuleValidatorFactoryHelper.TryCreateValidator(rule, propertySchemaDefinition, schemaPath, refSchemas, out validator) == false)
                        continue;
                    break;
                }
            }

            (ruleValidators??= []).Add(validator);
        }
        return ruleValidators?.ToArray();
    }
}
