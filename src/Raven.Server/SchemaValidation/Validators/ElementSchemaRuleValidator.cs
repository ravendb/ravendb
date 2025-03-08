using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Server.SchemaValidation.Validators.Array;
using Raven.Server.SchemaValidation.Validators.Object;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

[DebuggerDisplay("'{SchemaPath}' property validator")]
public abstract class ElementSchemaRuleValidator<TParent, TAccessor>
{
    // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
    private readonly ISchemaRuleValidator[] _ruleValidators;
    private readonly BlittableJsonToken[] _typesRestriction;
    private readonly string[] _publicTypesRestriction;
    protected readonly SchemaPath SchemaPath;
    
    public BlittableJsonReaderObject SchemaDefinition { get; init; }

    // ReSharper disable once ConvertToPrimaryConstructor
    protected ElementSchemaRuleValidator(BlittableJsonToken[] typesRestriction, ISchemaRuleValidator[] ruleValidators, SchemaPath schemaPath)
    {
        _typesRestriction = typesRestriction;
        _publicTypesRestriction = _typesRestriction?.Select(SchemaValidationHelper.GetPublicType).Distinct().ToArray();
        _ruleValidators = ruleValidators;
        SchemaPath = schemaPath;
    }
    
    public bool Validate(TParent parent, TAccessor accessor, IErrorBuilder errorBuilder)
    {
        if (TryGetElement(parent, accessor, out var element) == false)
            return true;

        if (IsOfRequiredType(element.Type) == false)
        {
            errorBuilder?.AddError($"'{errorBuilder.Path}' should be of type '{string.Join("' or '", _publicTypesRestriction)}' but actual type is '{SchemaValidationHelper.GetPublicType(element.Type)}'.");
            return false;
        }
        
        return CheckAllValidators(element.Value, errorBuilder);
    }

    protected abstract bool TryGetElement(TParent parent, TAccessor accessor, out (BlittableJsonToken Type, object Value) element);

    private bool CheckAllValidators(object value, IErrorBuilder errorBuilder)
    {
        if (_ruleValidators == null)
            return true;

        var isValid = true;
        //TODO Maybe to filter _ruleValidators by argument type and avoid cast and checking inside ruleValidator.Validate
        foreach (var ruleValidator in _ruleValidators)
        {
            isValid &= ruleValidator.Validate(value, errorBuilder);
        }
        return isValid;
    }

    private bool IsOfRequiredType(BlittableJsonToken jsonToken) => _typesRestriction == null || _typesRestriction.Length == 0 || _typesRestriction.Contains(jsonToken);
}

public static class ElementSchemaRuleValidatorFactory
{
    public static SelfElementSchemaRuleValidator CreateSelfElementSchemaRuleValidator(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return TryReadSchema(schemaDefinition, schemaPath, out BlittableJsonToken[] typesRestriction, out ISchemaRuleValidator[] ruleValidators)
            ? new SelfElementSchemaRuleValidator(typesRestriction, ruleValidators, schemaPath) : 
            null;
    }
    
    public static ArrayItemSchemaRuleValidator CreateArrayItemSchemaRuleValidator(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return TryReadSchema(schemaDefinition, schemaPath, out BlittableJsonToken[] typesRestriction, out ISchemaRuleValidator[] ruleValidators)
            ? new ArrayItemSchemaRuleValidator(typesRestriction, ruleValidators, schemaPath){SchemaDefinition = schemaDefinition} : 
            null;
    }
    public static PropertySchemaRuleValidator CreatePropertySchemaRuleValidator(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return TryReadSchema(schemaDefinition, schemaPath, out BlittableJsonToken[] typesRestriction, out ISchemaRuleValidator[] ruleValidators)
            ? new PropertySchemaRuleValidator(typesRestriction, ruleValidators, schemaPath) : 
            null;
    }

    private static bool TryReadSchema(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        out BlittableJsonToken[] typesRestriction, out ISchemaRuleValidator[] ruleValidators)
    {
        if (schemaDefinition == null)
        {
            typesRestriction = null;
            ruleValidators = null;
            return false;
        }
        typesRestriction = ReadTypeRestrictionsRule(schemaDefinition, schemaPath);
        ruleValidators = ReadValueSchemaRuleValidators(schemaDefinition, schemaPath);
        return true;
    }

    private static BlittableJsonToken[] ReadTypeRestrictionsRule(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        if (schemaDefinition.TryGet(SchemaValidatorConstants.type, out object typesRestrictionSchema) == false)
            return null;

        schemaPath += SchemaValidatorConstants.type;
        var allowedTypes = new List<BlittableJsonToken>();
        if (typesRestrictionSchema is BlittableJsonReaderArray types)
        {
            foreach (var type in types)
            {
                allowedTypes.AddRange(ConvertTypeToTokens(type, schemaPath));
            }
        }
        else 
        {
            allowedTypes.AddRange(ConvertTypeToTokens(typesRestrictionSchema, schemaPath));
        }

        return allowedTypes.ToArray();
        // return _typesRestriction.Select(SchemaValidationHelper.GetPublicType).Distinct().ToArray();
    }
    
    private static BlittableJsonToken[] ConvertTypeToTokens(object type, SchemaPath schemaPath)
    {
        var stringType = GetLazyString(type, schemaPath);
        if(SchemaValidationHelper.TryGetTokensForType(stringType, out BlittableJsonToken[] tokens) == false)
        {
            throw new InvalidSchemaValidationDefinitionException(
                $"The '{SchemaValidatorConstants.type}' restriction must be one of the allowed types ('{string.Join("', '", SchemaValidationHelper.PublicTypes)}'), but found '{type}'. " +
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
    
    private static ISchemaRuleValidator[] ReadValueSchemaRuleValidators(BlittableJsonReaderObject propertySchemaDefinition, SchemaPath schemaPath)
    {
        List<ISchemaRuleValidator> ruleValidators = null;
        var hasObjectRestrictions = false;
        var hasArrayRestrictions = false;
        foreach (var rule in propertySchemaDefinition.GetPropertyNames())
        {
            if (rule is SchemaValidatorConstants.type or SchemaValidatorConstants.description)
                continue;

            ISchemaRuleValidator validator;
            switch (rule)
            {
                case SchemaValidatorConstants.properties or SchemaValidatorConstants.patternProperties or SchemaValidatorConstants.additionalProperties:
                {
                    if (hasObjectRestrictions)
                        continue;
                    validator = SchemaRuleValidatorFactoryHelper.CreateObjectValidator(propertySchemaDefinition, schemaPath);
                    hasObjectRestrictions = true;
                    break;
                }
                case SchemaValidatorConstants.prefixItems or SchemaValidatorConstants.items:
                {
                    if (hasArrayRestrictions)
                        continue;
                    validator = SchemaRuleValidatorFactoryHelper.CreateArrayValidator(propertySchemaDefinition, schemaPath);
                    hasArrayRestrictions = true;
                    break;
                }
                default:
                {
                    if (SchemaRuleValidatorFactoryHelper.TryCreateValidator(rule, propertySchemaDefinition, schemaPath, out validator) == false)
                        continue;
                    break;
                }
            }

            (ruleValidators??= []).Add(validator);
        }
        return ruleValidators?.ToArray();
    }
}
