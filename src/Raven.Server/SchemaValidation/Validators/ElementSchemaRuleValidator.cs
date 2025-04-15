using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Server.SchemaValidation.ErrorMessage;
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
    
    public bool Validate(TParent parent, TAccessor accessor, ErrorBuilder errorBuilder)
    {
        if (TryGetElement(parent, accessor, out var element) == false)
            return true;

        if (IsOfRequiredType(element.Type) == false)
        {
            errorBuilder?.AddError($"'{errorBuilder.Path}' should be of type '{_publicTypesRestriction:' or '}' but actual type is '{SchemaValidationHelper.GetPublicType(element.Type)}'.");
            return false;
        }
        
        return CheckAllValidators(element.Value, errorBuilder);
    }

    protected abstract bool TryGetElement(TParent parent, TAccessor accessor, out (BlittableJsonToken Type, object Value) element);

    private bool CheckAllValidators(object value, ErrorBuilder errorBuilder)
    {
        if (_ruleValidators == null)
            return true;

        var isValid = true;
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
    public static SelfObjectElementSchemaRuleValidator CreateSelfElementSchemaRuleValidator(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        RefSchemas refSchemas)
    {
        return TryReadSchema(schemaDefinition, schemaPath, refSchemas, out BlittableJsonToken[] typesRestriction, out ISchemaRuleValidator[] ruleValidators)
            ? new SelfObjectElementSchemaRuleValidator(typesRestriction, ruleValidators, schemaPath) : 
            null;
    }
    
    
    public static ArrayItemSchemaRuleValidator CreateArrayItemSchemaRuleValidator(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        RefSchemas refSchemas)
    {
        return TryReadSchema(schemaDefinition, schemaPath, refSchemas, out BlittableJsonToken[] typesRestriction, out ISchemaRuleValidator[] ruleValidators)
            ? new ArrayItemSchemaRuleValidator(typesRestriction, ruleValidators, schemaPath){SchemaDefinition = schemaDefinition} : 
            null;
    }
    public static PropertySchemaRuleValidator CreatePropertySchemaRuleValidator(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        RefSchemas refSchemas)
    {
        return TryReadSchema(schemaDefinition, schemaPath, refSchemas, out BlittableJsonToken[] typesRestriction, out ISchemaRuleValidator[] ruleValidators)
            ? new PropertySchemaRuleValidator(typesRestriction, ruleValidators, schemaPath) : 
            null;
    }

    public static bool TryReadSchema(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        RefSchemas refSchemas,
        out BlittableJsonToken[] typesRestriction, out ISchemaRuleValidator[] ruleValidators)
    {
        if (schemaDefinition == null)
        {
            typesRestriction = null;
            ruleValidators = null;
            return false;
        }
        typesRestriction = ReadTypeRestrictionsRule(schemaDefinition, schemaPath);
        ruleValidators = ReadValueSchemaRuleValidators(schemaDefinition, schemaPath, refSchemas);

        if (SchemaValidationHelper.TryGetString(schemaDefinition, SchemaValidatorConstants.Ref, schemaPath.ToString(), out var @ref))
        {
            if (refSchemas.TryGet(@ref, out var refSchema) == false)
                throw new InvalidSchemaValidationDefinitionException(
                    $"The reference '{@ref}' at '{schemaPath}' does not match any defined subschema.");

            typesRestriction = Concat(typesRestriction, refSchema.TypesRestriction)?.Distinct().ToArray();
            ruleValidators = Concat(ruleValidators, refSchema.RuleValidators);
        }
        
        return true;

        T[] Concat<T>(T[] first, T[] second) => first == null ? second 
            : second == null ? first : first.Concat(second).ToArray();
    }

    private static BlittableJsonToken[] ReadTypeRestrictionsRule(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        if (schemaDefinition.TryGet(SchemaValidatorConstants.Type, out object typesRestrictionSchema) == false)
            return null;

        schemaPath += SchemaValidatorConstants.Type;
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
    }
    
    private static BlittableJsonToken[] ConvertTypeToTokens(object type, SchemaPath schemaPath)
    {
        var stringType = GetLazyString(type, schemaPath);
        if(SchemaValidationHelper.TryGetTokensForType(stringType, out BlittableJsonToken[] tokens) == false)
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
                case SchemaValidatorConstants.Ref:
                {
                    continue;
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
