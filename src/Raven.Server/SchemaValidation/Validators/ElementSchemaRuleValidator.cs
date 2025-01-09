using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Server.SchemaValidation.Validators.Array;
using Raven.Server.SchemaValidation.Validators.Object;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

[DebuggerDisplay("'{_schemaPath}' property validator")]
public abstract class ElementSchemaRuleValidator<TParent, TAccessor>
{
    private readonly string _schemaPath;
    private ISchemaRuleValidator[] _ruleValidators;
    private BlittableJsonToken[] _typesRestriction;
    private string[] _publicTypesRestriction;

    
    // ReSharper disable once ConvertToPrimaryConstructor
    protected ElementSchemaRuleValidator(string schemaPath)
    {
        _schemaPath = schemaPath;
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
        _publicTypesRestriction = _typesRestriction.Select(SchemaValidationHelper.GetPublicType).Distinct().ToArray();
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
        //TODO Maybe to filter _ruleValidators by afgument type and avoid cast and checking inside ruleValidator.Validate
        foreach (var ruleValidator in _ruleValidators)
        {
            isValid &= ruleValidator.Validate(value, errorBuilder);
        }
        return isValid;
    }

    private bool IsOfRequiredType(BlittableJsonToken jsonToken) => _typesRestriction == null || _typesRestriction.Length == 0 || _typesRestriction.Contains(jsonToken);

    private void ReadValueSchemaRuleValidators(BlittableJsonReaderObject propertySchemaDefinition)
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
                    var objValidator = new ObjectSchemaRuleValidator(_schemaPath);
                    objValidator.Init(propertySchemaDefinition);
                    validator = objValidator;
                    hasObjectRestrictions = true;
                    break;
                }
                case SchemaValidatorConstants.prefixItems or SchemaValidatorConstants.items:
                {
                    if (hasArrayRestrictions)
                        continue;
                    var arrayValidator = new ArraySchemaRuleValidator(_schemaPath);
                    arrayValidator.Init(propertySchemaDefinition);
                    validator = arrayValidator;
                    hasArrayRestrictions = true;
                    break;
                }
                default:
                {
                    if (SchemaRuleValidatorFactoryHelper.TryCreateValidator(rule, propertySchemaDefinition, _schemaPath, out validator) == false)
                        continue;
                    break;
                }
            }

            (ruleValidators??= []).Add(validator);
        }
        _ruleValidators = ruleValidators?.ToArray();
    }

    private BlittableJsonToken[] ConvertTypeToTokens(object type)
    {
        var stringType = GetLazyString(type);
        if(SchemaValidationHelper.TryGetTokensForType(stringType, out BlittableJsonToken[] tokens) == false)
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
