using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Array;

[DebuggerDisplay("'{_schemaPath}' array validator")]

public class ArraySchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderArray>
{
    private readonly string _schemaPath;
    private readonly ArrayItemSchemaRuleValidator[] _prefixValidators;
    private readonly (bool Allowed, ArrayItemSchemaRuleValidator validator) _itemsValidator;
    
    // ReSharper disable once ConvertToPrimaryConstructor
    public ArraySchemaRuleValidator(ArrayItemSchemaRuleValidator[] prefixValidators, (bool Allowed, ArrayItemSchemaRuleValidator validator) itemsValidator, string schemaPath)
    {
        _prefixValidators = prefixValidators;
        _itemsValidator = itemsValidator;
        _schemaPath = schemaPath;
    }
    
    protected override bool ValidateInternal(BlittableJsonReaderArray value, IErrorBuilder errorBuilder)
    {
        var isValid = true;
        int i = 0;

        if (_prefixValidators != null)
        {
            var length = Math.Min(value.Length, _prefixValidators.Length);
            if (_prefixValidators != null)
            {
                for (; i < length; i++)
                {
                    errorBuilder?.Path.StepIn(i);
                    isValid &= _prefixValidators[i].Validate(value, i, errorBuilder);
                    errorBuilder?.Path.StepOut();
                }
            }
        }
        
        if (value.Length > i && _itemsValidator.Allowed == false)
        {
            errorBuilder?.AddError($"The array at '{_schemaPath}' contains additional items, which are not allowed.");
            isValid = false;
        }
        
        if (_itemsValidator.validator != null)
        {
            for (; i < value.Length; i++)
            {
                errorBuilder?.Path.StepIn(i);
                isValid &=_itemsValidator.validator.Validate(value, i, errorBuilder);
                errorBuilder?.Path.StepOut();
            }
        }
        
        return isValid;
    }
}

public class ArraySchemaRuleValidatorFactory : SchemaRuleValidatorFactory<ArraySchemaRuleValidator>
{
    public override ArraySchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        var prefixValidators = ReadPrefixItemsSchema(schemaDefinition, schemaPath);
        var itemsValidators = ReadItemsSchema(schemaDefinition, schemaPath);
        return new ArraySchemaRuleValidator(prefixValidators, itemsValidators, schemaPath);
    }
    
    private static ArrayItemSchemaRuleValidator[] ReadPrefixItemsSchema(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(SchemaValidationHelper.TryGetArray(schemaDefinition, SchemaValidatorConstants.prefixItems, schemaPath, out var prefixItemsSchema) == false)
            return null;
        
        List<ArrayItemSchemaRuleValidator> validators = null;
        for (int i = 0; i < prefixItemsSchema.Length; i++)
        {
            var (value, token) = prefixItemsSchema.GetValueTokenTupleByIndex(i);
            
            const BlittableJsonToken expectedType = BlittableJsonToken.StartObject;
            if (token != expectedType)
                SchemaValidationHelper.TrowRuleTypeError($"{SchemaValidatorConstants.prefixItems}", value, expectedType, token, schemaPath);
                    
            var validator = ElementSchemaRuleValidatorFactory.CreateArrayItemSchemaRuleValidator((BlittableJsonReaderObject)prefixItemsSchema[i], schemaPath, i);
            (validators ??= []).Add(validator);
        }
        return validators?.ToArray();
    }
    
    private readonly BlittableJsonToken[] _prefixItemsSchemaTypes = [BlittableJsonToken.Boolean, BlittableJsonToken.StartObject];
    private (bool Allowed, ArrayItemSchemaRuleValidator Validator) ReadItemsSchema(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        const string rule = SchemaValidatorConstants.items;
        if (schemaDefinition.TryGet(rule, out object prefixItemsSchema) == false)
            return (true, null);
        
        switch (prefixItemsSchema)
        {
            case bool isAdditionalPropertiesAllowed:
                return (isAdditionalPropertiesAllowed, null);
            case BlittableJsonReaderObject additionalPropertiesSchema:
                var validator = ElementSchemaRuleValidatorFactory.CreateArrayItemSchemaRuleValidator(additionalPropertiesSchema, schemaPath);
                return (true, validator);
            default:
                SchemaValidationHelper.TrowRuleTypeError(
                    rule, prefixItemsSchema, _prefixItemsSchemaTypes, SchemaValidationHelper.GetPublicTypeOfObj(prefixItemsSchema), schemaPath);
                return (false, null);
        }
    }
}
