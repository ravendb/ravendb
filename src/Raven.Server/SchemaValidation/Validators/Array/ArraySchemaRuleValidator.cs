using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Array;

[DebuggerDisplay("'{_schemaPath}' array validator")]

public class ArraySchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderArray>
{
    private readonly string _schemaPath;
    private ArrayItemSchemaRuleValidator[] _prefixValidators;
    private (bool Allowed, ArrayItemSchemaRuleValidator validator) _itemsValidator;
    
    // ReSharper disable once ConvertToPrimaryConstructor
    public ArraySchemaRuleValidator(string schemaPath)
    {
        _schemaPath = schemaPath;
    }
    
    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        ReadPrefixItemsSchema(schemaDefinition);
        ReadItemsSchema(schemaDefinition);
    }

    private void ReadPrefixItemsSchema(BlittableJsonReaderObject schemaDefinition)
    {
        if(SchemaValidationHelper.TryGetArray(schemaDefinition, SchemaValidatorConstants.prefixItems, _schemaPath, out var prefixItemsSchema) == false)
            return;
        
        List<ArrayItemSchemaRuleValidator> validators = null;
        for (int i = 0; i < prefixItemsSchema.Length; i++)
        {
            var (value, token) = prefixItemsSchema.GetValueTokenTupleByIndex(i);
            
            const BlittableJsonToken expectedType = BlittableJsonToken.StartObject;
            if (token != expectedType)
                SchemaValidationHelper.TrowRuleTypeError($"{SchemaValidatorConstants.prefixItems}", value, expectedType, token, _schemaPath);
                    
            var validator = new ArrayItemSchemaRuleValidator(i, _schemaPath);
            validator.Init((BlittableJsonReaderObject)prefixItemsSchema[i]);
            (validators ??= new List<ArrayItemSchemaRuleValidator>()).Add(validator);
        }
        _prefixValidators = validators?.ToArray();
    }

    private readonly BlittableJsonToken[] _prefixItemsSchemaTypes = [BlittableJsonToken.Boolean, BlittableJsonToken.StartObject];
    private void ReadItemsSchema(BlittableJsonReaderObject schemaDefinition)
    {
        const string rule = SchemaValidatorConstants.items;
        if (schemaDefinition.TryGet(rule, out object prefixItemsSchema) == false)
        {
            _itemsValidator = (true, null);
            return;
        }
        
        switch (prefixItemsSchema)
        {
            case bool isAdditionalPropertiesAllowed:
                _itemsValidator = (isAdditionalPropertiesAllowed, null);
                break;
            case BlittableJsonReaderObject additionalPropertiesSchema:
            {
                var validator = new ArrayItemSchemaRuleValidator(_schemaPath);
                validator.Init(additionalPropertiesSchema);
                _itemsValidator = (true, validator);
                break;
            }
            default:
                SchemaValidationHelper.TrowRuleTypeError(rule, prefixItemsSchema, _prefixItemsSchemaTypes, SchemaValidationHelper.GetPublicTypeOfObj(prefixItemsSchema),
                    _schemaPath);
                break;
        }
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
