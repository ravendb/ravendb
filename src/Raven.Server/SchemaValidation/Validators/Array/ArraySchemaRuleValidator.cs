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
    private ArrayItemSchemaRuleValidator _containsValidator;
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
        ReadContainsSchema(schemaDefinition);
    }

    private void ReadContainsSchema(BlittableJsonReaderObject schemaDefinition)
    {
        if(SchemaValidationHelper.TryGetObject(schemaDefinition, SchemaValidatorConstants.contains, _schemaPath, out var containsSchema) == false)
            return;

        _containsValidator = new ArrayItemSchemaRuleValidator(_schemaPath);
        _containsValidator.Init(containsSchema);
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

    private BlittableJsonToken[] prefixItemsSchemaTypes = [BlittableJsonToken.Boolean, BlittableJsonToken.StartObject];
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
                SchemaValidationHelper.TrowRuleTypeError(rule, prefixItemsSchema, prefixItemsSchemaTypes, SchemaValidationHelper.GetPublicTypeOfObj(prefixItemsSchema),
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

        if (_containsValidator != null)
        {
            var contains = false;
            for (int j = 0; j < value.Length; j++)
            {
                //TODO Maybe to make sure the validation here return immediately after the first failure.
                contains |= _containsValidator.Validate(value, j, null);
            }
            if (contains == false)
            {
                errorBuilder?.AddError($"The array at '{_schemaPath}' must contain at least one item that matches the required schema, but no such item was found. Schema : {_containsValidator.SchemaDefinition}");
                isValid = false;
            }
        }
        
        return isValid;
    }
}
