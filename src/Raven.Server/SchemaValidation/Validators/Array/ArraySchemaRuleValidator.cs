using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Array;

[DebuggerDisplay("'{_schemaPath}' array validator")]

public class ArraySchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderArray>
{
    private readonly SchemaPath _schemaPath;
    private readonly ArrayItemSchemaRuleValidator[] _prefixValidators;
    private readonly (bool Allowed, ArrayItemSchemaRuleValidator validator) _itemsValidator;
    
    // ReSharper disable once ConvertToPrimaryConstructor
    public ArraySchemaRuleValidator(ArrayItemSchemaRuleValidator[] prefixValidators, (bool Allowed, ArrayItemSchemaRuleValidator validator) itemsValidator, SchemaPath schemaPath)
    {
        _prefixValidators = prefixValidators;
        _itemsValidator = itemsValidator;
        _schemaPath = schemaPath;
    }
    
    public override bool Validate(BlittableJsonReaderArray value, ErrorBuilder errorBuilder)
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
            errorBuilder?.AddError($"The array at '{errorBuilder.Path}' contains additional items, which are not allowed.");
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
    public override ArraySchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        var prefixValidators = ReadPrefixItemsSchema(schemaDefinition, schemaPath, refSchemas);
        var itemsValidators = ReadItemsSchema(schemaDefinition, schemaPath, refSchemas);
        return new ArraySchemaRuleValidator(prefixValidators, itemsValidators, schemaPath);
    }
    
    private static ArrayItemSchemaRuleValidator[] ReadPrefixItemsSchema(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        const string rule =  SchemaValidatorConstants.PrefixItems;
        if(SchemaValidationHelper.TryGetArray(schemaDefinition, rule, schemaPath.FullPath, out var prefixItemsSchema) == false)
            return null;
        schemaPath += rule;

        List<ArrayItemSchemaRuleValidator> validators = null;
        for (int i = 0; i < prefixItemsSchema.Length; i++)
        {
            var (value, token) = prefixItemsSchema.GetValueTokenTupleByIndex(i);
            
            const BlittableJsonToken expectedType = BlittableJsonToken.StartObject;
            if (token != expectedType)
                SchemaValidationHelper.TrowRuleTypeError(rule, value, expectedType, token, schemaPath.FullPath);
                    
            var validator = ElementSchemaRuleValidatorFactory.CreateArrayItemSchemaRuleValidator((BlittableJsonReaderObject)prefixItemsSchema[i], schemaPath + i, refSchemas);
            (validators ??= []).Add(validator);
        }
        return validators?.ToArray();
    }
    
    private readonly BlittableJsonToken[] _prefixItemsSchemaTypes = [BlittableJsonToken.Boolean, BlittableJsonToken.StartObject];
    private (bool Allowed, ArrayItemSchemaRuleValidator Validator) ReadItemsSchema(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        RefSchemas refSchemas)
    {
        const string rule = SchemaValidatorConstants.Items;
        if (schemaDefinition.TryGet(rule, out object prefixItemsSchema) == false)
            return (true, null);
        
        switch (prefixItemsSchema)
        {
            case bool isAdditionalPropertiesAllowed:
                return (isAdditionalPropertiesAllowed, null);
            case BlittableJsonReaderObject additionalPropertiesSchema:
                var validator = ElementSchemaRuleValidatorFactory.CreateArrayItemSchemaRuleValidator(additionalPropertiesSchema, schemaPath + rule, refSchemas);
                return (true, validator);
            default:
                SchemaValidationHelper.TrowRuleTypeError(
                    rule, prefixItemsSchema, _prefixItemsSchemaTypes, SchemaValidationHelper.GetPublicTypeOfObj(prefixItemsSchema), schemaPath.FullPath);
                return (false, null);
        }
    }
}
