using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Array;

[DebuggerDisplay("'{_schemaPath}' array validator")]

public class ArraySchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderArray>
{
    private readonly SchemaPath _schemaPath;
    private readonly ElementSchemaRuleValidator[] _prefixValidators;
    private readonly (bool Allowed, ElementSchemaRuleValidator validator) _itemsValidator;
    
    // ReSharper disable once ConvertToPrimaryConstructor
    public ArraySchemaRuleValidator(ElementSchemaRuleValidator[] prefixValidators, (bool Allowed, ElementSchemaRuleValidator validator) itemsValidator, SchemaPath schemaPath)
    {
        _prefixValidators = prefixValidators;
        _itemsValidator = itemsValidator;
        _schemaPath = schemaPath;
    }
    
    public override bool Validate(SchemaValidationContext context, BlittableJsonReaderArray value)
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
                    context.StepIn(i);
                    isValid &= _prefixValidators[i].Validate(context, value[i]);
                    context.StepOut();

                    if (context.ErrorBuilder == null && isValid == false)
                        return false;
                }
            }
        }
        
        if (value.Length > i && _itemsValidator.Allowed == false)
        {
            context.ErrorBuilder?.AddError($"The array at '{context.ErrorBuilder.Path}' contains additional items, which are not allowed.");
            isValid = false;
            if (context.ErrorBuilder == null)
                return false;
        }
        
        if (_itemsValidator.validator != null)
        {
            for (; i < value.Length; i++)
            {
                context.StepIn(i);
                isValid &=_itemsValidator.validator.Validate(context, value[i]);
                context.StepOut();
                
                if (context.ErrorBuilder == null && isValid == false)
                    return false;
            }
        }
        
        return isValid;
    }
}

public class ArraySchemaRuleValidatorFactory : SchemaRuleValidatorFactory<ArraySchemaRuleValidator>
{
    public override ArraySchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        var prefixValidators = ReadPrefixItemsSchema(context, schemaDefinition, schemaPath);
        var itemsValidators = ReadItemsSchema(context, schemaDefinition, schemaPath);
        return new ArraySchemaRuleValidator(prefixValidators, itemsValidators, schemaPath);
    }
    
    private static ElementSchemaRuleValidator[] ReadPrefixItemsSchema(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        const string rule =  SchemaValidatorConstants.PrefixItems;
        schemaPath += rule;
        if(SchemaValidationHelper.TryGetArray(schemaDefinition, rule, schemaPath, out var prefixItemsSchema) == false)
            return null;

        List<ElementSchemaRuleValidator> validators = null;
        for (var i = 0; i < prefixItemsSchema.Length; i++)
        {
            var item = prefixItemsSchema[i];
            if (item is not BlittableJsonReaderObject blittableItem)
            {
                SchemaValidationHelper.ThrowRuleTypeError(item, typeof(BlittableJsonReaderObject), schemaPath);
                return null;// Required to satisfy compiler flow analysis; method above always throws
            }
                    
            var validator = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(context, blittableItem, schemaPath + i);
            (validators ??= []).Add(validator);
        }
        return validators?.ToArray();
    }
    
    
    private static (bool Allowed, ElementSchemaRuleValidator Validator) ReadItemsSchema(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        const string rule = SchemaValidatorConstants.Items;
        schemaPath += rule;
        if (schemaDefinition.TryGet(rule, out object prefixItemsSchema) == false)
            return (true, null);

        switch (prefixItemsSchema)
        {
            case bool isAdditionalPropertiesAllowed:
                return (isAdditionalPropertiesAllowed, null);
            case BlittableJsonReaderObject additionalPropertiesSchema:
                var validator = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(context, additionalPropertiesSchema, schemaPath + rule);
                return (true, validator);
            default:
                var expectedTypes = new HashSet<Type> { typeof(bool), typeof(BlittableJsonReaderObject) };
                SchemaValidationHelper.ThrowRuleTypeError(prefixItemsSchema, expectedTypes, schemaPath);
                return (false, null);
        }
    }
}
