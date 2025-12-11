using System;
using System.Collections.Generic;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators;

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
        if(SchemaValidationHelper.TryGetTypesForPublicType(stringType, out Type[] tokens) == false)
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
