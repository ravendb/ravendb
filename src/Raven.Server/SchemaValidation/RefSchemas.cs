using System;
using System.Collections.Generic;
using Raven.Server.SchemaValidation.ErrorMessage;
using Raven.Server.SchemaValidation.Validators;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public class RefSchemas
{
    private readonly Dictionary<string, RefSchema> _data = new Dictionary<string, RefSchema>();

    public bool TryGet(string refPath, out (Type[] TypesRestriction, ISchemaRuleValidator[] RuleValidators) rules)
    {
        if (_data.TryGetValue(refPath, out var refSchema))
        {
            rules = (refSchema?.Rules.typesRestriction, refSchema?.Rules.ruleValidators);
            return true;
        }

        rules = (null, null);
        return false;
    }
    
    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        var root = new SchemaPath();
        ReadAllDefinitions(schemaDefinition, root);
        ValidateReferences(schemaDefinition, root, new Stack<string>());

        foreach (var (fullPath, refSchema) in _data)
        {
            if (ElementSchemaRuleValidatorFactory.TryReadSchema(refSchema.Raw, root + fullPath, this, out var typesRestriction, out var ruleValidators) == false)
                continue;
            
            refSchema.Rules = (typesRestriction, ruleValidators);
        }
    }

    private void ReadAllDefinitions(BlittableJsonReaderObject schema, SchemaPath schemaPath)
    {
        if(schema == null)
            return;
        
        var property = default(BlittableJsonReaderObject.PropertyDetails);
        for (int i = 0; i < schema.Count; i++)
        {
            schema.GetPropertyByIndex(i, ref property);
            var propertyPath =  schemaPath + property.Name;
            if (property.Name.Equals(SchemaValidatorConstants.Defs))
            {
                var defSchemas = SchemaValidationHelper.CheckTypeAndThrow<BlittableJsonReaderObject>(property.Name, property.Value, propertyPath.FullPath);
                var defSchemaProp = default(BlittableJsonReaderObject.PropertyDetails);
                for (int j = 0; j < defSchemas.Count; j++)
                {
                    schema.GetPropertyByIndex(j, ref defSchemaProp);
                    var defSchemaPath = propertyPath + defSchemaProp.Name;
                    var defsSchema = SchemaValidationHelper.CheckTypeAndThrow<BlittableJsonReaderObject>(defSchemaProp.Name, defSchemaProp.Value, defSchemaPath.FullPath);
                    _data[defSchemaPath.FullPath] = new RefSchema {Raw = defsSchema};
                }
            }
            
            if (schema.TryGetWithoutThrowingOnError(property.Name, out BlittableJsonReaderObject propSchema))
                ReadAllDefinitions(propSchema, propertyPath);
        }
    }

    private void ValidateReferences(BlittableJsonReaderObject schema, SchemaPath path, Stack<string> refStack)
    {
        var property = default(BlittableJsonReaderObject.PropertyDetails);
        for (int i = 0; i < schema.Count; i++)
        {
            schema.GetPropertyByIndex(i, ref property);
            var propPath = path + property.Name;
            if (property.Name.Equals(SchemaValidatorConstants.Ref))
            {
                var @ref = SchemaValidationHelper.CheckTypeAndThrow<LazyStringValue>(property.Name, property.Value, propPath.ToString());
                if (refStack.Contains(@ref))
                    throw new InvalidSchemaValidationDefinitionException(
                        $"A circular reference was detected at '{propPath.FullPath}'.");
                        
                if(_data.TryGetValue(@ref, out var refSchema) == false)
                    throw new InvalidSchemaValidationDefinitionException(
                        $"The reference '{@ref}' does not match any defined subschema.");

                refStack.Push(@ref);
                ValidateReferences(refSchema.Raw, propPath, refStack);
                refStack.Pop();
                continue;
            }
                    
            if (schema.TryGetWithoutThrowingOnError(property.Name, out BlittableJsonReaderObject propSchema) == false || propSchema == null)
                continue;

            ValidateReferences(propSchema, propPath, refStack);
        }
    }
}
