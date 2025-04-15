using System;
using System.Collections.Generic;
using Raven.Server.SchemaValidation.ErrorMessage;
using Raven.Server.SchemaValidation.Validators;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public class RefSchemas
{
    private readonly Dictionary<string, RefSchema> _data = new Dictionary<string, RefSchema>();

    public bool TryGet(string refPath, out (BlittableJsonToken[] TypesRestriction, ISchemaRuleValidator[] RuleValidators) rules)
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
        
        foreach (var property in schema.GetPropertyNames())
        {
            var propertyPath =  schemaPath + property;
            if (property.Equals(SchemaValidatorConstants.Defs))
            {
                if(SchemaValidationHelper.TryGetObject(schema, property, propertyPath.FullPath, out var defSchemas) == false)
                    throw new InvalidOperationException(
                        $"Should not happen. {property} exists and wrong type should throw {nameof(InvalidSchemaValidationDefinitionException)}");

                foreach (var defSchemaName in defSchemas.GetPropertyNames())
                {
                    var defSchemaPath = propertyPath + defSchemaName;
                    if(SchemaValidationHelper.TryGetObject(defSchemas, defSchemaName, defSchemaPath.FullPath, out var defsSchema) == false)
                        throw new InvalidOperationException(
                            $"Should not happen. {property} exists and wrong type should throw {nameof(InvalidSchemaValidationDefinitionException)}");
                    _data[defSchemaPath.FullPath] = new RefSchema {Raw = defsSchema};
                }
            }
            
            if (schema.TryGetWithoutThrowingOnError(property, out BlittableJsonReaderObject propSchema))
                ReadAllDefinitions(propSchema, propertyPath);
        }
    }

    private void ValidateReferences(BlittableJsonReaderObject schema, SchemaPath path, Stack<string> refStack)
    {
        foreach (var property in schema.GetPropertyNames())
        {
            var propPath = path + property;
            if (property.Equals(SchemaValidatorConstants.Ref))
            {
                if (SchemaValidationHelper.TryGetString(schema, property, propPath.ToString(),out var @ref) == false)
                    throw new InvalidOperationException($"Should not happen. {property} exists and wrong type should throw {nameof(InvalidSchemaValidationDefinitionException)}");
                        
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
                    
            if (schema.TryGetWithoutThrowingOnError(property, out BlittableJsonReaderObject propSchema) == false || propSchema == null)
                continue;

            ValidateReferences(propSchema, propPath, refStack);
        }
    }
}
