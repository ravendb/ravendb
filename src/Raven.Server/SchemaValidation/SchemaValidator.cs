using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

internal class SchemaValidator
{
    private ObjectSchemaRuleValidator _root;

    public SchemaValidator(BlittableJsonReaderObject schemaDefinition)
    {
        ReadSchemaDefinition(schemaDefinition);
    }

    private void ReadSchemaDefinition(BlittableJsonReaderObject schemaDefinition)
    {
        var root = new RootObjectSchemaRuleValidator();
        root.ReadSchemaDefinition(schemaDefinition);
        // RecursiveReadSchemaDefinition(root, schemaDefinition);
        _root = root;
    }

    // private void RecursiveReadSchemaDefinition(ObjectSchemaRuleValidator current, BlittableJsonReaderObject schemaDefinition)
    // {
    //     //TODO To create an informative error when fails to read
    //     
    //     var rulesValidators = new List<ISchemaRuleValidator<BlittableJsonReaderObject>>();
    //
    //     if (schemaDefinition.TryGet(SchemaValidatorConstants.properties, out BlittableJsonReaderObject properties))
    //     {
    //         var requiredHashSet = schemaDefinition.TryGet(SchemaValidatorConstants.required, out BlittableJsonReaderArray required)
    //             ? required.Select(x => x.ToString()).ToHashSet()
    //             : null;
    //         
    //         foreach (var propertyName in properties.GetPropertyNames())
    //         {
    //             if (properties.TryGet(propertyName, out BlittableJsonReaderObject propertySchemaDefinition) == false)
    //             {
    //                 Debug.Assert(false, "Should not happen");
    //                 continue;
    //             }
    //             
    //             if (propertySchemaDefinition.TryGet("type", out TypeRestriction type))
    //             {
    //                 //TODO check what should be done if there is no type
    //                 var isRequired = requiredHashSet?.Remove(propertyName) ?? false;
    //                 var path = $"{current.Property}.{propertyName}";
    //                 PropertySchemaRuleValidator validator = type switch
    //                 {
    //                     TypeRestriction.@object => new ObjectSchemaRuleValidator(path, propertyName, isRequired),
    //                     TypeRestriction.@string => new StringSchemaRuleValidator(path, propertyName, isRequired),
    //                     TypeRestriction.integer => new IntegerSchemaRuleValidator(path, propertyName, isRequired),
    //                     _ => throw new ArgumentOutOfRangeException()
    //                 };
    //                 validator.ReadSchemaDefinition(propertySchemaDefinition);
    //                 rulesValidators.Add(validator);
    //             }
    //         }
    //     }
    //     
    //     current.RuleValidators = rulesValidators.ToArray();
    // }

    public bool Validate(BlittableJsonReaderObject obj, out string errors)
    {
        var errorBuilder = new StringBuilder();
        _root.Validate(obj, errorBuilder);
        errors = errorBuilder.ToString();
        return string.IsNullOrEmpty(errors);
    }
}
