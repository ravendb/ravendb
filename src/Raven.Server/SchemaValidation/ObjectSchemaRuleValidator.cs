using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

internal class ObjectSchemaRuleValidator : PropertySchemaRuleValidator<BlittableJsonReaderObject>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public ObjectSchemaRuleValidator(string path, string property, bool isRequired) : base(path, property, isRequired)
    {
    }

    protected override bool IsOfRequiredType(BlittableJsonToken token) => (token & BlittableJsonReaderBase.TypesMask) == BlittableJsonToken.StartObject;

    public override void ReadSchemaDefinition(BlittableJsonReaderObject schemaDefinition)
    {
        //TODO To create an informative error when fails to read
        var rulesValidators = new List<ISchemaRuleValidator<BlittableJsonReaderObject>>();

        if (schemaDefinition.TryGet(SchemaValidatorConstants.properties, out BlittableJsonReaderObject properties))
        {
            var requiredHashSet = schemaDefinition.TryGet(SchemaValidatorConstants.required, out BlittableJsonReaderArray required)
                ? required.Select(x => x.ToString()).ToHashSet()
                : null;
            
            foreach (var propertyName in properties.GetPropertyNames())
            {
                if (properties.TryGet(propertyName, out BlittableJsonReaderObject propertySchemaDefinition) == false)
                {
                    Debug.Assert(false, "Should not happen");
                    continue;
                }
                
                if (propertySchemaDefinition.TryGet("type", out TypeRestriction type))
                {
                    //TODO check what should be done if there is no type
                    var isRequired = requiredHashSet?.Remove(propertyName) ?? false;
                    var path = $"{Property}.{propertyName}";
                    PropertySchemaRuleValidator validator = type switch
                    {
                        TypeRestriction.@object => new ObjectSchemaRuleValidator(path, propertyName, isRequired),
                        TypeRestriction.@string => new StringSchemaRuleValidator(path, propertyName, isRequired),
                        TypeRestriction.integer => new IntegerSchemaRuleValidator(path, propertyName, isRequired),
                        //TODO To handle the exception
                        _ => throw new ArgumentOutOfRangeException()
                    };
                    validator.ReadSchemaDefinition(propertySchemaDefinition);
                    rulesValidators.Add(validator);
                }
            }
        }
        
        RuleValidators = rulesValidators.ToArray();
    }
}
