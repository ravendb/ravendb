using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

public class ObjectSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private PropertySchemaRuleValidator[] _propertyValidators;

    // ReSharper disable once ConvertToPrimaryConstructor
    protected ObjectSchemaRuleValidator(string path, string property, bool isRequired)
    {
    }

    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        var rulesValidators = new List<PropertySchemaRuleValidator>();

        //TODO To create an informative error when fails to read
        ReadPropertyValidators(schemaDefinition, rulesValidators);
        ReadPatternPropertyValidators(schemaDefinition, rulesValidators);
        
        _propertyValidators = rulesValidators.ToArray();
    }

    private void ReadPropertyValidators(BlittableJsonReaderObject schemaDefinition, List<PropertySchemaRuleValidator> validators)
    {
        //TODO To throw a good error if the required array contains not string valueds
        var requiredHashSet = schemaDefinition.TryGet(SchemaValidatorConstants.required, out BlittableJsonReaderArray required)
            ? required.Select(x => x.ToString()).ToHashSet()
            : null;

        if (schemaDefinition.TryGet(SchemaValidatorConstants.properties, out BlittableJsonReaderObject properties))
        {
            foreach (var propertyName in properties.GetPropertyNames())
            {
                if (properties.TryGet(propertyName, out BlittableJsonReaderObject propertySchemaDefinition) == false)
                {
                    //TODO To put better message
                    Debug.Assert(false, "Should not happen");
                    continue;
                }

                var isRequired = requiredHashSet?.Remove(propertyName) ?? false;
                // var path = $"{Path}.{propertyName}"; TODO To add path for errors

                var validator = new NamedPropertySchemaRuleValidator( propertyName, isRequired);
                //TODO Maybe to init with parent and check isRequired there
                validator.Init(propertySchemaDefinition);
                validators.Add(validator);
            }
        }

        if (requiredHashSet == null)
            return;
        
        foreach (var propertyName in requiredHashSet)
        {
            //TODO Maybe to implement RequiredPropertySchemaRuleValidator
            var validator = new NamedPropertySchemaRuleValidator( propertyName, true);
            //TODO Maybe to init with parent and check isRequired there
            validator.Init(null);
            validators.Add(validator);
        }
    }

    private void ReadPatternPropertyValidators(BlittableJsonReaderObject schemaDefinition, List<PropertySchemaRuleValidator> validators)
    {
        if (schemaDefinition.TryGet(SchemaValidatorConstants.patternProperties, out BlittableJsonReaderObject patternProperties) == false) 
            return;
        
        foreach (var propertyPattern in patternProperties.GetPropertyNames())
        {
            if (patternProperties.TryGet(propertyPattern, out BlittableJsonReaderObject propertySchemaDefinition) == false)
            {
                //TODO To throw error when it is not an object
                Debug.Assert(false, "Should not happen");
                continue;
            }

            // var path = $"{Path}.{propertyPattern}"; //TODO To add path for errors on init

            var validator = new PatternPropertiesSchemaRuleValidator(propertyPattern);
            validator.Init(propertySchemaDefinition);
            validators.Add(validator);
        }
    }

    protected override void ValidateInternal(BlittableJsonReaderObject value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        foreach (var validator in _propertyValidators)
        {
            validator.Validate(value, path, errorBuilder);
        }
    }

    protected bool TryGetValueSchemaRuleValidator(string rule, out (Type Type, string[] AdditionalInfoProps) schemaRuleValidator)
    {
        //TODO To refactor and remove it if not needed
        throw new NotImplementedException();
    }
}
