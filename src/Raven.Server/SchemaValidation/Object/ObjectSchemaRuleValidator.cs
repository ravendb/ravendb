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
    protected ObjectSchemaRuleValidator(string path, string property, bool isRequired) : base(path)
    {
    }

    protected bool IsOfRequiredType(BlittableJsonToken token) => (token & BlittableJsonReaderBase.TypesMask) == BlittableJsonToken.StartObject;

    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        //TODO To create an informative error when fails to read
        var rulesValidators = new List<PropertySchemaRuleValidator>();

        if (schemaDefinition.TryGet(SchemaValidatorConstants.properties, out BlittableJsonReaderObject properties))
        {
            //TODO To throw a good error if the required array contains not string valueds
            var requiredHashSet = schemaDefinition.TryGet(SchemaValidatorConstants.required, out BlittableJsonReaderArray required)
                ? required.Select(x => x.ToString()).ToHashSet()
                : null;
            
            foreach (var propertyName in properties.GetPropertyNames())
            {
                if (properties.TryGet(propertyName, out BlittableJsonReaderObject propertySchemaDefinition) == false)
                {
                    //TODO To put better message
                    Debug.Assert(false, "Should not happen");
                    continue;
                }

                var isRequired = requiredHashSet?.Contains(propertyName) ?? false;
                var path = $"{Path}.{propertyName}";

                var validator = new PropertySchemaRuleValidator(path, propertyName, isRequired);
                //TODO Maybe to init with parent and check isRequired there
                validator.Init(propertySchemaDefinition);
                rulesValidators.Add(validator);
            }
        }
        
        _propertyValidators = rulesValidators.ToArray();
    }

    protected override void ValidateInternal(BlittableJsonReaderObject value, IErrorBuilder errorBuilder)
    {
        foreach (var validator in _propertyValidators)
        {
            validator.Validate(value, errorBuilder);
        }
    }

    protected bool TryGetValueSchemaRuleValidator(string rule, out (Type Type, string[] AdditionalInfoProps) schemaRuleValidator)
    {
        //TODO To refactor and remove it if not needed
        throw new NotImplementedException();
    }
}
