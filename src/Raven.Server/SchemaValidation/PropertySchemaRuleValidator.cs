using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

//TODO To find a better name
internal abstract class PropertySchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    protected readonly bool IsRequired;

    public string Property { get; }
    
    // ReSharper disable once ConvertToPrimaryConstructor
    protected PropertySchemaRuleValidator(string path, string property, bool isRequired) : base(path)
    {
        IsRequired = isRequired;
        Property = property;
    }
    
    public abstract void ReadSchemaDefinition(BlittableJsonReaderObject propertySchemaDefinition);
    
    protected abstract bool IsOfRequiredType(BlittableJsonToken token);


    protected string GetActualPublicTypeName(BlittableJsonToken token)
    {
        if ((token & BlittableJsonToken.Integer) == BlittableJsonToken.Integer)
        {
            return "Integer";
        }
        
        if((token & BlittableJsonToken.String) == BlittableJsonToken.String || (token & BlittableJsonToken.CompressedString) == BlittableJsonToken.CompressedString)
        {
            return "String";
        }

        if((token & BlittableJsonToken.LazyNumber) != 0)
        {
            return "Number";
        }

        if ((token & BlittableJsonToken.Boolean) == BlittableJsonToken.Boolean)
        {
            return "Boolean";
        }

        if ((token & BlittableJsonToken.StartObject) != 0)
        {
            return "Object";
        }

        if((token & BlittableJsonToken.StartArray) != 0)
        {
            return "Array";
        }

        if((token & BlittableJsonToken.Null) != 0)
        {
            return "Null";
        }
        
        //TODO To think about the error message
        throw new InvalidOperationException("some error");
    }
    

}
internal abstract class PropertySchemaRuleValidator<T> : PropertySchemaRuleValidator
{
    
    public ISchemaRuleValidator<T>[] RuleValidators;
    
    // ReSharper disable once ConvertToPrimaryConstructor
    protected PropertySchemaRuleValidator(string path, string property, bool isRequired) : base(path, property, isRequired)
    {
    }

    public override void Validate(BlittableJsonReaderObject parent, StringBuilder errorBuilder)
    {
        if (TryGetValue(parent, errorBuilder, out var value) == false)
            return;
        
        if (RuleValidators != null)
        {
            foreach (var validator in RuleValidators)
            {
                validator.Validate(value, errorBuilder);
            }    
        }
    }
    
    private bool TryGetValue(BlittableJsonReaderObject parent, StringBuilder errorBuilder, out T value)
    {
        value = default;
        if (parent.TryGetPropertyType(new StringSegment(Property), out var token) == false)
        {
            if (IsRequired)
            {
                errorBuilder.AppendLine($"{Path} is required");
            }
            return false;
        }
        
        if (IsOfRequiredType(token) == false)
        {
            if((token & BlittableJsonToken.Null) != BlittableJsonToken.Null)
            {
                errorBuilder.AppendLine($"{Path} should be of type object but actual type is {GetActualPublicTypeName(token)}");
            }
            else
            {
                if (IsRequired)
                    //TODO To check if not required integer can be null
                    errorBuilder.AppendLine($"{Path} is required but it is null");
            }
            return false;
        }

        if(TryGetValue(parent, out value) == false)
        {
            Debug.Assert(false, "Should not happen - we already confirmed the property exists and has the right type");
            return false;
        }
        
        return true;
    }

    protected virtual bool TryGetValue(BlittableJsonReaderObject parent, out T value) => parent.TryGetWithoutThrowingOnError(Property, out value);

    protected static void ReadValueSchemaRuleValidators(BlittableJsonReaderObject propertySchemaDefinition, string path, List<ISchemaRuleValidator<T>> intRulesValidators)
    {
        foreach (var p in propertySchemaDefinition.GetPropertyNames())
        {
            if(p == "type" || p == "description")
                //TODO Check if there more
                continue;
                                
            if(propertySchemaDefinition.TryGet(p, out object v) == false)
                //TODO Should not happen
                continue;
                                
            var foundAdditionalInfoProps = new List<object> { v };
                                
            //TODO Should handle other type as well and not just integer
            if(IntegerSchemaRuleValidator.LongValuesSchemaRuleValidator.TryGetValue(p, out var validatorInfo) == false)
                //Can happen if the restriction rule is an additional info for another rule
                continue;

            foreach (string additionalInfoProp in validatorInfo.AdditionalInfoProps)
            {
                if(propertySchemaDefinition.TryGet(additionalInfoProp, out object additionalInfoPropValue) == false)
                    continue;
                                    
                foundAdditionalInfoProps.Add(additionalInfoPropValue);
            }

            var factory = ValueSchemaRuleValidatorFactory.Create<T>(p, path, foundAdditionalInfoProps.ToArray());
            intRulesValidators.Add(factory);
        }
    }

}
