using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

internal class PropertySchemaRuleValidator
{
    private readonly string _path;
    private readonly string _property;
    private readonly bool _isRequired;
    private BlittableJsonToken[] _typesRestriction;
    private SchemaRuleValidator[] _ruleValidators;

    // ReSharper disable once ConvertToPrimaryConstructor
    public PropertySchemaRuleValidator(string path, string property, bool isRequired) 
    {
        _path = path;
        _property = property;
        _isRequired = isRequired;
    }

    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        ReadTypeRestrictionsRule(schemaDefinition);
        ReadValueSchemaRuleValidators(schemaDefinition);
    }

    private void ReadTypeRestrictionsRule(BlittableJsonReaderObject schemaDefinition)
    {
        if (schemaDefinition.TryGet(SchemaValidatorConstants.type, out object typesRestriction) == false) 
            return;
        
        var allowedTypes = new List<BlittableJsonToken>();
        if (typesRestriction is BlittableJsonReaderArray types)
        {
            foreach (var type in types)
            {
                allowedTypes.AddRange(ConvertTypeToToken(type));
            }
        }
        else 
        {
            allowedTypes.AddRange(ConvertTypeToToken(typesRestriction));
        }

        _typesRestriction = allowedTypes.ToArray();
    }

    private IEquatable<string> GetEquatable(object type)
    {
        return type switch
        {
            LazyStringValue lazyStringValue => lazyStringValue,
            LazyCompressedStringValue lazyCompressedStringValue => lazyCompressedStringValue.ToString(),
            _ => throw new InvalidSchemaValidationDefinitionException($"Expected array or string for 'type', got {GetPublicType(type)}. Path '{_path}'.")
        };
    }

    private IEnumerable<BlittableJsonToken> ConvertTypeToToken(object type)
    {
        var equatable = GetEquatable(type);
        //TODO To replace with constant
        if (equatable.Equals("string"))
        {
            yield return BlittableJsonToken.String;
            yield return BlittableJsonToken.CompressedString;
        }
        //TODO To replace with constant
        else if (equatable.Equals("integer"))
        {
            yield return BlittableJsonToken.Integer;
        }
        else if (equatable.Equals("object"))
        {
            yield return BlittableJsonToken.StartObject;
        }
    }

    private void ReadValueSchemaRuleValidators(BlittableJsonReaderObject propertySchemaDefinition)
    {
        var ruleValidators = new List<SchemaRuleValidator>();
        foreach (var rule in propertySchemaDefinition.GetPropertyNames())
        {
            if (rule is SchemaValidatorConstants.type or SchemaValidatorConstants.description)
                //TODO Check if there more
                continue;

            if (propertySchemaDefinition.TryGet(rule, out object v) == false)
                //TODO Should not happen
                continue;

            var foundAdditionalInfoProps = new List<object> { v };

            if (ValueSchemaRuleValidatorFactory.TryGetValueSchemaRuleValidatorFactory(rule, out var validatorsInfo) == false)
                //Can happen if the restriction rule is an additional info for another rule
                continue;

            foreach (var validatorInfo in validatorsInfo)
            {
                foreach (string additionalInfoProp in validatorInfo.AdditionalInfoProps)
                {
                    if (propertySchemaDefinition.TryGet(additionalInfoProp, out object additionalInfoPropValue) == false)
                        continue;

                    foundAdditionalInfoProps.Add(additionalInfoPropValue);
                }

                if(validatorInfo.TryCreate(_path, foundAdditionalInfoProps.ToArray(), out var validator) == false)
                    continue;
                ruleValidators.Add(validator);
            }
        }

        _ruleValidators = ruleValidators.ToArray();
    }

    //TODO Maybe to move those method to another class
    // #region GetValueSchemaRuleValidatorFactory
    // private static bool TryGetValueSchemaRuleValidatorFactory(string rule, Type argType, out ValueSchemaRuleValidatorFactory schemaRuleValidator)
    // {
    //     if(CashedValuesSchemaRuleValidator.TryGetValue(rule, out var list))
    //     {
    //         schemaRuleValidator = list.First(x => x.ArgType == argType);
    //         return true;
    //     }
    //
    //     schemaRuleValidator = null;
    //     return false;
    // }
    //
    // private static readonly Dictionary<string, List<ValueSchemaRuleValidatorFactory>> CashedValuesSchemaRuleValidator = GetValuesSchemaRuleValidator();
    //
    // private static Dictionary<string, List<ValueSchemaRuleValidatorFactory>> GetValuesSchemaRuleValidator()
    // {
    //     var ret = new Dictionary<string, List<ValueSchemaRuleValidatorFactory>>();
    //     
    //     var schemaRuleValidators = Assembly.GetExecutingAssembly().GetTypes()
    //         .Select(x => (Type: x, RuleInfo: x.GetCustomAttribute<SchemaRuleAttribute>()))
    //         .Where(x => typeof(ISchemaRuleValidator).IsAssignableFrom(x.Type) && !x.Type.IsAbstract && x.RuleInfo != null);
    //     
    //     foreach (var validator in schemaRuleValidators)
    //     {
    //         var rule = validator.RuleInfo.Rule;
    //         if (ret.TryGetValue(rule, out var list) == false)
    //             ret[rule] = list = new List<ValueSchemaRuleValidatorFactory>();
    //
    //         var argType = GetGenericSchemaRuleValidatorType(validator.Type);
    //         list.Add(new ValueSchemaRuleValidatorFactory(validator.Type, argType, validator.RuleInfo.AdditionalInfoProps));
    //     }
    //
    //     return ret;
    // } 
    //
    // static Type GetGenericSchemaRuleValidatorType(Type type)
    // {
    //     if (type == null || type == typeof(object))
    //         return null;
    //     
    //     if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(SchemaRuleValidator<>))
    //         return type.GetGenericArguments()[0];
    //
    //     return GetGenericSchemaRuleValidatorType(type.BaseType);
    // }
    // #endregion
    
    private string GetPublicType(object type)
    {
        throw new NotImplementedException();
    }

    public void Validate(BlittableJsonReaderObject parent, IErrorBuilder errorBuilder)
    {
        if (TryGetPropertyType(parent, out BlittableJsonToken token) == false 
            || (token & BlittableJsonReaderBase.TypesMask) == BlittableJsonToken.Null)
        {
            if (_isRequired)
                //TODO To improve the error message
                errorBuilder.AddError($"{_path} is required");

            return;
        }

        if (IsOfRequiredType(token) == false)
        {
            errorBuilder.AddError($"{_path} should be of type object but actual type is {GetActualPublicTypeName(token)}");
        }

        var value = parent[_property];
        //TODO Maybe to filter _ruleValidators by afgument type and avoid cast and checking inside ruleValidator.Validate
        foreach (var ruleValidator in _ruleValidators)
        {
            ruleValidator.Validate(value, errorBuilder);
        }
    }

    private bool TryGetPropertyType(BlittableJsonReaderObject parent, out BlittableJsonToken token)
    {
        var ret = parent.TryGetPropertyType(new StringSegment(_property), out var intToken);
        token = intToken & BlittableJsonReaderBase.TypesMask;
        return ret;
    }

    private string GetActualPublicTypeName(BlittableJsonToken token)
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
    
    private bool IsOfRequiredType(BlittableJsonToken jsonToken) => _typesRestriction == null || _typesRestriction.Contains(jsonToken);
}


//TODO To find a better name
// internal abstract class PropertySchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
// {
//     protected readonly bool IsRequired;
//
//     public string Property { get; }
//     
//     // ReSharper disable once ConvertToPrimaryConstructor
//     protected PropertySchemaRuleValidator(string path, string property, bool isRequired) : base(path)
//     {
//         IsRequired = isRequired;
//         Property = property;
//     }
//     
//     public abstract void ReadSchemaDefinition(BlittableJsonReaderObject propertySchemaDefinition);
//     
//     protected abstract bool IsOfRequiredType(BlittableJsonToken token);
//
//     protected string GetActualPublicTypeName(BlittableJsonToken token)
//     {
//         if ((token & BlittableJsonToken.Integer) == BlittableJsonToken.Integer)
//         {
//             return "Integer";
//         }
//         
//         if((token & BlittableJsonToken.String) == BlittableJsonToken.String || (token & BlittableJsonToken.CompressedString) == BlittableJsonToken.CompressedString)
//         {
//             return "String";
//         }
//
//         if((token & BlittableJsonToken.LazyNumber) != 0)
//         {
//             return "Number";
//         }
//
//         if ((token & BlittableJsonToken.Boolean) == BlittableJsonToken.Boolean)
//         {
//             return "Boolean";
//         }
//
//         if ((token & BlittableJsonToken.StartObject) != 0)
//         {
//             return "Object";
//         }
//
//         if((token & BlittableJsonToken.StartArray) != 0)
//         {
//             return "Array";
//         }
//
//         if((token & BlittableJsonToken.Null) != 0)
//         {
//             return "Null";
//         }
//         
//         //TODO To think about the error message
//         throw new InvalidOperationException("some error");
//     }
//     
//
// }
// internal abstract class PropertySchemaRuleValidator<T> : PropertySchemaRuleValidator
// {
//     protected ISchemaRuleValidator<T>[] RuleValidators;
//     
//     // ReSharper disable once ConvertToPrimaryConstructor
//     protected PropertySchemaRuleValidator(string path, string property, bool isRequired) : base(path, property, isRequired)
//     {
//     }
//
//     public override void Validate(object value, StringBuilder errorBuilder)
//     {
//         if (TryGetValue(parent, errorBuilder, out var value) == false)
//             return;
//         
//         if (RuleValidators != null)
//         {
//             foreach (var validator in RuleValidators)
//             {
//                 validator.Validate(value, errorBuilder);
//             }    
//         }
//     }
//     
//     private bool TryGetValue(BlittableJsonReaderObject parent, StringBuilder errorBuilder, out T value)
//     {
//         value = default;
//         if (parent.TryGetPropertyType(new StringSegment(Property), out var token) == false)
//         {
//             if (IsRequired)
//             {
//                 errorBuilder.AppendLine($"{Path} is required");
//             }
//             return false;
//         }
//         
//         if (IsOfRequiredType(token) == false)
//         {
//             if((token & BlittableJsonToken.Null) != BlittableJsonToken.Null)
//             {
//                 errorBuilder.AppendLine($"{Path} should be of type object but actual type is {GetActualPublicTypeName(token)}");
//             }
//             else
//             {
//                 if (IsRequired)
//                     //TODO To check if not required integer can be null
//                     errorBuilder.AppendLine($"{Path} is required but it is null");
//             }
//             return false;
//         }
//
//         if(TryGetValue(parent, out value) == false)
//         {
//             Debug.Assert(false, "Should not happen - we already confirmed the property exists and has the right type");
//             return false;
//         }
//         
//         return true;
//     }
//
//     protected virtual bool TryGetValue(BlittableJsonReaderObject parent, out T value) => parent.TryGetWithoutThrowingOnError(Property, out value);
//
//     protected abstract bool TryGetValueSchemaRuleValidator(string rule, out (Type Type, string[] AdditionalInfoProps) schemaRuleValidator);
//     
//     protected static Dictionary<string, (Type x, string[] AdditionalInfoProps)> GetValuesSchemaRuleValidator() => Assembly.GetExecutingAssembly().GetTypes()
//         .Where(t => typeof(SchemaRuleValidator<T>).IsAssignableFrom(t) && !t.IsAbstract && t.GetCustomAttribute<SchemaRuleAttribute>() != null)
//         .ToDictionary(x => x.GetCustomAttribute<SchemaRuleAttribute>()?.Rule, x => (x, x.GetCustomAttribute<SchemaRuleAttribute>()?.AdditionalInfoProps));
//
//     protected void ReadValueSchemaRuleValidators(BlittableJsonReaderObject propertySchemaDefinition, string path, List<ISchemaRuleValidator<T>> intRulesValidators)
//     {
//         foreach (var p in propertySchemaDefinition.GetPropertyNames())
//         {
//             if(p is SchemaValidatorConstants.type or SchemaValidatorConstants.description)
//                 //TODO Check if there more
//                 continue;
//
//             if(propertySchemaDefinition.TryGet(p, out object v) == false)
//                 //TODO Should not happen
//                 continue;
//                                 
//             var foundAdditionalInfoProps = new List<object> { v };
//                                 
//             if(TryGetValueSchemaRuleValidator(p, out var validatorInfo) == false)
//                 //Can happen if the restriction rule is an additional info for another rule
//                 continue;
//
//             foreach (string additionalInfoProp in validatorInfo.AdditionalInfoProps)
//             {
//                 if(propertySchemaDefinition.TryGet(additionalInfoProp, out object additionalInfoPropValue) == false)
//                     continue;
//                                     
//                 foundAdditionalInfoProps.Add(additionalInfoPropValue);
//             }
//
//             var factory = ValueSchemaRuleValidatorFactory.Create<T>(p, path, foundAdditionalInfoProps.ToArray());
//             intRulesValidators.Add(factory);
//         }
//     }
// }
