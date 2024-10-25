using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

internal class ValueSchemaRuleValidatorFactory(Type classType, Type argType, string[] additionalInfoProps)
{
    public Type ArgType => argType;
    public string[] AdditionalInfoProps { get; } = additionalInfoProps;

    private static readonly Dictionary<string, List<ValueSchemaRuleValidatorFactory>> CashedValuesSchemaRuleValidator =
        GetValuesSchemaRuleValidator();
    
    public static bool TryGetValueSchemaRuleValidatorFactory(string rule, out List<ValueSchemaRuleValidatorFactory> schemaRuleValidator)
    {
        return CashedValuesSchemaRuleValidator.TryGetValue(rule, out schemaRuleValidator);
    }
    
    private static Dictionary<string, List<ValueSchemaRuleValidatorFactory>> GetValuesSchemaRuleValidator()
    {
        var ret = new Dictionary<string, List<ValueSchemaRuleValidatorFactory>>();
        
        var schemaRuleValidators = Assembly.GetExecutingAssembly().GetTypes()
            .Select(x => (Type: x, RuleInfo: x.GetCustomAttribute<SchemaRuleAttribute>()))
            .Where(x => typeof(SchemaRuleValidator).IsAssignableFrom(x.Type) && !x.Type.IsAbstract && x.RuleInfo != null);
        
        foreach (var validator in schemaRuleValidators)
        {
            var rule = validator.RuleInfo.Rule;
            if (ret.TryGetValue(rule, out var list) == false)
                ret[rule] = list = new List<ValueSchemaRuleValidatorFactory>();

            var argType = GetGenericSchemaRuleValidatorType(validator.Type);
            list.Add(new ValueSchemaRuleValidatorFactory(validator.Type, argType, validator.RuleInfo.AdditionalInfoProps));
        }

        return ret;
    }
    
    private static Type GetGenericSchemaRuleValidatorType(Type type)
    {
        if (type == null || type == typeof(object))
            return null;
        
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(SchemaRuleValidator<>))
            return type.GetGenericArguments()[0];

        return GetGenericSchemaRuleValidatorType(type.BaseType);
    }
    public bool TryCreate(string path, object[] args, out SchemaRuleValidator validator)
    {
        var ctor = classType.GetConstructors().FirstOrDefault(x => x.GetParameters().Length == args.Length + 1);
        if (ctor == null)
            //TODO To log the error up
            throw new Exception();

        var ctorParams = new List<object>{path};
        var ctorParamInfo = ctor.GetParameters();
        for (int i = 0; i < args.Length; i++)
        {
            if (TryChangeTypeIfNeeded(ref args[i], ctorParamInfo[i + 1].ParameterType) == false)
            {
                validator = null;
                return false;
            }
            ctorParams.Add(args[i]);
        }

        validator = (SchemaRuleValidator)ctor.Invoke(ctorParams.ToArray());
        return true;
    }

    private static bool TryChangeTypeIfNeeded(ref object arg, Type parameterType)
    {
        if (arg.GetType() == parameterType)
            return true;

        if (parameterType == typeof(decimal))
        {
            if (arg is LazyNumberValue lArg)
            {
                arg = (decimal)lArg;
                return true;
            }

            if (arg is long longArg)
            {
                arg = (decimal)longArg;
                return true;
            }
        }
        else if(parameterType == typeof(string))
        {
            if (arg is LazyStringValue or LazyCompressedStringValue)
            {
                arg = arg.ToString();
                return true;
            }
        }        

        return false;
    }
}
