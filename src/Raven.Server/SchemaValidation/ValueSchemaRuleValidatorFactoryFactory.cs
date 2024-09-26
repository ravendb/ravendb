using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;

namespace Raven.Server.SchemaValidation;

internal class ValueSchemaRuleValidatorFactory(Type classType)
{
    private static readonly Dictionary<Type, Dictionary<string, ValueSchemaRuleValidatorFactory>> ValueSchemaRuleValidatorInfoPerType =
        new Dictionary<Type, Dictionary<string, ValueSchemaRuleValidatorFactory>>
        {
            { typeof(long), GetValuesSchemaRuleValidator<long>() }, 
            { typeof(string), GetValuesSchemaRuleValidator<string>() },
        };
    
    public static ISchemaRuleValidator<T> Create<T>(string rule, string path, object[] args)
    {
        var factory = ValueSchemaRuleValidatorInfoPerType[typeof(T)][rule];
        return factory.Create<T>(path, args);
    }
    
    private static Dictionary<string, ValueSchemaRuleValidatorFactory> GetValuesSchemaRuleValidator<T>() => Assembly.GetExecutingAssembly().GetTypes()
        .Select(x => (Type: x, RuleInfo: CustomAttributeExtensions.GetCustomAttribute<SchemaRuleAttribute>((MemberInfo)x)))
        .Where(x => typeof(SchemaRuleValidator<T>).IsAssignableFrom(x.Type) && !x.Type.IsAbstract && x.RuleInfo != null)
        .ToDictionary(x => x.RuleInfo.Rule, x => new ValueSchemaRuleValidatorFactory(x.Type));

    private ISchemaRuleValidator<T> Create<T>(string path, object[] args)
    {
        var ctor = classType.GetConstructors().FirstOrDefault(x => x.GetParameters().Length == args.Length + 1);
        if (ctor == null)
            //TODO To log the error up
            throw new Exception();

        var ctorParams = new List<object>{path};
        var ctorParamInfo = ctor.GetParameters();
        for (int i = 0; i < args.Length; i++)
        {
            var param = Convert.ChangeType(args[i], ctorParamInfo[i + 1].ParameterType, CultureInfo.InvariantCulture);
            ctorParams.Add(param);
        }

        return (SchemaRuleValidator<T>)ctor.Invoke(ctorParams.ToArray());
    }
}
