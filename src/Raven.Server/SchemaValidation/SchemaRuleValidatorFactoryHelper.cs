using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

//TODO Find better name
public abstract class SchemaRuleValidatorFactoryHelper
{
    private static readonly Dictionary<string, ISchemaRuleValidatorFactory> SchemaRuleValidatorFactories;

    static SchemaRuleValidatorFactoryHelper()
    {
        SchemaRuleValidatorFactories = typeof(ISchemaRuleValidatorFactory).Assembly.GetTypes()
            .Select(t =>
            {
                if (t.IsClass == false || t.IsAbstract)
                    return null;

                if (!typeof(ISchemaRuleValidatorFactory).IsAssignableFrom(t))
                    return null;

                var baseType = t;
                while (baseType != null && (baseType.IsGenericType == false || baseType.GetGenericTypeDefinition() != typeof(SchemaRuleValidatorFactory<>)))
                    baseType = baseType.BaseType;

                if (baseType == null)
                    return null;

                var validatorType = baseType.GetGenericArguments()[0];
                if (!typeof(ISchemaRuleValidator).IsAssignableFrom(validatorType))
                    return null;

                var schemaRuleAttribute = CustomAttributeExtensions.GetCustomAttribute<SchemaRuleAttribute>((MemberInfo)validatorType);
                if (schemaRuleAttribute == null)
                    return null;

                if (Activator.CreateInstance(t) is not ISchemaRuleValidatorFactory factoryInstance)
                    throw new InvalidOperationException($"Unable to create an instance of factory type '{t.Name}'.");

                return new
                {
                    schemaRuleAttribute.Rule,
                    FactoryInstance = factoryInstance
                };
            })
            .Where(x => x != null) // Filter out nulls
            .ToDictionary(x => x!.Rule, x => x!.FactoryInstance);;
    }
    
    public static bool TryCreateValidator(string rule, BlittableJsonReaderObject schemaDefinition, string schemaPath, out ISchemaRuleValidator validator)
    {
        if (SchemaRuleValidatorFactories.TryGetValue(rule, out ISchemaRuleValidatorFactory factory))
        {
            validator = factory.Create(schemaDefinition, schemaPath);
            return true;
        }
        validator = null;
        return false;
    }
    
    internal static string[] ForTestGetRuleNames() => SchemaRuleValidatorFactories.Keys.ToArray();
}
