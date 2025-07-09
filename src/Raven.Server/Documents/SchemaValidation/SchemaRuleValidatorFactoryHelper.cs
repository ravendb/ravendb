using System;
using System.Collections.Frozen;
using System.Linq;
using System.Reflection;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Raven.Server.Documents.SchemaValidation.Validators.Array;
using Raven.Server.Documents.SchemaValidation.Validators.Object;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation;

public abstract class SchemaRuleValidatorFactoryHelper
{
    private static readonly FrozenDictionary<string, ISchemaRuleValidatorFactory> SchemaRuleValidatorFactories;
    private static readonly ObjectSchemaRuleValidatorFactory ObjectSchemaRuleValidatorFactory = new ObjectSchemaRuleValidatorFactory();
    private static readonly ArraySchemaRuleValidatorFactory ArraySchemaRuleValidatorFactory = new ArraySchemaRuleValidatorFactory();

    static SchemaRuleValidatorFactoryHelper()
    {
        SchemaRuleValidatorFactories = typeof(ISchemaRuleValidatorFactory).Assembly.GetTypes()
            .Select(t =>
            {
                if (t.IsClass == false || t.IsAbstract)
                    return null;

                if (typeof(ISchemaRuleValidatorFactory).IsAssignableFrom(t) == false)
                    return null;

                var rule = t.GetCustomAttribute<SchemaRuleAttribute>()?.Rule;
                if (rule == null)
                    return null;

                if (Activator.CreateInstance(t) is not ISchemaRuleValidatorFactory factoryInstance)
                    throw new InvalidOperationException($"Unable to create an instance of factory type '{t.Name}'.");

                return new { Rule = rule, FactoryInstance = factoryInstance };
            })
            .Where(x => x != null) // Filter out nulls
            .ToFrozenDictionary(x => x.Rule, x => x!.FactoryInstance);
    }

    public static bool TryCreateValidator(string rule, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas,
        out ISchemaRuleValidator validator)
    {
        if (SchemaRuleValidatorFactories.TryGetValue(rule, out ISchemaRuleValidatorFactory factory))
        {
            validator = factory.Create(schemaDefinition, schemaPath, refSchemas);
            return validator != null;
        }
        validator = null;
        return false;
    }

    public static ObjectSchemaRuleValidator CreateObjectValidator(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        return ObjectSchemaRuleValidatorFactory.Create(schemaDefinition, schemaPath, refSchemas);
    }
    public static ArraySchemaRuleValidator CreateArrayValidator(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        return ArraySchemaRuleValidatorFactory.Create(schemaDefinition, schemaPath, refSchemas);
    }

    internal static class TestingStuff
    {
        public static string[] GetRuleNames() => SchemaRuleValidatorFactories.Keys.ToArray();
    } 
}
