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

    public static bool TryCreateValidator(SchemaBuilderContext context, string rule, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath,
        out ISchemaRuleValidator validator)
    {
        if (SchemaRuleValidatorFactories.TryGetValue(rule, out ISchemaRuleValidatorFactory factory))
        {
            validator = factory.Create(context, schemaDefinition, schemaPath);
            return validator != null;
        }
        validator = null;
        return false;
    }

    public static ObjectSchemaRuleValidator CreateObjectValidator(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return ObjectSchemaRuleValidatorFactory.Create(context, schemaDefinition, schemaPath);
    }
    public static ArraySchemaRuleValidator CreateArrayValidator(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return ArraySchemaRuleValidatorFactory.Create(context, schemaDefinition, schemaPath);
    }

    internal static class TestingStuff
    {
        public static string[] GetRuleNames() => SchemaRuleValidatorFactories.Keys.ToArray();
    } 
}
