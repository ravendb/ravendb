using System;
using System.Linq;
using System.Reflection;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public interface ISchemaRuleValidatorFactory
{
    ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath);
}

public abstract class SchemaRuleValidatorFactory<T> : ISchemaRuleValidatorFactory where T : ISchemaRuleValidator
{
    protected readonly string Rule = typeof(T).GetCustomAttribute<SchemaRuleAttribute>()?.Rule 
                                     ?? throw new InvalidOperationException($"The type '{typeof(T).Name}' must have a SchemaRuleAttribute defined with a Rule property.");

    public abstract ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath);
}
