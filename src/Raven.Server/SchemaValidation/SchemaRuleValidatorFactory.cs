using System;
using System.Linq;
using System.Reflection;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public interface ISchemaRuleValidatorFactory
{
    ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath);
}

public abstract class SchemaRuleValidatorFactory<T> : ISchemaRuleValidatorFactory where T : ISchemaRuleValidator
{
    protected readonly string Rule;

    protected SchemaRuleValidatorFactory()
    {
        Rule = GetType().GetCustomAttribute<SchemaRuleAttribute>()?.Rule;
    }    
    
    public abstract ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath);
}
