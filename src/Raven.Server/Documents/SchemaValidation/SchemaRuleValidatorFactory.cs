using System.Reflection;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation;

public interface ISchemaRuleValidatorFactory
{
    ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas);
}

public abstract class SchemaRuleValidatorFactory<T> : ISchemaRuleValidatorFactory where T : ISchemaRuleValidator
{
    protected readonly string Rule;
    protected SchemaRuleValidatorFactory() => Rule = GetType().GetCustomAttribute<SchemaRuleAttribute>()?.Rule;
    ISchemaRuleValidator ISchemaRuleValidatorFactory.Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas) => Create(schemaDefinition, schemaPath, refSchemas);
    public abstract T Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas);
}
