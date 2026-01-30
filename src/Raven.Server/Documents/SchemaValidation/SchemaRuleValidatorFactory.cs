using System.Reflection;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation;

public interface ISchemaRuleValidatorFactory
{
    ISchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath);
}

public abstract class SchemaRuleValidatorFactory<T> : ISchemaRuleValidatorFactory where T : ISchemaRuleValidator
{
    protected readonly string Rule;
    protected SchemaRuleValidatorFactory() => Rule = GetType().GetCustomAttribute<SchemaRuleAttribute>()?.Rule;
    ISchemaRuleValidator ISchemaRuleValidatorFactory.Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath) => Create(context, schemaDefinition, schemaPath);
    public abstract T Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath);
}
