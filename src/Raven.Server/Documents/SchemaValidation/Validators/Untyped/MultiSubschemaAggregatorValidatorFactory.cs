using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Untyped;

public abstract class MultiSubschemaAggregatorValidator : SchemaRuleValidator<object>
{
    protected readonly ElementSchemaRuleValidator[] Validators;

    // ReSharper disable once ConvertToPrimaryConstructor
    protected MultiSubschemaAggregatorValidator([NotNull] ElementSchemaRuleValidator[] validators)
    {
        Validators = validators;
    }
}


public abstract class MultiSubschemaAggregatorValidatorFactory<T> : SchemaRuleValidatorFactory<T> where T : MultiSubschemaAggregatorValidator
{
    public ElementSchemaRuleValidator[] Read(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        schemaPath += Rule;
        if (SchemaValidationHelper.TryGetArray(schemaDefinition, Rule, schemaPath, out var validatorsSchema) == false)
            return null;

        List<ElementSchemaRuleValidator> validators = null;
        for (int i = 0; i < validatorsSchema.Length; i++)
        {
            var itemPath = schemaPath + i;
            var itemSchema = SchemaValidationHelper.CheckTypeAndThrow<BlittableJsonReaderObject>(validatorsSchema[i], itemPath);
            (validators ??= []).Add(ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(context, itemSchema, itemPath));
        }
        if(validators == null)
            return null;

        return validators.ToArray();
    }
}
