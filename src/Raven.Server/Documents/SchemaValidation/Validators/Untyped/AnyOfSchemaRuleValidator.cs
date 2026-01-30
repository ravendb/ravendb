using System.Diagnostics.CodeAnalysis;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Untyped;

public class AnyOfSchemaRuleValidator : MultiSubschemaAggregatorValidator
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public AnyOfSchemaRuleValidator([NotNull] ElementSchemaRuleValidator[] validators)
        : base(validators)
    {
    }

    public override bool Validate(SchemaValidationContext context, object value)
    {
        foreach (var validator in Validators)
        {
            using (context.WithoutCollectingErrors())
            {
                if (validator.Validate(context, value))
                    return true;
            }
        }
            
        context.ErrorBuilder?.AddError($"The value at '{context.ErrorBuilder.Path}' does not match any of the schema restrictions.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.AnyOf)]
// ReSharper disable once UnusedType.Global
public class AnyOfSchemaRuleValidatorFactory : MultiSubschemaAggregatorValidatorFactory<AnyOfSchemaRuleValidator>
{
    public override AnyOfSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        var validators = Read(context, schemaDefinition, schemaPath);
        return new AnyOfSchemaRuleValidator(validators);
    }
}
