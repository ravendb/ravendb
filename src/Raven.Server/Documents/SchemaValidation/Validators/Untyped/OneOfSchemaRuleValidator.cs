using System.Diagnostics.CodeAnalysis;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Untyped;

public class OneOfSchemaRuleValidator : MultiSubschemaAggregatorValidator
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public OneOfSchemaRuleValidator([NotNull] ElementSchemaRuleValidator[] validators)
        : base(validators)
    {
    }

    public override bool Validate(SchemaValidationContext context, object value)
    {
        var alreadyHasOneValid = false;
        foreach (var validator in Validators)
        {
            using (context.WithoutCollectingErrors())
            {
                if(validator.Validate(context, value) == false)
                    continue;
            }
            
            if(alreadyHasOneValid)
            {                
                context.ErrorBuilder?.AddError($"The value at '{context.ErrorBuilder.Path}' matches more than one schema, but it must match exactly one.");
                return false;
            }

            alreadyHasOneValid = true;
        }

        if (alreadyHasOneValid == false)
        {
            context.ErrorBuilder?.AddError($"The value at '{context.ErrorBuilder.Path}' does not match any of the schema restrictions, and it must match exactly one.");
            return false;
        }

        return true;
    }
}

[SchemaRule(SchemaValidatorConstants.OneOf)]
// ReSharper disable once UnusedType.Global
public class OneOfSchemaRuleValidatorFactory : MultiSubschemaAggregatorValidatorFactory<OneOfSchemaRuleValidator>
{
    public override OneOfSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        var validators = Read(context, schemaDefinition, schemaPath);
        return new OneOfSchemaRuleValidator(validators);
    }
}
