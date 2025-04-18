using System.Diagnostics.CodeAnalysis;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Untyped;

public class AnyOfSchemaRuleValidator : MultiSubschemaAggregatorValidator
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public AnyOfSchemaRuleValidator([NotNull] ElementSchemaRuleValidator[] validators)
        : base(validators)
    {
    }

    public override bool Validate(object value, ErrorBuilder errorBuilder)
    {
        foreach (var validator in Validators)
        {
            if (validator.Validate(value, null))
                return true;
        }
        errorBuilder?.AddError($"The value at '{errorBuilder.Path}' does not match any of the schema restrictions.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.AnyOf)]
// ReSharper disable once UnusedType.Global
public class AnyOfSchemaRuleValidatorFactory : MultiSubschemaAggregatorValidatorFactory<AnyOfSchemaRuleValidator>
{
    public override AnyOfSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        var validators = Read(schemaDefinition, schemaPath, refSchemas);
        return new AnyOfSchemaRuleValidator(validators);
    }
}
