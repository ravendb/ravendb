using System.Diagnostics.CodeAnalysis;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Untyped;

public class OneOfSchemaRuleValidator : MultiSubschemaAggregatorValidator
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public OneOfSchemaRuleValidator([NotNull] ElementSchemaRuleValidator[] validators)
        : base(validators)
    {
    }

    public override bool Validate(object value, ErrorBuilder errorBuilder)
    {
        var alreadyHasOneValid = false;
        foreach (var validator in Validators)
        {
            if(validator.Validate(value, null) == false)
                continue;
            
            if(alreadyHasOneValid)
            {                
                errorBuilder?.AddError($"The value at '{errorBuilder.Path}' matches more than one schema, but it must match exactly one.");
                return false;
            }

            alreadyHasOneValid = true;
        }
        return true;
    }
}

[SchemaRule(SchemaValidatorConstants.OneOf)]
// ReSharper disable once UnusedType.Global
public class OneOfSchemaRuleValidatorFactory : MultiSubschemaAggregatorValidatorFactory<OneOfSchemaRuleValidator>
{
    public override OneOfSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        var validators = Read(schemaDefinition, schemaPath, refSchemas);
        return new OneOfSchemaRuleValidator(validators);
    }
}
