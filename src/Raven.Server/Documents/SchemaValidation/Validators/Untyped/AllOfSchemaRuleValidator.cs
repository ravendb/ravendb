using System.Diagnostics.CodeAnalysis;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Untyped;

public class AllOfSchemaRuleValidator : MultiSubschemaAggregatorValidator
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public AllOfSchemaRuleValidator([NotNull] ElementSchemaRuleValidator[] validators)
        :base(validators)
    {
    }

    public override bool Validate(object value, ErrorBuilder errorBuilder)
    {
        var validate = true;
        foreach (var validator in Validators)
        {
            validate &= validator.Validate(value, errorBuilder);
            if(errorBuilder == null && validate == false)
               return false;
        }
        return validate;
    }
}

[SchemaRule(SchemaValidatorConstants.AllOf)]
// ReSharper disable once UnusedType.Global
public class AllOfSchemaRuleValidatorFactory : MultiSubschemaAggregatorValidatorFactory<AllOfSchemaRuleValidator>
{
    public override AllOfSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        var validators = Read(schemaDefinition, schemaPath, refSchemas);
        return new AllOfSchemaRuleValidator(validators);
    }
}

