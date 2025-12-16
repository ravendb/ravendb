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

    public override bool Validate(SchemaValidationContext context, object value)
    {
        var validate = true;
        foreach (var validator in Validators)
        {
            validate &= validator.Validate(context, value);
            if(context.ErrorBuilder == null && validate == false)
               return false;
        }
        return validate;
    }
}

[SchemaRule(SchemaValidatorConstants.AllOf)]
// ReSharper disable once UnusedType.Global
public class AllOfSchemaRuleValidatorFactory : MultiSubschemaAggregatorValidatorFactory<AllOfSchemaRuleValidator>
{
    public override AllOfSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        var validators = Read(context, schemaDefinition, schemaPath);
        return new AllOfSchemaRuleValidator(validators);
    }
}

