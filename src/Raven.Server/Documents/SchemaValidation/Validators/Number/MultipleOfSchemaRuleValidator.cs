using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Number;

public class MultipleOfSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _multipleOf;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MultipleOfSchemaRuleValidator(decimal multipleOf)
    {
        _multipleOf = multipleOf;
    }

    public override bool Validate(SchemaValidationContext context, decimal value)
    {
        if (value % _multipleOf == 0) 
            return true;
        
        context.ErrorBuilder?.AddError($"The value '{value}' at '{context.ErrorBuilder.Path}' should be a multiple of {_multipleOf}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.MultipleOf)]
// ReSharper disable once UnusedType.Global
public class MultipleOfSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MultipleOfSchemaRuleValidator>
{
    public override MultipleOfSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetNumber(schemaDefinition, Rule, schemaPath + Rule, out var multipleOf)
            ? new MultipleOfSchemaRuleValidator(multipleOf) 
            : null;
    }
}
