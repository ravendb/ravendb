using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

public class MultipleOfSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _multipleOf;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MultipleOfSchemaRuleValidator(decimal multipleOf)
    {
        _multipleOf = multipleOf;
    }

    public override bool Validate(decimal value, ErrorBuilder errorBuilder)
    {
        if (value % _multipleOf == 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{errorBuilder.Path}' should be a multiple of {_multipleOf}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.MultipleOf)]
// ReSharper disable once UnusedType.Global
public class MultipleOfSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MultipleOfSchemaRuleValidator>
{
    public override MultipleOfSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        return SchemaValidationHelper.TryGetNumber(schemaDefinition, Rule, schemaPath + Rule, out var multipleOf)
            ? new MultipleOfSchemaRuleValidator(multipleOf) 
            : null;
    }
}
