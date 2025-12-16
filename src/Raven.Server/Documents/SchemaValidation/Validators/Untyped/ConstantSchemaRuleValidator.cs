using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Untyped;

public class ConstantSchemaRuleValidator : FixedValueSchemaRuleValidator
{
    private readonly object _constantValue;
    private readonly object _constantValueForError;

    // ReSharper disable once ConvertToPrimaryConstructor
    public ConstantSchemaRuleValidator(object constantValue)
    {
        _constantValue = ConvertTypeForComparison(constantValue);
        _constantValueForError = IsString(_constantValue) ? $"\"{_constantValue}\"" : _constantValue;
    }

    public override bool Validate(SchemaValidationContext context, object value)
    {
        //The order here is extremely important since when comparing between blittable objects the function uses the first object context and _constantValue is used concurrently 
        if (Equals(ConvertTypeForComparison(value), _constantValue)) 
            return true;

        var quoteIfString = IsString(value) ? "\"" : "";
        context.ErrorBuilder?.AddError($"The value at '{context.ErrorBuilder.Path}' must be '{_constantValueForError}', but it is '{quoteIfString}{value}{quoteIfString}'.");
        return false;
    }

    protected override bool CheckTypeAndGetValue(object value, out object tValue)
    {
        tValue = ConvertTypeForComparison(value);
        return true;
    }
}

[SchemaRule(SchemaValidatorConstants.Const)]
// ReSharper disable once UnusedType.Global
public class ConstantSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<ConstantSchemaRuleValidator>
{
    public override ConstantSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return schemaDefinition.TryGet(Rule, out object multipleOf)
            ? new ConstantSchemaRuleValidator(multipleOf) 
            : null;
    }
}
