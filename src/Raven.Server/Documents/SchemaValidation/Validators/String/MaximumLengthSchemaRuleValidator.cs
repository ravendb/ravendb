using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.String;

public class MaximumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    private readonly long _maxLength;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MaximumLengthSchemaRuleValidator(long maxLength)
    {
        _maxLength = maxLength;
    }

    public override bool Validate(SchemaValidationContext context, LazyStringValue value)
    {
        if (value.Length <= _maxLength) 
            return true;
        
        context.ErrorBuilder?.AddError($"The length of the {Target} '{value}' at '{context.ErrorBuilder.Path}' should not exceed {_maxLength}, but its actual length is {value.Length}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.MaxLength)]
// ReSharper disable once UnusedType.Global
public class MaximumLengthSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MaximumLengthSchemaRuleValidator>
{
    public override MaximumLengthSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath + Rule, out var maximumLength) 
            ? new MaximumLengthSchemaRuleValidator(maximumLength)
            : null;
    }
}
