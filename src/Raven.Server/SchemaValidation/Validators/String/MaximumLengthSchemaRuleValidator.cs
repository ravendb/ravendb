using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.String;

public class MaximumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    private readonly long _maxLength;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MaximumLengthSchemaRuleValidator(long maxLength)
    {
        _maxLength = maxLength;
    }

    public override bool Validate(LazyStringValue value, ErrorBuilder errorBuilder)
    {
        if (value.Length <= _maxLength) 
            return true;
        
        errorBuilder?.AddError($"The length of the {Target} '{value}' at '{errorBuilder.Path}' should not exceed {_maxLength}, but its actual length is {value.Length}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.MaxLength)]
// ReSharper disable once UnusedType.Global
public class MaximumLengthSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MaximumLengthSchemaRuleValidator>
{
    public override MaximumLengthSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath + Rule, out var maximumLength) 
            ? new MaximumLengthSchemaRuleValidator(maximumLength)
            : null;
    }
}
