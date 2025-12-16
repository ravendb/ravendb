using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.String;

public class MinimumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    private readonly long _minLength;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MinimumLengthSchemaRuleValidator(long minLength)
    {
        _minLength = minLength;
    }

    public override bool Validate(SchemaValidationContext context, LazyStringValue value)
    {
        if (value.Length >= _minLength) 
            return true;
        
        context.ErrorBuilder?.AddError($"The length of the {Target} '{value}' at '{context.ErrorBuilder.Path}' should be at least {_minLength}, but its actual length is {value.Length}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.MinLength)]
// ReSharper disable once UnusedType.Global
public class MinimumLengthSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MinimumLengthSchemaRuleValidator>
{
    public override MinimumLengthSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath + Rule, out var minimumLength) 
            ? new MinimumLengthSchemaRuleValidator(minimumLength)
            : null;
    }
}

