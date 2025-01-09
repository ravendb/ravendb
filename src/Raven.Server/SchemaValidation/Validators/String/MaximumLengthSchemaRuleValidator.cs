using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.String;

[SchemaRule(SchemaValidatorConstants.maxLength)]
public class MaximumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    private readonly long _maxLength;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MaximumLengthSchemaRuleValidator(long maxLength)
    {
        _maxLength = maxLength;
    }

    protected override bool ValidateInternal(string value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (value.Length <= _maxLength) 
            return true;
        
        errorBuilder?.AddError($"The length of the {Target} '{value}' at '{path}' should not exceed {_maxLength}, but its actual length is {value.Length}.");
        return false;
    }
}

// ReSharper disable once UnusedType.Global
public class MaximumLengthSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MaximumLengthSchemaRuleValidator>
{
    public override MaximumLengthSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath, out var maximumLength) 
            ? new MaximumLengthSchemaRuleValidator(maximumLength)
            : null;
    }
}
