using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.String;

public class MinimumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    private readonly long _minLength;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MinimumLengthSchemaRuleValidator(long minLength)
    {
        _minLength = minLength;
    }

    protected override bool ValidateInternal(string value, IErrorBuilder errorBuilder)
    {
        if (value.Length >= _minLength) 
            return true;
        
        errorBuilder?.AddError($"The length of the {Target} '{value}' at '{errorBuilder.Path}' should be at least {_minLength}, but its actual length is {value.Length}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.minLength)]
public class MinimumLengthSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MinimumLengthSchemaRuleValidator>
{
    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath.FullPath, out var minimumLength) 
            ? new MinimumLengthSchemaRuleValidator(minimumLength)
            : null;
    }
}

