using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.String;

[SchemaRule(SchemaValidatorConstants.minLength)]
public class MinimumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    private readonly long _minLength;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MinimumLengthSchemaRuleValidator(long minLength)
    {
        _minLength = minLength;
    }

    protected override void ValidateInternal(string value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(value.Length < _minLength)
            errorBuilder.AddError($"The length of the {Target} '{value}' at '{path}' should be at least {_minLength}, but its actual length is {value.Length}.");
    }
}

// ReSharper disable once UnusedType.Global
public class MinimumLengthSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MinimumLengthSchemaRuleValidator>
{
    public override MinimumLengthSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        return SchemaValidationHelper.TryGetInteger(schemaDefinition, Rule, schemaPath, out var minimumLength) 
            ? new MinimumLengthSchemaRuleValidator(minimumLength)
            : null;
    }
}

