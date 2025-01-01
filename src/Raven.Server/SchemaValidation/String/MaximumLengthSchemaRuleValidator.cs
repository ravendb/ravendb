using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.String;

public class MaximumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    public const string RuleName = "maxLength";
    
    private readonly long _maxLength;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MaximumLengthSchemaRuleValidator(long maxLength)
    {
        _maxLength = maxLength;
    }

    protected override void ValidateInternal(string value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(value.Length > _maxLength)
            errorBuilder.AddError($"The length of the {Target} '{value}' at '{path}' should not exceed {_maxLength}, but its actual length is {value.Length}.");
    }
}

public class MaximumLengthSchemaRuleValidatorFactory : SchemaRuleValidatorFactory
{
    protected override string Rule => MaximumLengthSchemaRuleValidator.RuleName;

    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        const BlittableJsonToken expectedType = BlittableJsonToken.Integer;
        if (type != expectedType)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], expectedType, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out long maximumLength) == false)
            throw new InvalidOperationException($"'{Rule}' must to be convertable to decimal here. Should not happen");
        
        return new MaximumLengthSchemaRuleValidator(maximumLength);
    }
}
