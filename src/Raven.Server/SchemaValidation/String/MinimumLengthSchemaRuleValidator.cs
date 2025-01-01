using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.String;

internal class MinimumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    public const string RuleName = "minLength";
    
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

public class MinimumLengthSchemaRuleValidatorFactory : SchemaRuleValidatorFactory
{
    protected override string Rule => MinimumLengthSchemaRuleValidator.RuleName;

    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        const BlittableJsonToken expectedType = BlittableJsonToken.Integer;
        if (type != expectedType)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], expectedType, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out long minimumLength) == false)
            throw new InvalidOperationException($"'{Rule}' must to be convertable to decimal here. Should not happen");
        
        return new MinimumLengthSchemaRuleValidator(minimumLength);
    }
}

