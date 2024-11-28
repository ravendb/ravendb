using Sparrow.Json;

namespace Raven.Server.SchemaValidation.String;

internal class MinimumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    public const string RuleName = "minLength";
    
    private readonly long _minLength;

    public static MinimumLengthSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition)
    {
        if (schemaDefinition.TryGet(RuleName, out int maximum) == false)
            //TODO Should not happen. Also maybe collect all error to return full error report
            return null;

        return new MinimumLengthSchemaRuleValidator(maximum);
    }
    
    // ReSharper disable once ConvertToPrimaryConstructor
    private MinimumLengthSchemaRuleValidator(long minLength)
    {
        _minLength = minLength;
    }

    protected override void ValidateInternal(string value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(value.Length < _minLength)
            errorBuilder.AddError($"The length of the value at '{path}' should be at least {_minLength}, but its actual length is {value.Length}.");
    }
}
