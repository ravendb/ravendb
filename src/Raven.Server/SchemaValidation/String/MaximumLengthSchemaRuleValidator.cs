using Sparrow.Json;

namespace Raven.Server.SchemaValidation.String;

public class MaximumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    public const string RuleName = "maxLength";
    
    private readonly long _maxLength;

    public static MaximumLengthSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition)
    {
        if (schemaDefinition.TryGet(RuleName, out int maximum) == false)
            //TODO Should not happen. Also maybe collect all error to return full error report
            return null;

        return new MaximumLengthSchemaRuleValidator(maximum);
    }
    
    // ReSharper disable once ConvertToPrimaryConstructor
    private MaximumLengthSchemaRuleValidator(long maxLength)
    {
        _maxLength = maxLength;
    }

    protected override void ValidateInternal(string value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(value.Length > _maxLength)
            errorBuilder.AddError($"The length of the value at '{path}' should not exceed {_maxLength}, but its actual length is {value.Length}.");
    }
}
