namespace Raven.Server.SchemaValidation.String;

[SchemaRule("maxLength")]
// ReSharper disable once UnusedType.Global
public class MaximumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    private readonly long _maxLength;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MaximumLengthSchemaRuleValidator(string path, long maxLength) : base(path)
    {
        _maxLength = maxLength;
    }

    protected override void ValidateInternal(string value, IErrorBuilder errorBuilder)
    {
        if(value.Length > _maxLength)
            errorBuilder.AddError($"The length of the value at '{Path}' should not exceed {_maxLength}, but its actual length is {value.Length}.");
    }
}
