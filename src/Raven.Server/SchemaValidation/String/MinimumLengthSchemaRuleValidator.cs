using System.Text;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.String;

[SchemaRule("minLength")]
// ReSharper disable once UnusedType.Global
internal class MinimumLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    private readonly long _minLength;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MinimumLengthSchemaRuleValidator(string path, long minLength) : base(path)
    {
        _minLength = minLength;
    }

    protected override void ValidateInternal(string value, IErrorBuilder errorBuilder)
    {
        if(value.Length < _minLength)
            errorBuilder.AddError($"The length of the value at '{Path}' should be at least {_minLength}, but its actual length is {value.Length}.");
    }
}
