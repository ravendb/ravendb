using System.Text.RegularExpressions;

namespace Raven.Server.SchemaValidation.String;

[SchemaRule("pattern")]
// ReSharper disable once UnusedType.Global
internal class RegexLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    private readonly Regex _pattern;

    // ReSharper disable once ConvertToPrimaryConstructor
    public RegexLengthSchemaRuleValidator(string path, string pattern) : base(path)
    {
        _pattern = new Regex(pattern);
    }

    protected override void ValidateInternal(string value, IErrorBuilder errorBuilder)
    {
        if(_pattern.IsMatch(value) == false)
            errorBuilder.AddError($"The value '{value}' at '{Path}' does not match the required pattern '{_pattern}'.");
    }
}
