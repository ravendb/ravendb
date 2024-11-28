using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.String;

internal class RegexLengthSchemaRuleValidator : StringSchemaRuleValidator
{
    public const string RuleName = "pattern";
    
    private readonly Regex _pattern;

    public static RegexLengthSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition)
    {
        if (schemaDefinition.TryGet(RuleName, out string maximum) == false)
            //TODO Should not happen. Also maybe collect all error to return full error report
            return null;

        return new RegexLengthSchemaRuleValidator(maximum);
    }
    
    // ReSharper disable once ConvertToPrimaryConstructor
    private RegexLengthSchemaRuleValidator(string pattern)
    {
        _pattern = new Regex(pattern, RegexOptions.Compiled);
    }

    protected override void ValidateInternal(string value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(_pattern.IsMatch(value) == false)
            errorBuilder.AddError($"The value '{value}' at '{path}' does not match the required pattern '{_pattern}'.");
    }
}
