using System;
using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.String;

internal class PatternSchemaRuleValidator : StringSchemaRuleValidator
{
    public const string RuleName = "pattern";
    
    private readonly Regex _pattern;

    // ReSharper disable once ConvertToPrimaryConstructor
    public PatternSchemaRuleValidator(string pattern) 
    {
        _pattern = new Regex(pattern, RegexOptions.Compiled);
    }

    protected override void ValidateInternal(string value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(_pattern.IsMatch(value) == false)
            errorBuilder.AddError($"The pattern of the {Target} '{value}' at '{path}' does not match the required pattern '{_pattern}'.");
    }
}

public class PatternSchemaRuleValidatorFactory : SchemaRuleValidatorFactory
{
    protected override string Rule => PatternSchemaRuleValidator.RuleName;

    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        var pattern = GetStringOrThrow(Rule, schemaDefinition, schemaPath, type);

        return new PatternSchemaRuleValidator(pattern);
    }
}
