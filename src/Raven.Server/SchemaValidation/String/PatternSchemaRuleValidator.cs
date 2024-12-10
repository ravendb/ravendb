using System;
using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.String;

internal class PatternSchemaRuleValidator : StringSchemaRuleValidator
{
    public const string RuleName = "pattern";
    
    private readonly Regex _pattern;

    public static ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if (schemaDefinition.TryGet(RuleName, out object maximumObj) == false)
            return null;

        if (maximumObj is string maximum == false)
            throw new InvalidSchemaValidationDefinitionException(
                $"The value of 'pattern' at '{schemaPath}' must be a string, but received '{maximumObj}' of type '{SchemaValidationHelper.GetPublicTypeOfObj(maximumObj)}'.");

        return new PatternSchemaRuleValidator(maximum);
    }
    
    // ReSharper disable once ConvertToPrimaryConstructor
    public PatternSchemaRuleValidator(string pattern)
    {
        _pattern = new Regex(pattern, RegexOptions.Compiled);
    }

    protected override void ValidateInternal(string value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(_pattern.IsMatch(value) == false)
            errorBuilder.AddError($"The value '{value}' at '{path}' does not match the required pattern '{_pattern}'.");
    }
}

public class PatternSchemaRuleValidatorFactory : SchemaRuleValidatorFactory
{
    protected override string Rule => PatternSchemaRuleValidator.RuleName;

    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        if (type != BlittableJsonToken.String)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], BlittableJsonToken.String, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out string pattern) == false)
            throw new InvalidOperationException($"'{Rule}' must to convertable to decimal here. Should not happen");
        
        return new PatternSchemaRuleValidator(pattern);
    }
}
