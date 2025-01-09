using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.String;

[SchemaRule(SchemaValidatorConstants.pattern)]
public class PatternSchemaRuleValidator : StringSchemaRuleValidator
{
    private readonly Regex _pattern;

    // ReSharper disable once ConvertToPrimaryConstructor
    public PatternSchemaRuleValidator(string pattern) 
    {
        _pattern = new Regex(pattern, RegexOptions.Compiled);
    }

    protected override bool ValidateInternal(string value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (_pattern.IsMatch(value)) 
            return true;
        
        errorBuilder?.AddError($"The pattern of the {Target} '{value}' at '{path}' does not match the required pattern '{_pattern}'.");
        return false;
    }
}

// ReSharper disable once UnusedType.Global
public class PatternSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<PatternSchemaRuleValidator>
{
    public override PatternSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        return SchemaValidationHelper.TryGetString(schemaDefinition, Rule, schemaPath, out var pattern) 
            ? new PatternSchemaRuleValidator(pattern)
            : null;
    }
}
