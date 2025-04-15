using System.Text.RegularExpressions;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.String;

public class PatternSchemaRuleValidator : StringSchemaRuleValidator
{
    private readonly Regex _pattern;

    // ReSharper disable once ConvertToPrimaryConstructor
    public PatternSchemaRuleValidator(string pattern) 
    {
        _pattern = new Regex(pattern, RegexOptions.Compiled);
    }

    public override bool Validate(string value, ErrorBuilder errorBuilder)
    {
        if (_pattern.IsMatch(value)) 
            return true;
        
        errorBuilder?.AddError($"The pattern of the {Target} '{value}' at '{errorBuilder.Path}' does not match the required pattern '{_pattern}'.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.Pattern)]
// ReSharper disable once UnusedType.Global
public class PatternSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<PatternSchemaRuleValidator>
{
    public override PatternSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        return SchemaValidationHelper.TryGetString(schemaDefinition, Rule, schemaPath.FullPath, out var pattern) 
            ? new PatternSchemaRuleValidator(pattern)
            : null;
    }
}
