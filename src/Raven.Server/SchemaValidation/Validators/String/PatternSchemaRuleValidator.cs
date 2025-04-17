using System;
using System.Buffers;
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

    public override bool Validate(LazyStringValue value, ErrorBuilder errorBuilder)
    {
        var buffer = ArrayPool<char>.Shared.Rent(value.Length);
        try
        {
            value.TryCopyTo(buffer);
            if (_pattern.IsMatch(buffer.AsSpan(0, value.Length))) 
                return true;
        
            errorBuilder?.AddError($"The pattern of the {Target} '{value}' at '{errorBuilder.Path}' does not match the required pattern '{_pattern}'.");
            return false;
        }
        finally
        {
            ArrayPool<char>.Shared.Return(buffer);
        }
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
