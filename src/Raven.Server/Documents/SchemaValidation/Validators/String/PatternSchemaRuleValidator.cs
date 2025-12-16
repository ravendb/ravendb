using System;
using System.Buffers;
using System.Text.RegularExpressions;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.String;

public class PatternSchemaRuleValidator : StringSchemaRuleValidator
{
    public const int MaxTimeoutInMilliseconds = 2147483646;

    private readonly Regex _regex;

    // ReSharper disable once ConvertToPrimaryConstructor
    public PatternSchemaRuleValidator(string pattern, TimeSpan timeout)
    {
        try
        {
            _regex = new Regex(pattern, RegexOptions.Compiled, timeout);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    public override bool Validate(SchemaValidationContext context, LazyStringValue value)
    {
        var buffer = ArrayPool<char>.Shared.Rent(value.Length);
        try
        {
            value.TryCopyTo(buffer);
            if (_regex.IsMatch(buffer.AsSpan(0, value.Length)))
                return true;
        
            context.ErrorBuilder?.AddError($"The pattern of the {Target} '{value}' at '{context.ErrorBuilder.Path}' does not match the required pattern '{_regex}'.");
            return false;
        }
        catch (RegexMatchTimeoutException)
        {
            context.ErrorBuilder?.AddError($"The pattern matching of the {Target} '{value}' at '{context.ErrorBuilder.Path}' timed out for pattern '{_regex}'.");
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
    public override PatternSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetString(schemaDefinition, Rule, schemaPath + Rule, out var pattern) 
            ? new PatternSchemaRuleValidator(pattern, context.Configuration.RegexTimeout)
            : null;
    }
}
