using System.Diagnostics;
using System.Text.RegularExpressions;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

[DebuggerDisplay("'{_propertyPattern}' pattern property validator")]
public class PatternPropertiesSchemaRuleValidator : PropertySchemaRuleValidator
{
    private readonly Regex _propertyPattern;
    
    // ReSharper disable once ConvertToPrimaryConstructor
    public PatternPropertiesSchemaRuleValidator(string propertyPattern) 
    {
        _propertyPattern = new Regex(propertyPattern, RegexOptions.Compiled);
    }

    public override void Validate(BlittableJsonReaderObject parent, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        foreach (var prop in parent.GetPropertyNames())
        {
            if(_propertyPattern.IsMatch(prop) == false)
                continue;
            
            path.StepIn(prop);
            Validate(parent, prop, path, errorBuilder);
            path.StepOut();
        }
    }
}
