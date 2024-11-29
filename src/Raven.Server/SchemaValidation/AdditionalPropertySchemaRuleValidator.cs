using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

//TODO To add when a logical path is added
// [DebuggerDisplay("'{_property}' property validator" )]
public class AdditionalPropertySchemaRuleValidator : PropertySchemaRuleValidator
{
    protected void Validate(BlittableJsonReaderObject parent, IEnumerable<string> properties, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        foreach (var property in properties)
        {
            Validate(parent, property, path, errorBuilder);
        }
    }
}
