using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

[DebuggerDisplay("'{PropertySpecifier}' pattern property validator")]
public class SpecifiedPropertySchemaRuleValidator  : PropertySchemaRuleValidator
{
    public string PropertySpecifier;

    public SpecifiedPropertySchemaRuleValidator(string propertySpecifier)
    {
        PropertySpecifier = propertySpecifier;
    }
    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        base.Init(schemaDefinition);
    }
}
