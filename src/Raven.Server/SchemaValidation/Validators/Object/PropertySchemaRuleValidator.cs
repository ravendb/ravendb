using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[DebuggerDisplay("'{_schemaPath}' property validator")]
public class PropertySchemaRuleValidator : ElementSchemaRuleValidator<BlittableJsonReaderObject, string>
{
    public string Accessor { get; }

    // ReSharper disable once ConvertToPrimaryConstructor
    public PropertySchemaRuleValidator(string accessor, string schemaPath) : base($"{schemaPath}.{accessor}")
    {
        Accessor = accessor;
    }
    
    protected override bool TryGetElement(BlittableJsonReaderObject parent, string accessor, out (BlittableJsonToken Type, object Value) element)
    {
        if (parent.TryGetPropertyType(accessor, out var internalToken))
        {
            element.Type = internalToken & BlittableJsonReaderBase.TypesMask;
            element.Value = parent[accessor];
            return true;
        }

        element.Type = 0;
        element.Value = null;
        return false;
    }
}
