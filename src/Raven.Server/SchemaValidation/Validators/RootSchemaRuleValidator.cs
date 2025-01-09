using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

[DebuggerDisplay("root validator")]
public class RootSchemaRuleValidator : PropertySchemaRuleValidator
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public RootSchemaRuleValidator() : base(string.Empty, string.Empty)
    {
    }

    protected override bool TryGetElement(BlittableJsonReaderObject parent, string accessor, out (BlittableJsonToken Type, object Value) element)
    {
        element = (BlittableJsonToken.StartObject, parent);
        return true;
    }
}
