using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

[DebuggerDisplay("'{_schemaPath}' array item validator")]
public class ArrayItemSchemaRuleValidator : ElementSchemaRuleValidator<BlittableJsonReaderArray, int>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public ArrayItemSchemaRuleValidator(int index, string schemaPath) : base($"{schemaPath}[{index}]")
    {
    }
    
    public ArrayItemSchemaRuleValidator(string schemaPath) : base($"{schemaPath}[*]")
    {
    }
    
    protected override bool TryGetElement(BlittableJsonReaderArray parent, int accessor, out (BlittableJsonToken Type, object Value) element)
    {
        var result = parent.GetValueTokenTupleByIndex(accessor);
        element.Type = result.Item2 & BlittableJsonReaderBase.TypesMask;
        element.Value = result.Item1;
        return true;
    }
}
