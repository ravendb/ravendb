using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Array;

[DebuggerDisplay("'{_schemaPath}' array item validator")]
public class ArrayItemSchemaRuleValidator : ElementSchemaRuleValidator<BlittableJsonReaderArray, int>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public ArrayItemSchemaRuleValidator(BlittableJsonToken[] typesRestriction, ISchemaRuleValidator[] ruleValidators, string schemaPath) 
        : base(typesRestriction, ruleValidators, schemaPath)
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
