using System.Diagnostics;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Array;

[DebuggerDisplay("'{SchemaPath}' array item validator")]
public class ArrayItemSchemaRuleValidator : ElementSchemaRuleValidator<BlittableJsonReaderArray, int>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public ArrayItemSchemaRuleValidator(BlittableJsonToken[] typesRestriction, ISchemaRuleValidator[] ruleValidators, SchemaPath schemaPath) 
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
