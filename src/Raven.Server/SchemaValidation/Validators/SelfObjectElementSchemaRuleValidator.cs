using System.Diagnostics;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

[DebuggerDisplay("'{SchemaPath}' validator")]
public class SelfObjectElementSchemaRuleValidator : ElementSchemaRuleValidator<BlittableJsonReaderObject, object>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public SelfObjectElementSchemaRuleValidator(BlittableJsonToken[] typesRestriction, ISchemaRuleValidator[] ruleValidators, SchemaPath schemaPath) 
        : base(typesRestriction, ruleValidators, schemaPath)
    {
    }

    protected override bool TryGetElement(BlittableJsonReaderObject parent, object accessor, out (BlittableJsonToken Type, object Value) element)
    {
        element = (BlittableJsonToken.StartObject, parent);
        return true;
    }
}
