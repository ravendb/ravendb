using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

public class SelfElementSchemaRuleValidator : ElementSchemaRuleValidator<BlittableJsonReaderObject, object>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public SelfElementSchemaRuleValidator(BlittableJsonToken[] typesRestriction, ISchemaRuleValidator[] ruleValidators, string schemaPath) 
        : base(typesRestriction, ruleValidators, schemaPath)
    {
    }

    protected override bool TryGetElement(BlittableJsonReaderObject parent, object accessor, out (BlittableJsonToken Type, object Value) element)
    {
        element = (BlittableJsonToken.StartObject, parent);
        return true;
    }
}
