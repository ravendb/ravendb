using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

public class SelfElementSchemaRuleValidator : ElementSchemaRuleValidator<BlittableJsonReaderObject, object>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public SelfElementSchemaRuleValidator(string schemaPath) : base(schemaPath)
    {
    }

    protected override bool TryGetElement(BlittableJsonReaderObject parent, object accessor, out (BlittableJsonToken Type, object Value) element)
    {
        element = (BlittableJsonToken.StartObject, parent);
        return true;
    }
}
