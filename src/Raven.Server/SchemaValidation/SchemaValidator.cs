using Raven.Server.SchemaValidation.Object;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public class SchemaValidator
{
    private ObjectSchemaRuleValidator _root;

    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        var root = new ObjectSchemaRuleValidator();
        root.Init(schemaDefinition);
        _root = root;
    }

    public bool Validate(BlittableJsonReaderObject obj, out string errors)
    {
        var errorBuilder = new ErrorBuilder();
        _root.Validate(obj, new SchemaValidatorPath(), errorBuilder);
        return errorBuilder.TryGetErrors(out errors) == false;
    }
}
