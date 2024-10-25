using Raven.Server.SchemaValidation.Object;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

internal class SchemaValidator
{
    private ObjectSchemaRuleValidator _root;

    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        var root = new RootObjectSchemaRuleValidator();
        root.Init(schemaDefinition);
        _root = root;
    }

    public bool Validate(BlittableJsonReaderObject obj, out string errors)
    {
        var errorBuilder = new ErrorBuilder();
        _root.Validate(obj, errorBuilder);
        errors = errorBuilder.ToString();
        return string.IsNullOrEmpty(errors);
    }
}
