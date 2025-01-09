using Raven.Server.SchemaValidation.Validators;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public class SchemaValidator
{
    private PropertySchemaRuleValidator _root;

    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        var root = new RootSchemaRuleValidator();
        root.Init(schemaDefinition);
        _root = root;
    }

    public bool Validate(BlittableJsonReaderObject obj, out string errors)
    {
        var errorBuilder = new ErrorBuilder();
        var isValid = _root.Validate(obj, string.Empty, errorBuilder);
        errors = errorBuilder.GetErrors();
        return isValid;
    }
}
