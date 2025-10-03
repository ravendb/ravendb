using System;
using System.Diagnostics;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Raven.Server.Documents.SchemaValidation.Validators;
using Sparrow.Json;
using Sparrow.Threading;

namespace Raven.Server.Documents.SchemaValidation;

[DebuggerDisplay("{SchemaDefinition}")]
public class SchemaValidator
{
    public readonly bool Disabled;

    private ElementSchemaRuleValidator _root;
    public string SchemaDefinition { get; set; }

    public SchemaValidator(bool disabled = false)
    {
        Disabled = disabled;
    }
    
    // The caller (holder) is responsible for keeping the underlying Blittable / context alive (and disposing it) for as long as this SchemaValidator is used.
    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        var refSchemas = new RefSchemas();
        refSchemas.Init(schemaDefinition);

        _root = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(schemaDefinition, new SchemaPath(), refSchemas);
    }

    public bool Validate(BlittableJsonReaderObject obj, ErrorBuilder errorBuilder)
    {
        return _root.Validate(obj, errorBuilder);
    }
}
