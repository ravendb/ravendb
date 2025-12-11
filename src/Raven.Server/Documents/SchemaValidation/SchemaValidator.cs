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
    //The context is only written during the initialization phase. During validation, it is used for reading only and can be used in parallel.
    private readonly SingleUseFlag _disposing = new SingleUseFlag();
    
    public string SchemaDefinition { get; set; }

    public SchemaValidator(bool disabled = false)
    {
        Disabled = disabled;
    }
    
    // TODO: add a comment about the blittable lifetime
    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        var refSchemas = new RefSchemas();
        refSchemas.Init(schemaDefinition);

        _root = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(schemaDefinition, new SchemaPath(), refSchemas);
    }

    public bool Validate(BlittableJsonReaderObject obj, ErrorBuilder errorBuilder)
    {
        ObjectDisposedException.ThrowIf(_disposing.IsRaised(), nameof(SchemaValidator));

        return _root.Validate(obj, errorBuilder);
    }
}
