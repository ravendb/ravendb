using System;
using Raven.Server.SchemaValidation.Validators;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public class SchemaValidator : IDisposable
{
    private SelfElementSchemaRuleValidator _root;
    private readonly (IDisposable Return, JsonOperationContext Value) _context;
    
    public SchemaValidator(JsonContextPool contextPool)
    {
        _context.Return = contextPool.AllocateOperationContext(out _context.Value);
    }
    
    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        schemaDefinition = schemaDefinition.Clone(_context.Value);
        _root = ElementSchemaRuleValidatorFactory.CreateSelfElementSchemaRuleValidator(schemaDefinition, new SchemaPath());
    }

    public bool Validate(BlittableJsonReaderObject obj, out string errors)
    {
        using var errorBuilder = new ErrorBuilder();
        var isValid = _root.Validate(obj, string.Empty, errorBuilder);
        errors = errorBuilder.GetErrors();
        return isValid;
    }

    public void Dispose()
    {
        //TODO To make sure no one read
        _context.Return?.Dispose();
    }
}
