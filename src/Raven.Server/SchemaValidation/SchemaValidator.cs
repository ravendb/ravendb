using System;
using System.Linq;
using System.Threading;
using Raven.Server.SchemaValidation.Validators;
using Sparrow.Json;
using Sparrow.Threading;

namespace Raven.Server.SchemaValidation;

public class SchemaValidator : IDisposable
{
    private SelfObjectElementSchemaRuleValidator _root;
    //The context is only written during the initialization phase. During validation, it is used for reading only and can be used in parallel.
    private readonly (IDisposable Return, JsonOperationContext Value) _context;
    private int _activeValidations;
    private readonly SingleUseFlag _disposing = new SingleUseFlag();
    
    public SchemaValidator(JsonContextPool contextPool)
    {
        _context.Return = contextPool.AllocateOperationContext(out _context.Value);
    }
    
    public void Init(BlittableJsonReaderObject schemaDefinition)
    {
        schemaDefinition = schemaDefinition.Clone(_context.Value);
        
        var refSchemas = new RefSchemas();
        refSchemas.Init(schemaDefinition);
        
        _root = ElementSchemaRuleValidatorFactory.CreateSelfElementSchemaRuleValidator(schemaDefinition, new SchemaPath(), refSchemas);
    }

    public bool Validate(BlittableJsonReaderObject obj, out string errors)
    {
        ObjectDisposedException.ThrowIf(_disposing.IsRaised(), nameof(SchemaValidator));
        
        Interlocked.Increment(ref _activeValidations);
        try
        {
            using var errorBuilder = new ErrorBuilder();
            var isValid = _root.Validate(obj, string.Empty, errorBuilder);
            errors = errorBuilder.GetErrors();
            return isValid;
        }
        finally
        {
            Interlocked.Decrement(ref _activeValidations);
        }
    }

    public void Dispose()
    {
        if (_disposing.Raise() == false)
            return;
            
        Interlocked.Decrement(ref _activeValidations);
        while (true)
        {
            if (_activeValidations == -1)
                break;
            Thread.Sleep(10);
        }
        
        _context.Return?.Dispose();
    }
}

public class RefSchema
{
    public (BlittableJsonToken[] typesRestriction, ISchemaRuleValidator[] ruleValidators) Rules { get; set; } 
    public BlittableJsonReaderObject Raw { get; set; }
}
