using System;
using System.Threading;
using Raven.Server.SchemaValidation.Validators;
using Sparrow.Json;
using Sparrow.Threading;

namespace Raven.Server.SchemaValidation;

public class SchemaValidator : IDisposable
{
    private SelfElementSchemaRuleValidator _root;
    //The context is only written during the initialization phase. During validation, it is used for reading only.
    private readonly (IDisposable Return, JsonOperationContext Value) _context;
    private int _activeValidations;
    private SingleUseFlag _disposing = new SingleUseFlag();
    
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
        _disposing.Raise();
            
        while (true)
        {
            var origin = Interlocked.CompareExchange(ref _activeValidations, -1, 0);
            if (origin == -1)
                return;
            if (origin == 0)
                break;
            Thread.Sleep(10);
        }
        
        _context.Return?.Dispose();
    }
}
