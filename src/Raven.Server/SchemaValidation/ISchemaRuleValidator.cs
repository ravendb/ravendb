using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public interface ISchemaRuleValidator
{
    void Validate(object value, SchemaValidatorPath path, IErrorBuilder errorBuilder);
}

public abstract class SchemaRuleValidator<T> : ISchemaRuleValidator
{
    protected abstract void ValidateInternal(T value, SchemaValidatorPath path, IErrorBuilder errorBuilder);
    
    public void Validate(object value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if (CheckTypeAndGetValue(value, out T tValue) == false) 
            return;
        ValidateInternal(tValue, path, errorBuilder);
    }

    protected virtual bool CheckTypeAndGetValue(object value, out T tValue)
    {
        if (value is T internalTValue == false)
        {
            tValue = default;
            return false;
        }
        tValue = internalTValue;
        return true;
    }
    
    //TODO Consider defining base class with EnumSchemaRuleValidator
    protected static object ConvertTypeForComparison(object x)
    {
        return x switch
        {
            LazyNumberValue lnx => (decimal)lnx,
            LazyStringValue or LazyCompressedStringValue => x.ToString(),
            //TODO To differentiate between string object to object
            BlittableJsonReaderObject or BlittableJsonReaderArray => x.ToString(),
            long lx => (decimal)lx,
            _ => throw new InvalidOperationException($"The type {x.GetType()} is not supported.")
        };
    }
}
