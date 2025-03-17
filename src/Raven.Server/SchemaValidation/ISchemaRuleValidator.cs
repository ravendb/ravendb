using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public interface ISchemaRuleValidator
{
    bool Validate(object value, ErrorBuilder errorBuilder);
}

public abstract class SchemaRuleValidator<T> : ISchemaRuleValidator
{
    protected abstract bool ValidateInternal(T value, ErrorBuilder errorBuilder);
    
    public bool Validate(object value, ErrorBuilder errorBuilder)
    {
        return CheckTypeAndGetValue(value, out T tValue) == false || ValidateInternal(tValue, errorBuilder);
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
}
