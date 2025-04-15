using Raven.Server.SchemaValidation.ErrorMessage;

namespace Raven.Server.SchemaValidation;

public interface ISchemaRuleValidator
{
    bool Validate(object value, ErrorBuilder errorBuilder);
}

public abstract class SchemaRuleValidator<T> : ISchemaRuleValidator
{
    public abstract bool Validate(T value, ErrorBuilder errorBuilder);
    
    bool ISchemaRuleValidator.Validate(object value, ErrorBuilder errorBuilder)
    {
        return CheckTypeAndGetValue(value, out T tValue) == false || Validate(tValue, errorBuilder);
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
