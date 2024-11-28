namespace Raven.Server.SchemaValidation;

public interface SchemaRuleValidator
{
    void Validate(object value, SchemaValidatorPath path, IErrorBuilder errorBuilder);
}

public abstract class SchemaRuleValidator<T> : SchemaRuleValidator
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
}
