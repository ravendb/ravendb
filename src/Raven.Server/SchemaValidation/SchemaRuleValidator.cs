namespace Raven.Server.SchemaValidation;

internal interface SchemaRuleValidator
{
    void Validate(object value, IErrorBuilder errorBuilder);
}

public abstract class SchemaRuleValidator<T> : SchemaRuleValidator
{
    protected string Path { get; }

    // ReSharper disable once ConvertToPrimaryConstructor
    protected SchemaRuleValidator(string path) 
    {
        Path = path;   
    }

    protected abstract void ValidateInternal(T value, IErrorBuilder errorBuilder);
    
    public void Validate(object value, IErrorBuilder errorBuilder)
    {
        if (CheckTypeAndGetValue(value, out T tValue) == false) 
            return;
        ValidateInternal(tValue, errorBuilder);
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
