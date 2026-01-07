using Raven.Server.Documents.SchemaValidation.ErrorMessage;

namespace Raven.Server.Documents.SchemaValidation;

public interface ISchemaRuleValidator
{
    bool Validate(SchemaValidationContext context, object value);
}

public abstract class SchemaRuleValidator<T> : ISchemaRuleValidator
{
    public abstract bool Validate(SchemaValidationContext context, T value);
    
    bool ISchemaRuleValidator.Validate(SchemaValidationContext context, object value)
    {
        return CheckTypeAndGetValue(value, out T tValue) == false || Validate(context, tValue);
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
