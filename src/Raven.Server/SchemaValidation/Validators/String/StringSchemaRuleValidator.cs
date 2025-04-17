using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.String;

public abstract class StringSchemaRuleValidator : SchemaRuleValidator<LazyStringValue>
{
    protected string Target = "value";
    
    public void FocusOnPropertyName() => Target = "property name";
    
    protected override bool CheckTypeAndGetValue(object value, out LazyStringValue stringValue)
    {
        switch (value)
        {
            case LazyStringValue lazyStringValue:
                stringValue = lazyStringValue;
                return true;
            case LazyCompressedStringValue lazyCompressedStringValue:
                stringValue = lazyCompressedStringValue.ToLazyStringValue();
                return true;
            default:
                stringValue = null;
                return false;
        }
    }
}
