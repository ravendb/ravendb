using Sparrow.Json;

namespace Raven.Server.SchemaValidation.String;

public abstract class StringSchemaRuleValidator : SchemaRuleValidator<string>
{
    protected override bool CheckTypeAndGetValue(object value, out string stringValue)
    {
        if (value is LazyStringValue or LazyCompressedStringValue)
        {
            stringValue = value.ToString();
            return true;
        }
       
        stringValue = null;
        return false;
    }
}
