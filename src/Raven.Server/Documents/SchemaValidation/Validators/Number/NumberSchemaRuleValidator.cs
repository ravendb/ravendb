using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Number;

public abstract class NumberSchemaRuleValidator : SchemaRuleValidator<decimal>
{
    protected override bool CheckTypeAndGetValue(object value, out decimal tValue)
    {
        if (value is double d)
        {
            tValue = (decimal)d;
            return true;
        }
        if (value is long l)
        {
            tValue = l;
            return true;
        }
        if (value is LazyNumberValue n)
        {
            tValue = (decimal)n;
            return true;
        }
        
        return base.CheckTypeAndGetValue(value, out tValue);
    }
}
