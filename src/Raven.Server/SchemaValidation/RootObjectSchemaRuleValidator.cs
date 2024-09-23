using System.Text;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

internal class RootObjectSchemaRuleValidator() : ObjectSchemaRuleValidator("root", "root", true)
{
    public override void Validate(BlittableJsonReaderObject current, StringBuilder errorBuilder)
    {
        foreach (var validator in RuleValidators)
        {
            validator.Validate(current, errorBuilder);
        }
    }
}
