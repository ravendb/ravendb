using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[DebuggerDisplay("root validator")]
public class RootSchemaRuleValidator : PropertySchemaRuleValidator
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public RootSchemaRuleValidator() : base(string.Empty, string.Empty)
    {
    }

    public override void Validate(BlittableJsonReaderObject parent, string property, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        CheckAllValidators(parent, path, errorBuilder);
    }
}
