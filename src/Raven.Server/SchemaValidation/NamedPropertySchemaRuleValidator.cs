using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

[DebuggerDisplay("'{_property}' property validator" )]
public class NamedPropertySchemaRuleValidator : PropertySchemaRuleValidator
{
    private readonly string _property;

    // ReSharper disable once ConvertToPrimaryConstructor
    public NamedPropertySchemaRuleValidator(string property, bool isRequired) : base(isRequired)
    {
        _property = property;
    }

    public override void Validate(BlittableJsonReaderObject parent, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        path.StepIn(_property);
        Validate(parent, _property, path, errorBuilder);
        path.StepOut();
    }
}
