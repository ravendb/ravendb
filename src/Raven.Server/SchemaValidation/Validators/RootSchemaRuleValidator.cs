using System.Diagnostics;

namespace Raven.Server.SchemaValidation.Validators;

[DebuggerDisplay("root validator")]
public class RootSchemaRuleValidator : SelfElementSchemaRuleValidator
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public RootSchemaRuleValidator() : base(string.Empty)
    {
    }
}

//TODO Think about how to remove the accessor type here in a clean way.
