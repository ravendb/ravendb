using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[DebuggerDisplay("'{SchemaPath}' property validator")]
public class PropertySchemaRuleValidator : ElementSchemaRuleValidator<BlittableJsonReaderObject, string>
{
    public string Property => SchemaPath.Property;

    // ReSharper disable once ConvertToPrimaryConstructor
    public PropertySchemaRuleValidator(BlittableJsonToken[] typesRestriction, ISchemaRuleValidator[] ruleValidators, SchemaPath schemaPath) 
        : base(typesRestriction, ruleValidators, schemaPath){
    }
    
    protected override bool TryGetElement(BlittableJsonReaderObject parent, string accessor, out (BlittableJsonToken Type, object Value) element)
    {
        if (parent.TryGetPropertyType(accessor, out var internalToken))
        {
            element.Type = internalToken & BlittableJsonReaderBase.TypesMask;
            element.Value = parent[accessor];
            return true;
        }

        element.Type = 0;
        element.Value = null;
        return false;
    }
}
