using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

internal class StringSchemaRuleValidator : PropertySchemaRuleValidator<string>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public StringSchemaRuleValidator(string path, string property, bool isRequired) : base(path, property, isRequired)
    {
    }

    protected override bool IsOfRequiredType(BlittableJsonToken token) => 
        (token & BlittableJsonReaderBase.TypesMask) is BlittableJsonToken.String or BlittableJsonToken.CompressedString;

    public override void ReadSchemaDefinition(BlittableJsonReaderObject propertySchemaDefinition)
    {
        var rulesValidators = new List<ISchemaRuleValidator<string>>();
        ReadValueSchemaRuleValidators(propertySchemaDefinition, Path, rulesValidators);
        RuleValidators = rulesValidators.ToArray();
    }

    protected override bool TryGetValue(BlittableJsonReaderObject parent, out string value)
    {
        if (parent.TryGetWithoutThrowingOnError(Property, out value) == false)
        {
            Debug.Assert(false, "We already confirmed the property exists and it is a number");
            return false;
        }

        return true;
    }
}
