using System.Collections.Generic;
using System.Linq;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Untyped;

public class EnumSchemaRuleValidator : FixedValueSchemaRuleValidator
{
    private readonly object[] _enums;

    // ReSharper disable once ConvertToPrimaryConstructor
    public EnumSchemaRuleValidator(IEnumerable<object> enums)
    {
        _enums = enums.Select(ConvertTypeForComparison).ToArray();
    }

    public override bool Validate(object value, ErrorBuilder errorBuilder)
    {
        //The order here is extremely important since when comparing between blittable objects the function uses the first object context and _constantValue is used concurrently 
        if (_enums.Any(x => Equals(value, x))) 
            return true;
        
        var quoteIfString = IsString(value) ? "\"" : "";
        errorBuilder?.AddError($"The value '{quoteIfString}{value}{quoteIfString}' at '{errorBuilder.Path}' is not an allowed value. Expected one of: '{_enums:', '}'.");
        return false;
    }
    
    protected override bool CheckTypeAndGetValue(object value, out object tValue)
    {
        tValue = ConvertTypeForComparison(value);
        return true;
    }
}

[SchemaRule(SchemaValidatorConstants.Enum)]
// ReSharper disable once UnusedType.Global
public class EnumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<EnumSchemaRuleValidator>
{
    public override EnumSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        return SchemaValidationHelper.TryGetArray(schemaDefinition, Rule, schemaPath + Rule, out var enums) 
            ? new EnumSchemaRuleValidator(enums)
            : null;
    }
}
