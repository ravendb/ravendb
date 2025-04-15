using System.Collections.Generic;
using System.Linq;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

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
        if (_enums.Any(x => Equals(x, value))) 
            return true;
        
        errorBuilder?.AddError($"The value '{WrapStringWithQuotationMarks(value)}' at '{errorBuilder.Path}' is not an allowed value. Expected one of: '{_enums:', '}'.");
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
        return SchemaValidationHelper.TryGetArray(schemaDefinition, Rule, schemaPath.FullPath, out var enums) 
            ? new EnumSchemaRuleValidator(enums)
            : null;
    }
}
