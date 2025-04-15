using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

public class ConstantSchemaRuleValidator : FixedValueSchemaRuleValidator
{
    private readonly object _constantValue;

    // ReSharper disable once ConvertToPrimaryConstructor
    public ConstantSchemaRuleValidator(object constantValue)
    {
        _constantValue = ConvertTypeForComparison(constantValue);
    }

    public override bool Validate(object value, ErrorBuilder errorBuilder)
    {
        if (Equals(_constantValue, ConvertTypeForComparison(value))) 
            return true;
        
        errorBuilder?.AddError($"The value at '{errorBuilder.Path}' must be '{WrapStringWithQuotationMarks(_constantValue)}', but it is '{WrapStringWithQuotationMarks(value)}'.");
        return false;
    }

    protected override bool CheckTypeAndGetValue(object value, out object tValue)
    {
        tValue = ConvertTypeForComparison(value);
        return true;
    }
}

[SchemaRule(SchemaValidatorConstants.Const)]
// ReSharper disable once UnusedType.Global
public class ConstantSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<ConstantSchemaRuleValidator>
{
    public override ConstantSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        return schemaDefinition.TryGet(Rule, out object multipleOf)
            ? new ConstantSchemaRuleValidator(multipleOf) 
            : null;
    }
}
