using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

public class ConstantSchemaRuleValidator : SchemaRuleValidator<object>
{
    private readonly object _constantValue;

    // ReSharper disable once ConvertToPrimaryConstructor
    public ConstantSchemaRuleValidator(object constantValue)
    {
        _constantValue = ConvertTypeForComparison(constantValue);
    }

    protected override bool ValidateInternal(object value, ErrorBuilder errorBuilder)
    {
        if (_constantValue.Equals(value)) 
            return true;
        
        //TODO Clear error to differentiate between number and string (15 or "15")
        errorBuilder?.AddError($"The value at '{errorBuilder.Path}' must be '{_constantValue}', but it is '{value}'.");
        return false;
    }

    protected override bool CheckTypeAndGetValue(object value, out object tValue)
    {
        tValue = ConvertTypeForComparison(value);
        return true;
    }
}

[SchemaRule(SchemaValidatorConstants.@const)]
public class ConstantSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<ConstantSchemaRuleValidator>
{
    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return schemaDefinition.TryGet(Rule, out object multipleOf)
            ? new ConstantSchemaRuleValidator(multipleOf) 
            : null;
    }
}
