using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Untyped;

public class EnumSchemaRuleValidator : FixedValueSchemaRuleValidator
{
    private readonly object[] _enums;
    private readonly string _forError;

    // ReSharper disable once ConvertToPrimaryConstructor
    public EnumSchemaRuleValidator(IEnumerable<object> enums)
    {
        _enums = enums.Select(v => ConvertTypeForComparison(v, cloneAsRoot: true)).ToArray();
        _forError = string.Join(", ", _enums.Select(v => IsString(v) ? $"'\"{v}\"'" : $"'{v}'"));
    }

    public override bool Validate(SchemaValidationContext context, object value)
    {
        //The order here is mandatory since the validator 'value' is used concurrently and we CloneForConcurrentRead in case it is blittable 
        if (_enums.Any(x => SafeConcurrentEquals(context.OperationContext, schemaValue: x, documentValue: value))) 
            return true;
        
        var quoteIfString = IsString(value) ? "\"" : "";
        context.ErrorBuilder?.AddError($"The value '{quoteIfString}{value}{quoteIfString}' at '{context.ErrorBuilder.Path}' is not an allowed value. Expected one of: {_forError}.");
        return false;
    }
    
    protected override bool CheckTypeAndGetValue(object value, out object tValue)
    {
        tValue = ConvertTypeForComparison(value, cloneAsRoot: false);
        return true;
    }
}

[SchemaRule(SchemaValidatorConstants.Enum)]
// ReSharper disable once UnusedType.Global
public class EnumSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<EnumSchemaRuleValidator>
{
    public override EnumSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetArray(schemaDefinition, Rule, schemaPath + Rule, out var enums) 
            ? new EnumSchemaRuleValidator(enums)
            : null;
    }
}
