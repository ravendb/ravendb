using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

public class RequiredSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly HashSet<string> _requiredHashSet;

    public RequiredSchemaRuleValidator(BlittableJsonReaderArray required)
    {
        _requiredHashSet = required.Select(x => x.ToString()).ToHashSet();
    }
    
    public RequiredSchemaRuleValidator(string required)
    {
        _requiredHashSet = [required];
    }
    
    protected override bool ValidateInternal(BlittableJsonReaderObject value, IErrorBuilder errorBuilder)
    {
        var isValid = true;
        foreach (var required in _requiredHashSet)
        {
            if(value.Contains(required))
                continue;
            errorBuilder?.AddError($"The required property '{required}' is missing at '{errorBuilder.Path}'.");
            isValid = false;
        }
        return isValid;
    }
}

[SchemaRule(SchemaValidatorConstants.required)]
public class RequiredSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<RequiredSchemaRuleValidator>
{
    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetArray(schemaDefinition, Rule, schemaPath.FullPath, out var required)
            ? new RequiredSchemaRuleValidator(required)
            : null;
    }
}
