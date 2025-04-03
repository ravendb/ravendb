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
    
    public override bool Validate(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
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

[SchemaRule(SchemaValidatorConstants.Required)]
// ReSharper disable once UnusedType.Global
public class RequiredSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<RequiredSchemaRuleValidator>
{
    public override RequiredSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        return SchemaValidationHelper.TryGetArray(schemaDefinition, Rule, schemaPath.FullPath, out var required)
            ? new RequiredSchemaRuleValidator(required)
            : null;
    }
}
