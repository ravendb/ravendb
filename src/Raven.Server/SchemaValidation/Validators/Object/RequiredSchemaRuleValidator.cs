using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[SchemaRule("required")]
public class RequiredSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly HashSet<string> _requiredHashSet;

    public RequiredSchemaRuleValidator(BlittableJsonReaderArray required)
    {
        _requiredHashSet = required.Select(x => x.ToString()).ToHashSet();
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

// ReSharper disable once UnusedType.Global
public class RequiredSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<RequiredSchemaRuleValidator>
{
    public override RequiredSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        return SchemaValidationHelper.TryGetArray(schemaDefinition, Rule, schemaPath, out var required)
            ? new RequiredSchemaRuleValidator(required)
            : null;
    }
}
