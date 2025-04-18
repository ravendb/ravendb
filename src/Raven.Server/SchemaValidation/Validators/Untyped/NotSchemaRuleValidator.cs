using System.Diagnostics.CodeAnalysis;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Untyped;

public class NotSchemaRuleValidator : SchemaRuleValidator<object>
{
    private readonly ElementSchemaRuleValidator _not;

    // ReSharper disable once ConvertToPrimaryConstructor
    public NotSchemaRuleValidator([NotNull] ElementSchemaRuleValidator not)
    {
        _not = not;
    }

    public override bool Validate(object value, ErrorBuilder errorBuilder)
    {
        if (_not.Validate(value, null) == false)
            return true;
        
        errorBuilder?.AddError($"The value at '{errorBuilder.Path}' is invalid because it matches a `not` schema.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.Not)]
// ReSharper disable once UnusedType.Global
public class NotSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<NotSchemaRuleValidator>
{
    public override NotSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        schemaPath += Rule;
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath, out var not) == false)
            return null;
        
        var notSchemaValidator = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(not, schemaPath, refSchemas);

        return new NotSchemaRuleValidator(notSchemaValidator);
    }
}

