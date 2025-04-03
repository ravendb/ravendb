using System.Diagnostics.CodeAnalysis;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

public class NotSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly SelfObjectElementSchemaRuleValidator _not;

    // ReSharper disable once ConvertToPrimaryConstructor
    public NotSchemaRuleValidator([NotNull] SelfObjectElementSchemaRuleValidator not)
    {
        _not = not;
    }

    public override bool Validate(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
    {
        if (_not.Validate(value, null, null) == false)
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
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath.FullPath, out var not) == false)
            return null;
        
        var notSchemaValidator = ElementSchemaRuleValidatorFactory.CreateSelfElementSchemaRuleValidator(not, schemaPath + Rule, refSchemas);

        return new NotSchemaRuleValidator(notSchemaValidator);
    }
}

