using System.Diagnostics.CodeAnalysis;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

public class NotSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly SelfElementSchemaRuleValidator _not;

    // ReSharper disable once ConvertToPrimaryConstructor
    public NotSchemaRuleValidator([NotNull] SelfElementSchemaRuleValidator not)
    {
        _not = not;
    }

    protected override bool ValidateInternal(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
    {
        if (_not.Validate(value, null, null) == false)
            return true;
        
        errorBuilder?.AddError($"The value at '{errorBuilder.Path}' is invalid because it matches a `not` schema.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.not)]
public class NotSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<NotSchemaRuleValidator>
{
    public override NotSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath.FullPath, out var not) == false)
            return null;
        
        var notSchemaValidator = ElementSchemaRuleValidatorFactory.CreateSelfElementSchemaRuleValidator(not, schemaPath + Rule);

        return new NotSchemaRuleValidator(notSchemaValidator);
    }
}

