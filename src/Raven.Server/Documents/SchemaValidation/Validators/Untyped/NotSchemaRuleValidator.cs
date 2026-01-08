using System.Diagnostics.CodeAnalysis;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Untyped;

public class NotSchemaRuleValidator : SchemaRuleValidator<object>
{
    private readonly ElementSchemaRuleValidator _not;

    // ReSharper disable once ConvertToPrimaryConstructor
    public NotSchemaRuleValidator([NotNull] ElementSchemaRuleValidator not)
    {
        _not = not;
    }

    public override bool Validate(SchemaValidationContext context, object value)
    {
        using (context.WithoutCollectingErrors())
        {
            if (_not.Validate(context, value) == false)
                return true;
        }
        
        context.ErrorBuilder?.AddError($"The value at '{context.ErrorBuilder.Path}' is invalid because it matches a `not` schema.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.Not)]
// ReSharper disable once UnusedType.Global
public class NotSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<NotSchemaRuleValidator>
{
    public override NotSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        schemaPath += Rule;
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath, out var not) == false)
            return null;
        
        var notSchemaValidator = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(context, not, schemaPath);

        return new NotSchemaRuleValidator(notSchemaValidator);
    }
}

