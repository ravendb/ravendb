using System.Diagnostics.CodeAnalysis;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Untyped;

public class ReferenceSchemaRuleValidator : SchemaRuleValidator<object>
{
    private readonly ElementSchemaRuleValidator _reference;

    // ReSharper disable once ConvertToPrimaryConstructor
    public ReferenceSchemaRuleValidator([NotNull] ElementSchemaRuleValidator reference)
    {
        _reference = reference;
    }

    public override bool Validate(SchemaValidationContext context, object value) => _reference.Validate(context, value);
}

[SchemaRule(SchemaValidatorConstants.Ref)]
// ReSharper disable once UnusedType.Global
public class ReferenceSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<ReferenceSchemaRuleValidator>
{
    public override ReferenceSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        schemaPath += Rule;
        if (SchemaValidationHelper.TryGetString(schemaDefinition, Rule, schemaPath, out var reference) == false)
            return null;

        if (context.RefSchemas.TryGet(reference, out var validator) == false)
            throw new InvalidSchemaValidationDefinitionException(
                $"The reference '{reference}' at '{schemaPath.FullPath}' does not match any defined subschema.");

        return new ReferenceSchemaRuleValidator(validator);
    }
}

