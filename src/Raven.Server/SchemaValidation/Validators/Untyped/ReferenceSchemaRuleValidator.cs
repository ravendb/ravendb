using System.Diagnostics.CodeAnalysis;
using Raven.Server.Exceptions.SchemaValidation;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Untyped;

public class ReferenceSchemaRuleValidator : SchemaRuleValidator<object>
{
    private readonly ElementSchemaRuleValidator _reference;

    // ReSharper disable once ConvertToPrimaryConstructor
    public ReferenceSchemaRuleValidator([NotNull] ElementSchemaRuleValidator reference)
    {
        _reference = reference;
    }

    public override bool Validate(object value, ErrorBuilder errorBuilder) => _reference.Validate(value, errorBuilder);
}

[SchemaRule(SchemaValidatorConstants.Ref)]
// ReSharper disable once UnusedType.Global
public class ReferenceSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<ReferenceSchemaRuleValidator>
{
    public override ReferenceSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        schemaPath += Rule;
        if (SchemaValidationHelper.TryGetString(schemaDefinition, Rule, schemaPath, out var reference) == false)
            return null;
        
        if(refSchemas.TryGet(reference, out var validator) == false)
            throw new InvalidSchemaValidationDefinitionException(
                $"The reference '{reference}' at '{schemaPath.FullPath}' does not match any defined subschema.");
                
        return new ReferenceSchemaRuleValidator(validator);
    }
}

