using System.Diagnostics.CodeAnalysis;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Untyped;

public class IfThenElseSchemaRuleValidator : SchemaRuleValidator<object>
{
    private readonly ElementSchemaRuleValidator _ifValidator;
    private readonly ElementSchemaRuleValidator _thenValidator;
    private readonly ElementSchemaRuleValidator _elseValidator;

    // ReSharper disable once ConvertToPrimaryConstructor
    public IfThenElseSchemaRuleValidator([NotNull] ElementSchemaRuleValidator ifValidator, [NotNull] ElementSchemaRuleValidator thenValidator, ElementSchemaRuleValidator elseValidator = null)
    {
        _ifValidator = ifValidator;
        _thenValidator = thenValidator;
        _elseValidator = elseValidator;
    }

    public override bool Validate(object value, ErrorBuilder errorBuilder)
    {
        return _ifValidator.Validate(value, null) 
            ? _thenValidator.Validate(value, errorBuilder) 
            : _elseValidator == null || _elseValidator.Validate(value, errorBuilder);
    }
}

[SchemaRule(SchemaValidatorConstants.If)]
// ReSharper disable once UnusedType.Global
public class IfThenElseSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<IfThenElseSchemaRuleValidator>
{
    public override IfThenElseSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        var ifPath = schemaPath + Rule;
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, ifPath, out var ifSchema) == false)
            return null;
        
        var ifSchemaValidator = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(ifSchema, ifPath, refSchemas);

        var thenPath = schemaPath + SchemaValidatorConstants.Then;
        var thenValidator = SchemaValidationHelper.TryGetObject(schemaDefinition, SchemaValidatorConstants.Then, thenPath, out var thenSchema)
            ? ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(thenSchema, thenPath, refSchemas)
            : null;

        var elsePath = schemaPath + SchemaValidatorConstants.Else;
        var elseValidator = SchemaValidationHelper.TryGetObject(schemaDefinition, SchemaValidatorConstants.Else, elsePath, out var elseSchema)
            ? ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(elseSchema, elsePath, refSchemas)
            : null;

        if (thenValidator == null && elseValidator == null)
            return null;
        
        return new IfThenElseSchemaRuleValidator(ifSchemaValidator, thenValidator, elseValidator);
    }
}

