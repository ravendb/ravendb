using System.Diagnostics.CodeAnalysis;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

public class IfThenElseSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly SelfObjectElementSchemaRuleValidator _ifValidator;
    private readonly SelfObjectElementSchemaRuleValidator _thenValidator;
    private readonly SelfObjectElementSchemaRuleValidator _elseValidator;

    // ReSharper disable once ConvertToPrimaryConstructor
    public IfThenElseSchemaRuleValidator([NotNull] SelfObjectElementSchemaRuleValidator ifValidator, [NotNull] SelfObjectElementSchemaRuleValidator thenValidator, SelfObjectElementSchemaRuleValidator elseValidator = null)
    {
        _ifValidator = ifValidator;
        _thenValidator = thenValidator;
        _elseValidator = elseValidator;
    }

    public override bool Validate(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
    {
        //TODO Maybe to build a dedicated error
        return _ifValidator.Validate(value, null, null) 
            ? _thenValidator.Validate(value, null, errorBuilder) 
            : _elseValidator == null || _elseValidator.Validate(value, null, errorBuilder);
    }
}

[SchemaRule(SchemaValidatorConstants.If)]
// ReSharper disable once UnusedType.Global
public class IfThenElseSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<IfThenElseSchemaRuleValidator>
{
    public override IfThenElseSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath.FullPath, out var ifSchema) == false)
            return null;
        
        var ifSchemaValidator = ElementSchemaRuleValidatorFactory.CreateSelfElementSchemaRuleValidator(ifSchema, schemaPath + Rule, refSchemas);

        var thenValidator = SchemaValidationHelper.TryGetObject(schemaDefinition, SchemaValidatorConstants.Then, schemaPath.FullPath, out var thenSchema) == false
            ? ElementSchemaRuleValidatorFactory.CreateSelfElementSchemaRuleValidator(thenSchema, schemaPath + SchemaValidatorConstants.Then, refSchemas)
            : null;

        var elseValidator = SchemaValidationHelper.TryGetObject(schemaDefinition, SchemaValidatorConstants.Else, schemaPath.FullPath, out var elseSchema)
            ? ElementSchemaRuleValidatorFactory.CreateSelfElementSchemaRuleValidator(elseSchema, schemaPath + SchemaValidatorConstants.Else, refSchemas)
            : null;

        if (thenValidator == null && elseValidator == null)
            return null;
        
        return new IfThenElseSchemaRuleValidator(ifSchemaValidator, thenValidator, elseValidator);
    }
}

