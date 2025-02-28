using System.Diagnostics.CodeAnalysis;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

public class IfThenElseSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    //TODO To change RootSchemaRuleValidator name or implement new class
    private readonly SelfElementSchemaRuleValidator _ifValidator;
    private readonly SelfElementSchemaRuleValidator _thenValidator;
    private readonly SelfElementSchemaRuleValidator _elseValidator;

    // ReSharper disable once ConvertToPrimaryConstructor
    public IfThenElseSchemaRuleValidator([NotNull] SelfElementSchemaRuleValidator ifValidator, [NotNull] SelfElementSchemaRuleValidator thenValidator, SelfElementSchemaRuleValidator elseValidator = null)
    {
        _ifValidator = ifValidator;
        _thenValidator = thenValidator;
        _elseValidator = elseValidator;
    }

    protected override bool ValidateInternal(BlittableJsonReaderObject value, IErrorBuilder errorBuilder)
    {
        //TODO Maybe to build a dedicated error
        return _ifValidator.Validate(value, null, null) 
            ? _thenValidator.Validate(value, null, errorBuilder) 
            : _elseValidator == null || _elseValidator.Validate(value, null, errorBuilder);
    }
}

[SchemaRule(SchemaValidatorConstants.@if)]
public class IfThenElseSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<IfThenElseSchemaRuleValidator>
{
    public override IfThenElseSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath, out var ifSchema) == false)
            return null;
        var ifSchemaValidator = ElementSchemaRuleValidatorFactory.CreateSelfElementSchemaRuleValidator(ifSchema, schemaPath);

        if(SchemaValidationHelper.TryGetObject(schemaDefinition, SchemaValidatorConstants.then, schemaPath, out var thenSchema) == false)
            return null;
        var thenValidator = ElementSchemaRuleValidatorFactory.CreateSelfElementSchemaRuleValidator(thenSchema, schemaPath);

        var elseValidator = SchemaValidationHelper.TryGetObject(schemaDefinition, SchemaValidatorConstants.@else, schemaPath, out var elseSchema)
            ? ElementSchemaRuleValidatorFactory.CreateSelfElementSchemaRuleValidator(elseSchema, schemaPath)
            : null;
        
        return new IfThenElseSchemaRuleValidator(ifSchemaValidator, thenValidator, elseValidator);
    }
}

