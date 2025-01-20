using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[SchemaRule(SchemaValidatorConstants.@if)]
public class IfThenElseSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    //TODO To change RootSchemaRuleValidator name or implement new class
    private readonly SelfElementSchemaRuleValidator _ifSchema;
    private readonly SelfElementSchemaRuleValidator _thenSchema;
    private readonly SelfElementSchemaRuleValidator _elseSchema;

    // ReSharper disable once ConvertToPrimaryConstructor
    public IfThenElseSchemaRuleValidator(SelfElementSchemaRuleValidator ifSchema, SelfElementSchemaRuleValidator thenSchema, SelfElementSchemaRuleValidator elseSchema)
    {
        _ifSchema = ifSchema;
        _thenSchema = thenSchema;
        _elseSchema = elseSchema;
    }

    protected override bool ValidateInternal(BlittableJsonReaderObject value, IErrorBuilder errorBuilder)
    {
        return _ifSchema.Validate(value, null, null) 
            ? _thenSchema.Validate(value, null, errorBuilder) 
            : _elseSchema.Validate(value, null, errorBuilder);
    }
}

// ReSharper disable once UnusedType.Global
public class IfThenElseSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<IfThenElseSchemaRuleValidator>
{
    public override IfThenElseSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath, out var ifSchema) == false)
            return null;

        var ifSchemaValidator = new SelfElementSchemaRuleValidator(schemaPath);
        ifSchemaValidator.Init(ifSchema);

        SelfElementSchemaRuleValidator thenValidator = null;
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, SchemaValidatorConstants.then, schemaPath, out var then))
        {
            thenValidator = new SelfElementSchemaRuleValidator(schemaPath);
            thenValidator.Init(then);
        }

        SelfElementSchemaRuleValidator elseValidator = null;
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, SchemaValidatorConstants.@else, schemaPath, out var @else))
        {
            elseValidator = new SelfElementSchemaRuleValidator(schemaPath);
            elseValidator.Init(@else);
        }
        
        return new IfThenElseSchemaRuleValidator(ifSchemaValidator, thenValidator, @elseValidator);
    }
}
