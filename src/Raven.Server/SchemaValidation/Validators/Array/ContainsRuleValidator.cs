using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Array;

[SchemaRule(SchemaValidatorConstants.contains)]
public class ContainsRuleValidator : SchemaRuleValidator<BlittableJsonReaderArray>
{
    private readonly ArrayItemSchemaRuleValidator _containsValidator;

    // ReSharper disable once ConvertToPrimaryConstructor
    public ContainsRuleValidator(ArrayItemSchemaRuleValidator containsValidator)
    {
        _containsValidator = containsValidator;
    }
    
    protected override bool ValidateInternal(BlittableJsonReaderArray value, IErrorBuilder errorBuilder)
    {
        for (int j = 0; j < value.Length; j++)
        {
            //TODO Maybe to make sure the validation here return immediately after the first failure.
            if (_containsValidator.Validate(value, j, null))
                return true;
        }
        errorBuilder?.AddError($"The array at '{errorBuilder.Path}' must contain at least one item that matches the required schema, but no such item was found. Schema : {_containsValidator.SchemaDefinition}");
        return false;
    }
}

// ReSharper disable once UnusedType.Global
public class ContainsRuleValidatorRuleValidatorFactory : SchemaRuleValidatorFactory<ContainsRuleValidator>
{
    public override ContainsRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath, out var containsSchema) == false)
            return null;

        var containsValidator = new ArrayItemSchemaRuleValidator(schemaPath);
        containsValidator.Init(containsSchema);
        return new ContainsRuleValidator(containsValidator);
    }
}
