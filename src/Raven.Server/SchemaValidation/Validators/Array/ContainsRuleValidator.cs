using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Array;

public class ContainsRuleValidator : SchemaRuleValidator<BlittableJsonReaderArray>
{
    private readonly ArrayItemSchemaRuleValidator _containsValidator;
    private readonly long _minContains;
    private readonly long _maxContains;

    // ReSharper disable once ConvertToPrimaryConstructor
    public ContainsRuleValidator(ArrayItemSchemaRuleValidator containsValidator, long minContains, long maxContains)
    {
        _containsValidator = containsValidator;
        _minContains = minContains;
        _maxContains = maxContains;
    }

    protected override bool ValidateInternal(BlittableJsonReaderArray value, IErrorBuilder errorBuilder)
    {
        var count = 0;
        for (int j = 0; j < value.Length; j++)
        {
            //TODO Maybe to make sure the validation here return immediately after the first failure.
            if (_containsValidator.Validate(value, j, null) == false) 
                continue;
            
            if (++count >= _minContains && _maxContains > (value.Length - j + count))
                return true;
        }

        var isValid = true;
        if (count > _maxContains)
        {
            errorBuilder?.AddError(
                $"The array at '{errorBuilder.Path}' must not contain more than {_maxContains} items matching the required schema, but {count} matching items were found. schema : {_containsValidator.SchemaDefinition}");
            isValid = false;
        }
        if (count < _minContains)
        {
            errorBuilder?.AddError(
                $"The array at '{errorBuilder.Path}' must contain at least {_minContains} items matching the required schema, but {(count==0?"no items where": $"only {count} matching item{(count>1?"s":"")} were")} found. Schema : {_containsValidator.SchemaDefinition}");
            isValid = false;
        }

        return isValid;
    }
}

[SchemaRule(SchemaValidatorConstants.contains)]
public class ContainsRuleValidatorRuleValidatorFactory : SchemaRuleValidatorFactory<ContainsRuleValidator>
{
    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        if(SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath.FullPath, out var containsSchema) == false)
            return null;

        if (SchemaValidationHelper.TryGetInteger(schemaDefinition, SchemaValidatorConstants.minContains, schemaPath.FullPath, out var minContains) == false)
            minContains = 1L;
        
        if (SchemaValidationHelper.TryGetInteger(schemaDefinition, SchemaValidatorConstants.maxContains, schemaPath.FullPath, out var maxContains) == false)
            maxContains = long.MaxValue;
        
        var containsValidator = ElementSchemaRuleValidatorFactory.CreateArrayItemSchemaRuleValidator(containsSchema, schemaPath + Rule);
        return new ContainsRuleValidator(containsValidator, minContains, maxContains);
    }
}
