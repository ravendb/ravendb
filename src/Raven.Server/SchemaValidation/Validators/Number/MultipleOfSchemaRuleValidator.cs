using System;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

public class MultipleOfSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _multipleOf;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MultipleOfSchemaRuleValidator(decimal multipleOf)
    {
        _multipleOf = multipleOf;
    }

    protected override bool ValidateInternal(decimal value, ErrorBuilder errorBuilder)
    {
        if (value % _multipleOf == 0) 
            return true;
        
        errorBuilder?.AddError($"The value '{value}' at '{errorBuilder.Path}' should be a multiple of {_multipleOf}.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.multipleOf)]
public class MultipleOfSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MultipleOfSchemaRuleValidator>
{
    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        return SchemaValidationHelper.TryGetNumber(schemaDefinition, Rule, schemaPath.FullPath, out var multipleOf)
            ? new MultipleOfSchemaRuleValidator(multipleOf) 
            : null;
    }
}
