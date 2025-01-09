using System;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Number;

[SchemaRule(SchemaValidatorConstants.multipleOf)]
public class MultipleOfSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _multipleOf;

    // ReSharper disable once ConvertToPrimaryConstructor
    public MultipleOfSchemaRuleValidator(decimal multipleOf)
    {
        _multipleOf = multipleOf;
    }

    protected override void ValidateInternal(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(value % _multipleOf != 0)
            errorBuilder.AddError($"The value '{value}' at '{path}' should be a multiple of {_multipleOf}.");
    }
}

// ReSharper disable once UnusedType.Global
public class MultipleOfSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<MultipleOfSchemaRuleValidator>
{
    public override MultipleOfSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        return SchemaValidationHelper.TryGetNumber(schemaDefinition, Rule, schemaPath, out var multipleOf)
            ? new MultipleOfSchemaRuleValidator(multipleOf) 
            : null;
    }
}
