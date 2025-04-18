using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Raven.Server.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Untyped;

public class AllOfSchemaRuleValidator : SchemaRuleValidator<object>
{
    private readonly ElementSchemaRuleValidator[] _validators;

    // ReSharper disable once ConvertToPrimaryConstructor
    public AllOfSchemaRuleValidator([NotNull] ElementSchemaRuleValidator[] validators)
    {
        _validators = validators;
    }

    public override bool Validate(object value, ErrorBuilder errorBuilder)
    {
        var validate = true;
        foreach (var validator in _validators)
        {
            validate &= validator.Validate(value, errorBuilder);
            if(errorBuilder == null && validate == false)
               return false;
        }
        return validate;
    }
}

[SchemaRule(SchemaValidatorConstants.AllOf)]
// ReSharper disable once UnusedType.Global
public class AllOfSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<AllOfSchemaRuleValidator>
{
    public override AllOfSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        schemaPath += Rule;
        if (SchemaValidationHelper.TryGetArray(schemaDefinition, Rule, schemaPath, out var validatorsSchema) == false)
            return null;

        List<ElementSchemaRuleValidator> validators = null;
        for (int i = 0; i < validatorsSchema.Length; i++)
        {
            var itemPath = schemaPath + i;
            var itemSchema = SchemaValidationHelper.CheckTypeAndThrow<BlittableJsonReaderObject>(validatorsSchema[i], itemPath);
            (validators ??= []).Add(ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(itemSchema, itemPath, refSchemas));
        }
        if(validators == null)
            return null;
        
        return new AllOfSchemaRuleValidator(validators.ToArray());
    }
}

