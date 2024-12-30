using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

public class RequiredSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    public const string RuleName = "required";
    
    private readonly HashSet<string> _requiredHashSet;

    public RequiredSchemaRuleValidator(BlittableJsonReaderArray required)
    {
        _requiredHashSet = required.Select(x => x.ToString()).ToHashSet();
    }
    protected override void ValidateInternal(BlittableJsonReaderObject value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        foreach (var required in _requiredHashSet)
        {
            if(value.Contains(required))
                continue;
            errorBuilder.AddError($"The required property '{required}' is missing at '{path}'.");
        }
    }
}

public class RequiredSchemaRuleValidatorFactory : SchemaRuleValidatorFactory
{
    protected override string Rule => RequiredSchemaRuleValidator.RuleName;

    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        const BlittableJsonToken expectedType = BlittableJsonToken.StartArray;
        if (type != expectedType)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], expectedType, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out BlittableJsonReaderArray required) == false)
            throw new InvalidOperationException($"'{Rule}' must to be convertable to decimal here. Should not happen");
        
        return new RequiredSchemaRuleValidator(required);
    }
}
