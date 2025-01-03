using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Array;

[SchemaRule("uniqueItems")]
public class UniqueItemsRuleValidator : SchemaRuleValidator<BlittableJsonReaderArray>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    protected override void ValidateInternal(BlittableJsonReaderArray value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        var duplicates = new HashSet<object>();
        var hashSet = new HashSet<object>();
        foreach (var item in value)
        {
            if (hashSet.Add(item) == false)
                duplicates.Add(item);
        }
        
        if(duplicates.Any())
            errorBuilder.AddError($"The array at '{path}' contains duplicate values: {string.Join(", ", duplicates)}. Each item must be unique.");
    }
}

// ReSharper disable once UnusedType.Global
public class UniqueItemsRuleValidatorFactory : SchemaRuleValidatorFactory<UniqueItemsRuleValidator>
{
    public override UniqueItemsRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if(TryGetPropertyType(schemaDefinition, Rule, out var type) == false)
            return null;

        const BlittableJsonToken expectedType = BlittableJsonToken.Boolean;
        if (type != expectedType)
            TrowRuleTypeError(Rule, schemaDefinition[Rule], expectedType, type, schemaPath);

        if (schemaDefinition.TryGet(Rule, out bool uniqueItems) == false)
            throw new InvalidOperationException($"'{Rule}' must to be convertable to ${nameof(Boolean)} here. Should not happen");

        return uniqueItems ? new UniqueItemsRuleValidator() : null;
    }
}
