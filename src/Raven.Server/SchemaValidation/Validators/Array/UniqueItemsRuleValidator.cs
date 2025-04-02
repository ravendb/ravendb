using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Array;

public class UniqueItemsRuleValidator : SchemaRuleValidator<BlittableJsonReaderArray>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    protected override bool ValidateInternal(BlittableJsonReaderArray value, ErrorBuilder errorBuilder)
    {
        HashSet<object> duplicates = null;
        var hashSet = new HashSet<object>();
        foreach (var item in value)
        {
            if (hashSet.Add(item) == false)
                (duplicates ??= []).Add(item);
        }

        if (duplicates == null)
            return true;
        
        errorBuilder?.AddError($"The array at '{errorBuilder.Path}' contains duplicate value{(duplicates.Count == 1?"":"s")}: '{duplicates: \"', '\"}'. Each item must be unique.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.UniqueItems)]
// ReSharper disable once UnusedType.Global
public class UniqueItemsRuleValidatorFactory : SchemaRuleValidatorFactory<UniqueItemsRuleValidator>
{
    public override UniqueItemsRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        if (SchemaValidationHelper.TryGetBoolean(schemaDefinition, Rule, schemaPath.FullPath, out bool uniqueItems) == false) 
            return null;

        return uniqueItems ? new UniqueItemsRuleValidator() : null;
    }
}
