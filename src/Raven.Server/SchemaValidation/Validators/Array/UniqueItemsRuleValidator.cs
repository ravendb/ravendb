using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Array;

public class UniqueItemsRuleValidator : SchemaRuleValidator<BlittableJsonReaderArray>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    protected override bool ValidateInternal(BlittableJsonReaderArray value, IErrorBuilder errorBuilder)
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
        
        errorBuilder?.AddError($"The array at '{errorBuilder.Path}' contains duplicate value{(duplicates.Count == 1?"":'s')}: '{string.Join("', '", duplicates)}'. Each item must be unique.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.uniqueItems)]
public class UniqueItemsRuleValidatorFactory : SchemaRuleValidatorFactory<UniqueItemsRuleValidator>
{
    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        if (SchemaValidationHelper.TryGetBoolean(schemaDefinition, Rule, schemaPath.FullPath, out bool uniqueItems) == false) 
            return null;

        return uniqueItems ? new UniqueItemsRuleValidator() : null;
    }
}
