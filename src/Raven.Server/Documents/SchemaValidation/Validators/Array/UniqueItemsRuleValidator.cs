using System.Collections.Generic;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Array;

public class UniqueItemsRuleValidator : SchemaRuleValidator<BlittableJsonReaderArray>
{
    // ReSharper disable once ConvertToPrimaryConstructor
    public override bool Validate(SchemaValidationContext context, BlittableJsonReaderArray value)
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
        
        context.ErrorBuilder?.AddError($"The array at '{context.ErrorBuilder.Path}' contains duplicate value{(duplicates.Count == 1?"":"s")}: '{duplicates: \"', '\"}'. Each item must be unique.");
        return false;
    }
}

[SchemaRule(SchemaValidatorConstants.UniqueItems)]
// ReSharper disable once UnusedType.Global
public class UniqueItemsRuleValidatorFactory : SchemaRuleValidatorFactory<UniqueItemsRuleValidator>
{
    public override UniqueItemsRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        if (SchemaValidationHelper.TryGetBoolean(schemaDefinition, Rule, schemaPath + Rule, out bool uniqueItems) == false) 
            return null;

        return uniqueItems ? new UniqueItemsRuleValidator() : null;
    }
}
