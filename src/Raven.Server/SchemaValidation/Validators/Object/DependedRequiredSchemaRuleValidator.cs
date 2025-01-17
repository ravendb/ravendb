using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[SchemaRule(SchemaValidatorConstants.dependentRequired)]
public class DependentRequiredSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly Dictionary<string, LazyStringValue[]> _dependentRequired;

    // ReSharper disable once ConvertToPrimaryConstructor
    public DependentRequiredSchemaRuleValidator(Dictionary<string, LazyStringValue[]> dependentRequired)
    {
        _dependentRequired = dependentRequired;
    }

    protected override bool ValidateInternal(BlittableJsonReaderObject value, IErrorBuilder errorBuilder)
    {
        var isValid = true;

        List<LazyStringValue> missingProperties = null;
        foreach (var (prop, required) in _dependentRequired)
        {
            missingProperties?.Clear();
            if (value.Contains(prop) == false)
                continue;

            if (value.Contains(prop) == false)
                continue;

            foreach (var requiredProp in required)
            {
                if (value.Contains(requiredProp))
                    continue;

                if (errorBuilder == null)
                    return false;

                //TODO To reduce allocation
                (missingProperties ??= []).Add(requiredProp);
                isValid = false;
            }

            if (missingProperties == null || missingProperties.Count == 0)
                continue;

            if (missingProperties.Count == 1)
            {
                errorBuilder.AddError(
                    $"The object at '{errorBuilder.Path}' is missing property '{missingProperties[0]}' which is required when property '{prop}' is present.");
                continue;
            }

            errorBuilder.AddError(
                $"The object at '{errorBuilder.Path}' is missing properties '{string.Join("', '", missingProperties.Take(missingProperties.Count - 1))}' & '{missingProperties[^1]}' which are required when property '{prop}' is present.");
        }

        return isValid;
    }
}

// ReSharper disable once UnusedType.Global
public class DependentRequiredSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<DependentRequiredSchemaRuleValidator>
{
    public override DependentRequiredSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, string schemaPath)
    {
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath, out var dependentRequiredSchema) == false)
            return null;

        var dependentRequired = new Dictionary<string, LazyStringValue[]>();
        foreach (var propertyName in dependentRequiredSchema.GetPropertyNames())
        {
            var list = new List<LazyStringValue>();
            SchemaValidationHelper.TryGetArray(dependentRequiredSchema, propertyName, schemaPath, out var required);
            for (var i = 0; i < required.Length; i++)
            {
                var (value, token) = required.GetValueTokenTupleByIndex(i);
                if (token == BlittableJsonToken.String)
                {
                    list.Add((LazyStringValue)value);
                }
                else if (token == BlittableJsonToken.CompressedString)
                {
                    list.Add(((LazyCompressedStringValue)value).ToLazyStringValue());
                }
                else
                {
                    SchemaValidationHelper.ThrowRuleTypeError(Rule, value, token, schemaPath, i);
                }
            }
            dependentRequired[propertyName] = list.ToArray();
        }
        
        return new DependentRequiredSchemaRuleValidator(dependentRequired);
    }
}
