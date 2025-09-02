using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.SchemaValidation;

public sealed class SchemaValidationConfiguration
{
    private Dictionary<string, SchemaDefinition> _validatorsPerCollection;
    
    public bool Disabled { get; set; }

    public Dictionary<string, SchemaDefinition> ValidatorsPerCollection
    {
        get => _validatorsPerCollection;
        set => _validatorsPerCollection = new Dictionary<string, SchemaDefinition>(value, StringComparer.OrdinalIgnoreCase);
    }

    public DynamicJsonValue ToJson()
    {
        DynamicJsonValue validatorsPerCollection = null;
        if (ValidatorsPerCollection != null)
        {
            validatorsPerCollection = new DynamicJsonValue();
            foreach (var validator in ValidatorsPerCollection)
            {
                validatorsPerCollection[validator.Key] = validator.Value.ToJson();
            }
        }

        return new DynamicJsonValue
        {
            [nameof(Disabled)] = Disabled,
            [nameof(ValidatorsPerCollection)] = validatorsPerCollection
        };
    }
}
