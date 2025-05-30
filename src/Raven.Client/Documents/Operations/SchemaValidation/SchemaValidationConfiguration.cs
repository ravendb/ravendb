using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.SchemaValidation;

public sealed class SchemaValidationConfiguration
{
    public bool Disabled;

    public Dictionary<string, Validator> ValidatorsByCollection;

    public DynamicJsonValue ToJson()
    {
        DynamicJsonValue validatorsByCollection = null;
        if (ValidatorsByCollection != null)
        {
            validatorsByCollection = new DynamicJsonValue();
            foreach (var validator in ValidatorsByCollection)
            {
                validatorsByCollection[validator.Key] = validator.Value.ToJson();
            }
        }

        return new DynamicJsonValue
        {
            [nameof(Disabled)] = Disabled,
            [nameof(ValidatorsByCollection)] = validatorsByCollection
        };
    }

    public class Validator
    {
        public bool Disabled { get; set; }

        public BlittableJsonReaderObject SchemaDefinition { get; set; }

        public DateTime LastModifiedTime { get; private set; } = DateTime.UtcNow;

        public object ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(SchemaDefinition)] = SchemaDefinition,
                [nameof(LastModifiedTime)] = LastModifiedTime
            };
        }
    }
}
