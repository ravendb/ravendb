using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.SchemaValidation;

public class SchemaValidator
{
    public bool Disabled { get; set; }

    public string SchemaDefinition { get; set; }

    public DateTime LastModifiedTime { get; } = DateTime.UtcNow;

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Disabled)] = Disabled,
            [nameof(SchemaDefinition)] = SchemaDefinition,
            [nameof(LastModifiedTime)] = LastModifiedTime
        };
    }
}
