using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.SchemaValidation;

public class SchemaDefinition
{
    public bool Disabled { get; set; }

    public string Schema { get; set; }

    // set by the server when the definition is added or changed
    public DateTime LastModifiedTime { get; internal set; }

    // LastModifiedTime only records when the definition changed, so it is not compared
    internal bool IsSameAs(SchemaDefinition other)
    {
        return other != null && Disabled == other.Disabled && string.Equals(Schema, other.Schema, StringComparison.Ordinal);
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Disabled)] = Disabled,
            [nameof(Schema)] = Schema,
            [nameof(LastModifiedTime)] = LastModifiedTime
        };
    }
}
