using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink;

/// <summary>
/// PostgreSQL-specific settings for a CDC Sink task.
/// These are optional on creation — if omitted, auto-generated hash-based names are used.
/// Once set (either by the user or auto-filled), these values are immutable.
/// </summary>
public class CdcSinkPostgresSettings : IDynamicJson
{
    /// <summary>
    /// The PostgreSQL publication name used for logical replication.
    /// If null on creation, auto-filled with a deterministic hash-based name.
    /// Must be a valid PostgreSQL identifier (alphanumeric + underscore, max 63 chars).
    /// </summary>
    public string PublicationName { get; set; }

    /// <summary>
    /// The PostgreSQL logical replication slot name.
    /// If null on creation, auto-filled with a deterministic hash-based name.
    /// Must be a valid PostgreSQL identifier (alphanumeric + underscore, max 63 chars).
    /// </summary>
    public string SlotName { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(PublicationName)] = PublicationName,
            [nameof(SlotName)] = SlotName,
        };
    }
}
