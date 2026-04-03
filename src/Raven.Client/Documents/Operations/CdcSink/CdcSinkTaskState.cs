using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink;

/// <summary>
/// State document for a CDC Sink task, stored in the @cdc-states collection.
/// Tracks the last processed LSN and per-table initial load progress.
/// </summary>
public class CdcSinkTaskState : IDynamicJson
{
    /// <summary>
    /// Collection name for state documents.
    /// </summary>
    public const string CollectionName = "@cdc-states";

    /// <summary>
    /// The last successfully processed Log Sequence Number (LSN) from the CDC stream.
    /// Used to resume streaming after a restart.
    /// </summary>
    public string LastLsn { get; set; }

    /// <summary>
    /// Per-table initial load state. Key is "schema.tableName".
    /// </summary>
    public Dictionary<string, CdcSinkTableLoadState> Tables { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// The name of the CDC Sink configuration this state belongs to.
    /// </summary>
    public string ConfigurationName { get; set; }

    public DynamicJsonValue ToJson()
    {
        var tables = new DynamicJsonValue();
        foreach (var kvp in Tables)
        {
            tables[kvp.Key] = kvp.Value.ToJson();
        }

        return new DynamicJsonValue
        {
            [nameof(ConfigurationName)] = ConfigurationName,
            [nameof(LastLsn)] = LastLsn,
            [nameof(Tables)] = tables,
        };
    }

    /// <summary>
    /// Generates the document ID for the state document.
    /// Configuration names are compared case-insensitively, but the document ID
    /// preserves the original casing.
    /// </summary>
    public static string GetDocumentId(string configurationName)
    {
        return $"{CollectionName}/{configurationName}";
    }
}

/// <summary>
/// Per-table load state within a CDC Sink task state document.
/// </summary>
public class CdcSinkTableLoadState : IDynamicJson
{
    /// <summary>
    /// Whether the initial full-table load has completed for this table.
    /// </summary>
    public bool InitialLoadCompleted { get; set; }

    /// <summary>
    /// The last primary key values loaded during the initial load.
    /// Used to resume an interrupted initial load.
    /// Format: list of string representations of the PK column values (in PK column order).
    /// </summary>
    public List<string> LastKeyValues { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(InitialLoadCompleted)] = InitialLoadCompleted,
            [nameof(LastKeyValues)] = LastKeyValues != null ? new DynamicJsonArray(LastKeyValues) : null,
        };
    }
}
