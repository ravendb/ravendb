using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink;

public class CdcSinkTableConfig : IDynamicJson
{
    /// <summary>
    /// RavenDB collection name (e.g., "Orders").
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// SQL schema name (e.g., "dbo", "public").
    /// </summary>
    public string SourceTableSchema { get; set; }

    /// <summary>
    /// SQL table name (e.g., "orders").
    /// </summary>
    public string SourceTableName { get; set; }

    /// <summary>
    /// SQL column name → document property name mapping.
    /// Only mapped columns appear in the document.
    /// Unmapped columns are still available in $row for patches.
    /// </summary>
    public Dictionary<string, string> ColumnsMapping { get; set; } = new();

    /// <summary>
    /// SQL binary column name → attachment name mapping.
    /// </summary>
    public Dictionary<string, string> AttachmentNameMapping { get; set; } = new();

    /// <summary>
    /// SQL column names whose values are JSON (json/jsonb) and should be parsed as
    /// structured objects/arrays in the document rather than stored as plain strings.
    /// </summary>
    public List<string> JsonColumns { get; set; } = new();

    /// <summary>
    /// Primary key column names, used for document ID generation.
    /// </summary>
    public List<string> PrimaryKeyColumns { get; set; } = new();

    /// <summary>
    /// Optional JavaScript transformation patch.
    /// Runs on the document after column mapping and embedded operations.
    /// Available variables: this = document, $row = raw CDC row (all columns).
    /// </summary>
    public string Patch { get; set; }

    /// <summary>
    /// Controls how DELETE events are handled for this table.
    /// When null (default), deletes are processed normally (document is deleted).
    /// See <see cref="CdcSinkOnDeleteConfig"/> for archive, audit, and ignore patterns.
    /// </summary>
    public CdcSinkOnDeleteConfig OnDelete { get; set; }

    public bool Disabled { get; set; }

    /// <summary>
    /// Tables embedded as nested objects/arrays within this collection's documents.
    /// </summary>
    public List<CdcSinkEmbeddedTableConfig> EmbeddedTables { get; set; } = new();

    /// <summary>
    /// Tables referenced by document ID link within this collection's documents.
    /// </summary>
    public List<CdcSinkLinkedTableConfig> LinkedTables { get; set; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(SourceTableSchema)] = SourceTableSchema,
            [nameof(SourceTableName)] = SourceTableName,
            [nameof(ColumnsMapping)] = DynamicJsonValue.Convert(ColumnsMapping),
            [nameof(AttachmentNameMapping)] = DynamicJsonValue.Convert(AttachmentNameMapping),
            [nameof(JsonColumns)] = new DynamicJsonArray(JsonColumns),
            [nameof(PrimaryKeyColumns)] = new DynamicJsonArray(PrimaryKeyColumns),
            [nameof(Patch)] = Patch,
            [nameof(OnDelete)] = OnDelete?.ToJson(),
            [nameof(Disabled)] = Disabled,
            [nameof(EmbeddedTables)] = new DynamicJsonArray(EmbeddedTables.Select(x => x.ToJson())),
            [nameof(LinkedTables)] = new DynamicJsonArray(LinkedTables.Select(x => x.ToJson())),
        };
    }
}
