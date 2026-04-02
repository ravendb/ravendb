using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink;

public class CdcSinkEmbeddedTableConfig : IFillFromBlittableJson, IDynamicJson
{
    /// <summary>
    /// SQL schema name.
    /// </summary>
    public string SourceTableSchema { get; set; }

    /// <summary>
    /// SQL table name.
    /// </summary>
    public string SourceTableName { get; set; }

    /// <summary>
    /// Property name in the parent document (e.g., "Lines").
    /// </summary>
    public string PropertyName { get; set; }

    /// <summary>
    /// SQL column name → document property name mapping.
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
    /// Primary key columns of this embedded table.
    /// Used for matching items within arrays/maps during updates and deletes.
    /// </summary>
    public List<string> PrimaryKeyColumns { get; set; } = new();

    /// <summary>
    /// Foreign key columns that join this table to its parent.
    /// </summary>
    public List<string> JoinColumns { get; set; } = new();

    /// <summary>
    /// How the embedded data is stored:
    /// Array = JSON array, Map = JSON object keyed by PK, Value = single object.
    /// </summary>
    public CdcSinkRelationType Type { get; set; }

    /// <summary>
    /// Optional JavaScript patch that runs on the PARENT document after this embedded operation.
    /// Available variables: this = parent document, $row = the embedded row data.
    /// </summary>
    public string Patch { get; set; }

    /// <summary>
    /// Controls how DELETE events are handled for this embedded table.
    /// When null (default), deletes remove the embedded item from the parent's array/map/value.
    /// See <see cref="CdcSinkOnDeleteConfig"/> for archive, audit, and ignore patterns.
    /// </summary>
    public CdcSinkOnDeleteConfig OnDelete { get; set; }

    /// <summary>
    /// Whether primary key matching and map key comparison are case-sensitive.
    /// When false (default), string PK values and map keys are compared using ordinal case-insensitive comparison.
    /// When true, comparison is ordinal case-sensitive.
    /// </summary>
    public bool CaseSensitiveKeys { get; set; }

    /// <summary>
    /// Nested embedded tables (deep nesting).
    /// Requires that the nested table has a denormalized FK to the root table.
    /// </summary>
    public List<CdcSinkEmbeddedTableConfig> EmbeddedTables { get; set; } = new();

    public void FillFromBlittableJson(BlittableJsonReaderObject json)
    {
        var config = DocumentConventions.Default.Serialization.DefaultConverter
            .FromBlittable<CdcSinkEmbeddedTableConfig>(json, "CdcSinkEmbeddedTableConfig");

        SourceTableSchema = config.SourceTableSchema;
        SourceTableName = config.SourceTableName;
        PropertyName = config.PropertyName;
        ColumnsMapping = config.ColumnsMapping;
        AttachmentNameMapping = config.AttachmentNameMapping;
        JsonColumns = config.JsonColumns;
        PrimaryKeyColumns = config.PrimaryKeyColumns;
        JoinColumns = config.JoinColumns;
        Type = config.Type;
        Patch = config.Patch;
        OnDelete = config.OnDelete;
        CaseSensitiveKeys = config.CaseSensitiveKeys;
        EmbeddedTables = config.EmbeddedTables;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(SourceTableSchema)] = SourceTableSchema,
            [nameof(SourceTableName)] = SourceTableName,
            [nameof(PropertyName)] = PropertyName,
            [nameof(ColumnsMapping)] = DynamicJsonValue.Convert(ColumnsMapping),
            [nameof(AttachmentNameMapping)] = DynamicJsonValue.Convert(AttachmentNameMapping),
            [nameof(JsonColumns)] = new DynamicJsonArray(JsonColumns),
            [nameof(PrimaryKeyColumns)] = new DynamicJsonArray(PrimaryKeyColumns),
            [nameof(JoinColumns)] = new DynamicJsonArray(JoinColumns),
            [nameof(Type)] = Type.ToString(),
            [nameof(Patch)] = Patch,
            [nameof(OnDelete)] = OnDelete?.ToJson(),
            [nameof(CaseSensitiveKeys)] = CaseSensitiveKeys,
            [nameof(EmbeddedTables)] = new DynamicJsonArray(EmbeddedTables.Select(x => x.ToJson())),
        };
    }
}
