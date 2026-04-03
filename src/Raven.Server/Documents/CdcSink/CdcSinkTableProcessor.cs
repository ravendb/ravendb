using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations.CdcSink;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Per-table processing context. Allocated once when building the <see cref="CdcSinkDocumentProcessor"/>
/// and reused for all rows of the same table.
/// </summary>
public class CdcSinkTableProcessor
{
    private readonly StringBuilder _sb = new();

    /// <summary>
    /// The key used to look up this processor in the table index (schema.table or just table).
    /// </summary>
    public string Key { get; init; }

    /// <summary>
    /// The root table configuration this processor belongs to.
    /// </summary>
    public CdcSinkTableConfig RootConfig { get; init; }

    /// <summary>
    /// RavenDB collection name for document IDs and metadata.
    /// </summary>
    public string CollectionName { get; init; }

    /// <summary>
    /// True if this processor handles a root table (produces Put/Delete ops).
    /// False if it handles an embedded table (produces EmbeddedModify ops).
    /// </summary>
    public bool IsRoot { get; init; }

    /// <summary>
    /// For embedded tables: the embedded table configuration.
    /// </summary>
    public CdcSinkEmbeddedTableConfig EmbeddedConfig { get; init; }

    /// <summary>
    /// For embedded tables: the path from root to this embedded location.
    /// </summary>
    public List<EmbeddedPathSegment> PathFromRoot { get; init; }

    /// <summary>
    /// For embedded tables: the FK column names in the child that map to the root table's PK columns.
    /// Used to compute the parent document ID.
    /// </summary>
    public List<string> RootJoinColumns { get; init; }

    /// <summary>
    /// The column mappings for this table (root or embedded).
    /// Used by MapColumns, and also consumed by the batch command for PK lookups and attachment handling.
    /// </summary>
    public List<CdcColumnMapping> Columns { get; init; }

    /// <summary>
    /// Pre-filtered list of columns where Type == Attachment. Computed once during construction
    /// to avoid scanning all columns on every row.
    /// </summary>
    public List<CdcColumnMapping> AttachmentColumns { get; init; }

    /// <summary>
    /// Maps SQL column name → mapped property Name for non-attachment columns. Computed once during
    /// construction for property lookups during embedded path navigation.
    /// </summary>
    public Dictionary<string, string> PropertyLookup { get; init; }

    /// <summary>
    /// Pre-computed mapped property names for each primary key column, in the same order as the
    /// config's PrimaryKeyColumns list. Avoids per-row dictionary lookups in MatchesPrimaryKey
    /// and BuildMapKey.
    /// </summary>
    public string[] MappedPrimaryKeyNames { get; init; }

    /// <summary>
    /// Generate a document ID from row data using primary key values.
    /// Format: "{CollectionName}/{pk1}/{pk2}/..."
    /// </summary>
    public string GenerateDocumentId(Dictionary<string, object> rowData, List<string> pkColumns)
    {
        _sb.Clear();
        _sb.Append(CollectionName);

        for (int i = 0; i < pkColumns.Count; i++)
        {
            if (rowData.TryGetValue(pkColumns[i], out var val) == false || val == null)
                return null;

            _sb.Append('/');
            _sb.Append(val);
        }

        return _sb.ToString();
    }

    public string GetParentDocumentId(Dictionary<string, object> rowData)
    {
        if (RootJoinColumns == null || RootJoinColumns.Count == 0)
            throw new InvalidOperationException("Cannot compute parent document ID: no root join columns defined");

        return GenerateDocumentId(rowData, RootJoinColumns);
    }

    public DynamicJsonValue MapColumns(Dictionary<string, object> rowData, List<CdcColumnMapping> columns, JsonOperationContext context)
    {
        var result = new DynamicJsonValue();
        for (int i = 0; i < columns.Count; i++)
        {
            var col = columns[i];
            if (col.Type == CdcColumnType.Attachment)
                continue; // attachments are handled separately by the batch command

            if (rowData.TryGetValue(col.Column, out var value) == false)
                continue;

            bool isJsonColumn = col.Type == CdcColumnType.Json;
            result[col.Name] = NormalizeForJson(value, isJsonColumn, context);
        }
        return result;
    }

    /// <summary>
    /// Ensures a raw column value can be serialized into blittable JSON.
    /// Primitive types are passed through. CLR arrays/lists become DynamicJsonArray.
    /// String values in columns explicitly marked as JSON are parsed into native
    /// BlittableJsonReaderObject/BlittableJsonReaderArray using the provided context.
    /// Complex database-specific types (inet, tsvector, etc.) fall back to ToString().
    /// </summary>
    internal static object NormalizeForJson(object value, bool isJsonColumn, JsonOperationContext context )
    {
        return value switch
        {
            null or DBNull => null,
            byte[] bytes => Convert.ToBase64String(bytes),
            Guid guid => guid.ToString(),
            // Primitive types that ObjectJsonParser handles natively
            bool or int or long or float or double or decimal
                or DateTime or DateOnly or DateTimeOffset => value,
            // JSON columns: parse the string into a blittable object/array using the parent context
            string s when isJsonColumn && context != null => ParseJsonColumnValue(s, context),
            string s => s,
            // CLR arrays / collections (e.g., Npgsql string[], int[]) -> JSON arrays
            Array arr => ConvertArrayToJsonArray(arr),
            IList list => ConvertListToJsonArray(list),
            // Complex types (IPAddress, NpgsqlInet, tsvector, etc.) -> string fallback
            _ => value.ToString()
        };
    }

    /// <summary>
    /// Parse a string value from a column explicitly marked as JSON into a native
    /// value. Handles all JSON value types: objects, arrays, strings, numbers,
    /// booleans, and null.
    /// </summary>
    private static object ParseJsonColumnValue(string s, JsonOperationContext context)
    {
        if (string.IsNullOrEmpty(s))
            return null;

        var trimmed = s.AsSpan().TrimStart();
        if (trimmed.IsEmpty)
            return null;

        var first = trimmed[0];

        switch (first)
        {
            case '{':
                return context.Sync.ReadForMemory(s, "cdc-json-column");
            case '[':
                return context.ParseBufferToArray(s, "cdc-json-column", BlittableJsonDocumentBuilder.UsageMode.None);
            case '"':
            {
                // JSON string — strip the surrounding quotes
                var inner = s.Trim();
                if (inner.Length >= 2 && inner[0] == '"' && inner[inner.Length - 1] == '"')
                    return inner.Substring(1, inner.Length - 2);
                return s;
            }
            case 't' or 'f':
                if (bool.TryParse(s.Trim(), out var boolVal))
                    return boolVal;
                return s;
            case 'n':
                if (trimmed.SequenceEqual("null"))
                    return null;
                return s;
            default:
                // Numeric value
                var numStr = s.Trim();
                if (long.TryParse(numStr, out var longVal))
                    return longVal;
                if (double.TryParse(numStr, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out var doubleVal))
                    return doubleVal;
                return s;
        }
    }

    private static DynamicJsonArray ConvertArrayToJsonArray(Array arr)
    {
        var result = new DynamicJsonArray();
        for (int i = 0; i < arr.Length; i++)
            result.Add(NormalizeForJson(arr.GetValue(i), false, null));
        return result;
    }

    private static DynamicJsonArray ConvertListToJsonArray(IList list)
    {
        var result = new DynamicJsonArray();
        for (int i = 0; i < list.Count; i++)
            result.Add(NormalizeForJson(list[i], false, null));
        return result;
    }

    /// <summary>
    /// Resolves FK columns in the row to document ID references in the target collection.
    /// For example, if the row has customer_id=42 and there's a linked table config pointing
    /// to collection "Customers", this writes "Customer": "Customers/42" into the document.
    /// Only Value (single document ID) links are supported.
    /// </summary>
    /// <summary>
    /// Resolves FK columns in the row to document ID references in the target collection.
    ///
    /// Given a row with FK columns (e.g., customer_id=42) and a linked table config
    /// pointing to collection "Customers", writes the property as a document ID reference:
    ///   Before: { "customer_id": 42, "CompanyName": "Acme" }
    ///   After:  { "customer_id": 42, "CompanyName": "Acme", "Customer": "Customers/42" }
    ///
    /// If any FK column is null, the link property is set to null.
    /// </summary>
    public void ApplyLinks(DynamicJsonValue doc, Dictionary<string, object> rowData)
    {
        if (RootConfig.LinkedTables == null)
            return;

        foreach (var linked in RootConfig.LinkedTables)
        {
            _sb.Clear();
            _sb.Append(linked.LinkedCollectionName);

            bool hasNull = false;
            for (int i = 0; i < linked.JoinColumns.Count; i++)
            {
                if (rowData.TryGetValue(linked.JoinColumns[i], out var v) == false || v == null || v is DBNull)
                {
                    hasNull = true;
                    break;
                }
                _sb.Append('/');
                _sb.Append(v);
            }

            doc[linked.PropertyName] = hasNull ? null : _sb.ToString();
        }
    }
}
