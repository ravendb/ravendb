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

    /// <summary>Source table schema (e.g. "public", "dbo"). Empty string when not specified.</summary>
    public string Schema { get; init; }

    /// <summary>Source table name.</summary>
    public string Table { get; init; }

    /// <summary>Pre-computed Key + "__on_delete" for the OnDelete dispatch path.</summary>
    public string KeyOnDelete { get; init; }

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
    /// All SQL column names for this table, in positional order matching the source.
    /// Set once by the provider before initial load or streaming begins.
    ///
    /// Order guarantee: this array's indices correspond 1:1 with the object[] values
    /// in each CdcSinkRow/CdcSinkDocumentOp. The provider sets this from the same
    /// source it uses to decode rows:
    ///   - Postgres: RelationMessage.Columns (positional)
    ///   - SQL Server: cdc.captured_columns ORDER BY column_ordinal
    ///   - MySQL: INFORMATION_SCHEMA.COLUMNS ORDER BY ORDINAL_POSITION
    ///   - Initial load: SELECT * returns columns in table-definition order (same as above)
    ///
    /// All pre-computed index arrays (PrimaryKeyIndices, ColumnMappingIndices, etc.)
    /// are derived from this array and recomputed when it is set.
    /// </summary>
    public string[] SourceColumnNames { get; private set; }

    /// <summary>Pre-computed indices into SourceColumnNames for primary key columns.</summary>
    public int[] PrimaryKeyIndices { get; private set; }

    /// <summary>Pre-computed indices into SourceColumnNames for each non-attachment CdcColumnMapping.</summary>
    public int[] ColumnMappingIndices { get; private set; }

    /// <summary>Pre-computed indices into SourceColumnNames for attachment columns.</summary>
    public int[] AttachmentColumnIndices { get; private set; }

    /// <summary>For embedded tables: indices of root join FK columns in SourceColumnNames.</summary>
    public int[] RootJoinIndices { get; private set; }

    /// <summary>Per linked table: indices of FK columns in SourceColumnNames.</summary>
    public int[][] LinkedTableJoinIndices { get; private set; }

    /// <summary>
    /// Linked table configurations for this processor (root or embedded).
    /// </summary>
    public List<CdcSinkLinkedTableConfig> LinkedTables { get; init; }

    private readonly Queue<object[]> _valuesPool = new();

    public object[] RentValues()
    {
        return _valuesPool.TryDequeue(out var arr) ? arr : new object[SourceColumnNames.Length];
    }

    public void ReturnValues(object[] arr)
    {
        _valuesPool.Enqueue(arr);
    }

    public void ClearPool()
    {
        foreach (var arr in _valuesPool)
        {
            // we want to return the arrays, but we need to clear
            // the contents to release references and allow them to be GC'd
            Array.Clear(arr, 0, arr.Length);
        }
    }

    public void SetSourceColumnNames(string[] names)
    {
        SourceColumnNames = names;

        // Compute PrimaryKeyIndices
        var pkColumns = IsRoot ? RootConfig.PrimaryKeyColumns : EmbeddedConfig.PrimaryKeyColumns;
        PrimaryKeyIndices = new int[pkColumns.Count];
        for (int i = 0; i < pkColumns.Count; i++)
            PrimaryKeyIndices[i] = FindColumnIndex(names, pkColumns[i]);

        // Compute ColumnMappingIndices (one per non-attachment column)
        ColumnMappingIndices = new int[Columns.Count - AttachmentColumns.Count];
        int mapIdx = 0;
        for (int i = 0; i < Columns.Count; i++)
        {
            if (Columns[i].Type != CdcColumnType.Attachment)
                ColumnMappingIndices[mapIdx++] = FindColumnIndex(names, Columns[i].Column);
        }

        // Compute AttachmentColumnIndices
        AttachmentColumnIndices = new int[AttachmentColumns.Count];
        for (int i = 0; i < AttachmentColumns.Count; i++)
            AttachmentColumnIndices[i] = FindColumnIndex(names, AttachmentColumns[i].Column);

        // Compute RootJoinIndices (for embedded tables)
        if (RootJoinColumns != null)
        {
            RootJoinIndices = new int[RootJoinColumns.Count];
            for (int i = 0; i < RootJoinColumns.Count; i++)
                RootJoinIndices[i] = FindColumnIndex(names, RootJoinColumns[i]);
        }

        // Compute LinkedTableJoinIndices
        if (LinkedTables is { Count: > 0 })
        {
            LinkedTableJoinIndices = new int[LinkedTables.Count][];
            for (int lt = 0; lt < LinkedTables.Count; lt++)
            {
                var linked = LinkedTables[lt];
                LinkedTableJoinIndices[lt] = new int[linked.JoinColumns.Count];
                for (int j = 0; j < linked.JoinColumns.Count; j++)
                    LinkedTableJoinIndices[lt][j] = FindColumnIndex(names, linked.JoinColumns[j]);
            }
        }
    }

    internal static int FindColumnIndex(string[] names, string columnName)
    {
        for (int i = 0; i < names.Length; i++)
        {
            if (string.Equals(names[i], columnName, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        throw new InvalidOperationException($"Column '{columnName}' not found in source columns: [{string.Join(", ", names)}]");
    }

    /// <summary>
    /// Generate a document ID from positional row values using pre-computed PrimaryKeyIndices.
    /// Format: "{CollectionName}/{pk1}/{pk2}/..."
    /// </summary>
    public string GenerateDocumentId(object[] values) => BuildId(PrimaryKeyIndices, values);

    public string GetParentDocumentId(object[] values) => BuildId(RootJoinIndices, values);

    private string BuildId(int[] indices, object[] values)
    {
        _sb.Clear();
        _sb.Append(CollectionName);

        for (int i = 0; i < indices.Length; i++)
        {
            _sb.Append('/');
            _sb.Append(values[indices[i]] ?? "null");
        }

        return _sb.ToString();
    }

    public DynamicJsonValue MapColumns(object[] values, JsonOperationContext context)
    {
        var result = new DynamicJsonValue();
        var columns = Columns;
        for (int i = 0, mapIdx = 0; i < columns.Count; i++)
        {
            var col = columns[i];
            if (col.Type == CdcColumnType.Attachment)
                continue; // attachments are handled separately by the batch command

            var value = values[ColumnMappingIndices[mapIdx++]];
            result[col.Name] = NormalizeForJson(value, col.Type == CdcColumnType.Json, context);
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
                // JSON string — use System.Text.Json to properly unescape JSON escape sequences
                try
                {
                    return System.Text.Json.JsonSerializer.Deserialize<string>(s);
                }
                catch
                {
                    return s;
                }
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
    ///
    /// Given a row with FK columns (e.g., customer_id=42) and a linked table config
    /// pointing to collection "Customers", writes the property as a document ID reference:
    ///   Before: { "customer_id": 42, "CompanyName": "Acme" }
    ///   After:  { "customer_id": 42, "CompanyName": "Acme", "Customer": "Customers/42" }
    ///
    /// If all FK columns are null, the link property is set to null (no relationship).
    /// If only some are null (compound FK referencing a row with a null PK component),
    /// the ID is built as-is with "null" segments.
    /// </summary>
    public void ApplyLinks(DynamicJsonValue doc, object[] values)
    {
        if (LinkedTables is not { Count: > 0 })
            return;

        for (int lt = 0; lt < LinkedTables.Count; lt++)
        {
            var linked = LinkedTables[lt];
            var joinIndices = LinkedTableJoinIndices[lt];
            _sb.Clear();
            _sb.Append(linked.LinkedCollectionName);

            bool allNull = true;
            for (int i = 0; i < joinIndices.Length; i++)
            {
                var v = values[joinIndices[i]];
                if (v is not null and not DBNull)
                    allNull = false;
                _sb.Append('/');
                _sb.Append(v ?? "null");
            }

            doc[linked.PropertyName] = allNull ? null : _sb.ToString();
        }
    }
}
