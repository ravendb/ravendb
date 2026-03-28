using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.CdcSink;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Per-table processing context. Allocated once when building the <see cref="CdcSinkDocumentProcessor"/>
/// and reused for all rows of the same table. Avoids per-row allocations for table metadata.
/// </summary>
public class CdcSinkTableProcessor
{
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
    /// Generate a document ID from row data, using primary key values.
    /// Format: "{CollectionName}/{pk1}/{pk2}"
    /// </summary>
    public string GenerateDocumentId(Dictionary<string, object> rowData, List<string> pkColumns)
    {
        var pkValues = new object[pkColumns.Count];
        for (int i = 0; i < pkColumns.Count; i++)
        {
            if (rowData.TryGetValue(pkColumns[i], out var val) == false || val == null)
                return null;
            pkValues[i] = val;
        }

        return CollectionName + "/" + string.Join("/", pkValues);
    }

    /// <summary>
    /// For embedded tables: compute the parent document ID from the row's FK values.
    /// </summary>
    public string GetParentDocumentId(Dictionary<string, object> rowData)
    {
        if (RootJoinColumns == null || RootJoinColumns.Count == 0)
            throw new InvalidOperationException("Cannot compute parent document ID: no root join columns defined");

        return GenerateDocumentId(rowData, RootJoinColumns);
    }

    /// <summary>
    /// Apply column mapping to the raw row data, producing a DynamicJsonValue with renamed properties.
    /// </summary>
    public DynamicJsonValue MapColumns(Dictionary<string, object> rowData, Dictionary<string, string> columnsMapping)
    {
        var result = new DynamicJsonValue();
        foreach (var mapping in columnsMapping)
        {
            if (rowData.TryGetValue(mapping.Key, out var value))
            {
                result[mapping.Value] = ConvertValue(value);
            }
        }
        return result;
    }

    /// <summary>
    /// Apply linked table references to the document.
    /// </summary>
    public void ApplyLinks(DynamicJsonValue doc, Dictionary<string, object> rowData)
    {
        if (RootConfig.LinkedTables == null)
            return;

        foreach (var linked in RootConfig.LinkedTables)
        {
            var fkValues = linked.JoinColumns
                .Select(col => rowData.TryGetValue(col, out var v) ? v : null)
                .ToArray();

            if (fkValues.Any(v => v == null))
            {
                doc[linked.PropertyName] = null;
                continue;
            }

            var linkedId = linked.LinkedCollectionName + "/" + string.Join("/", fkValues);

            if (linked.Type == CdcSinkRelationType.Value)
            {
                doc[linked.PropertyName] = linkedId;
            }
            // Array links would require querying the source DB, which we don't do in CDC.
            // For now, only Value links are supported.
        }
    }

    private static object ConvertValue(object value)
    {
        if (value == null || value is DBNull)
            return null;

        return value switch
        {
            byte[] bytes => System.Convert.ToBase64String(bytes),
            Guid guid => guid.ToString(),
            _ => value
        };
    }
}
