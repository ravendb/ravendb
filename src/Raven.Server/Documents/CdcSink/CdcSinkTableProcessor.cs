using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Operations.CdcSink;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Per-table processing context. Allocated once when building the <see cref="CdcSinkDocumentProcessor"/>
/// and reused for all rows of the same table.
/// </summary>
public class CdcSinkTableProcessor
{
    private readonly StringBuilder _sb = new();

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

    public DynamicJsonValue MapColumns(Dictionary<string, object> rowData, Dictionary<string, string> columnsMapping)
    {
        var result = new DynamicJsonValue();
        foreach (var mapping in columnsMapping)
        {
            if (rowData.TryGetValue(mapping.Key, out var value))
            {
                result[mapping.Value] = value switch
                {
                    null or DBNull => null,
                    byte[] bytes => Convert.ToBase64String(bytes),
                    Guid guid => guid.ToString(),
                    _ => value
                };
            }
        }
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
            if (linked.Type != CdcSinkRelationType.Value)
                continue;

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
