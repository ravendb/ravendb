using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink;

public class CdcSinkLinkedTableConfig : IDynamicJson
{
    /// <summary>
    /// SQL schema name of the linked table.
    /// </summary>
    public string SourceTableSchema { get; set; }

    /// <summary>
    /// SQL table name of the linked table.
    /// </summary>
    public string SourceTableName { get; set; }

    /// <summary>
    /// Property name in the document (e.g., "Customer").
    /// </summary>
    public string PropertyName { get; set; }

    /// <summary>
    /// Foreign key columns used to resolve the link.
    /// </summary>
    public List<string> JoinColumns { get; set; } = new();

    /// <summary>
    /// Array = array of document IDs, Value = single document ID.
    /// Map is not applicable for links.
    /// </summary>
    public CdcSinkRelationType Type { get; set; }

    /// <summary>
    /// Target collection name used for document ID generation
    /// (e.g., "Customers" → generates "Customers/ALFKI").
    /// </summary>
    public string LinkedCollectionName { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(SourceTableSchema)] = SourceTableSchema,
            [nameof(SourceTableName)] = SourceTableName,
            [nameof(PropertyName)] = PropertyName,
            [nameof(JoinColumns)] = new DynamicJsonArray(JoinColumns),
            [nameof(Type)] = Type.ToString(),
            [nameof(LinkedCollectionName)] = LinkedCollectionName,
        };
    }
}
