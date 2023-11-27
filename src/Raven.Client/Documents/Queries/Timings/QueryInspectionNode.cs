using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Queries.Timings;

public sealed class QueryInspectionNode : IDynamicJson
{
    public string Operation { get; set; }
    public Dictionary<string, string> Parameters { get; set; }
    public List<QueryInspectionNode> Children { get; set; }

    public QueryInspectionNode()
    {
        //Deserialization
        // This class mirrors the structure of the 'Corax/[...]/QueryInspectionNode.cs for deserialization. Ensure changes here are also reflected in Corax's code.
    }

    public QueryInspectionNode Clone()
    {
        var cloned = new QueryInspectionNode() {Operation = Operation};

        if (Parameters != null)
        {
            cloned.Parameters = new();
            foreach (var item in Parameters)
                cloned.Parameters.Add(item.Key, item.Value);
        }

        if (Children != null)
        {
            cloned.Children = new();
            foreach (var children in Children)
                cloned.Children.Add(children.Clone());
        }

        return cloned;
    }
    
    public DynamicJsonValue ToJson()
    {
        throw new NotSupportedException($"'{nameof(QueryInspectionNode)}' should not be used for serialization.");
    }
}
