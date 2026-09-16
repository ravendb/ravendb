using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Corax.Querying.Matches.Meta
{
    // This class is reflected in the Raven.Client solution for deserialization purposes. Please ensure that any changes made here are also reflected in the Client's code.
    public sealed class QueryInspectionNode(string operation, List<QueryInspectionNode> children = null, Dictionary<string, string> parameters = null) : IDynamicJson
    {
        public readonly string Operation = operation;
        public readonly Dictionary<string, string> Parameters = parameters ?? new Dictionary<string, string>();
        public readonly List<QueryInspectionNode> Children = children ?? new List<QueryInspectionNode>();

        /// <summary>Set by a match that is structurally a per-entry post-filter (spatial / vector): it consumes the
        /// candidate set via AndWith rather than producing a bitmap slot. The plan-graph renderer reads this to place
        /// the node in the post-filter chain, instead of sniffing the operation name. Server-internal — not serialized
        /// to the client.</summary>
        public bool IsPostFilter;

        public DynamicJsonValue ToJson()
        {
            var children = new DynamicJsonArray();
            if (Children != null)
            {
                foreach (QueryInspectionNode child in Children)
                {
                    children.Add(child.ToJson());
                }
            }
            var parameters = new DynamicJsonValue();
            if (Parameters != null)
            {
                foreach (var (k,v) in Parameters)
                {
                    parameters[k] = v;
                }
            }
            return new DynamicJsonValue
            {
                [nameof(Operation)] = Operation,
                [nameof(Children)] = children,
                [nameof(Parameters)] = parameters
            };
        }
        
        public static QueryInspectionNode NotInitializedInspectionNode(string nameOperation) => new($"Not initialized: {nameOperation}");

        public override string ToString()
        {
            return ToString(this, 0);
        }

        public static string ToString(QueryInspectionNode node)
        {
            return ToString(node, 0);
        }

        private static string ToString(QueryInspectionNode node, int indent)
        {
            string indentation = string.Empty;
            for (int i = 0; i < indent; i++)
                indentation += "\t";

            string parameters = string.Empty;
            if (node.Parameters.Count != 0)
            {
                var items = new List<string>();
                foreach (var item in node.Parameters)
                    items.Add($"{item.Key}: {item.Value}");

                parameters = $"{{ {string.Join(", ", items)} }} ";
            }

            string children = string.Empty;
            if (node.Children.Count != 0)
            {
                foreach (var child in node.Children)
                    children += $"{ToString(child, indent + 1)}";
            }

            return $"{indentation}{node.Operation} {parameters}{Environment.NewLine}{children}";
        }
    }
}
