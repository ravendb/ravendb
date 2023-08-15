using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Sparrow.Json.Parsing;

namespace Corax.Queries
{
    public sealed class QueryInspectionNode : IDynamicJson
    {
        public readonly string Operation;
        public readonly Dictionary<string, string> Parameters;
        public readonly List<QueryInspectionNode> Children;

        public QueryInspectionNode(string operation, List<QueryInspectionNode> children = null, Dictionary<string, string> parameters = null)
        {
            Operation = operation;
            Parameters = parameters ?? new Dictionary<string, string>();            
            Children = children ?? new List<QueryInspectionNode>();
        }

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
