using System;
using System.Collections.Generic;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public struct SingleEdgeMatcher
    {
        public BlittableJsonReaderObject QueryParameters;
        public List<Match> Results;
        public IGraphQueryStep Right;
        public Dictionary<string, BlittableJsonReaderObject> IncludedEdges;
        public WithEdgesExpression Edge;
        public StringSegment EdgeAlias;

        public void SingleMatch(Match left, string alias)
        {
            var dummy = left.GetSingleDocumentResult(alias);
            if (dummy == null)
                throw new InvalidOperationException("Unable to find alias " + alias + " in query results, this is probably a bug");

            var leftDoc = dummy.Data;

            if (Edge.Where != null || Edge.Project != null)
            {
                if (BlittableJsonTraverser.Default.TryRead(leftDoc, Edge.Path.FieldValue, out var value, out _) == false)
                    return;

                switch (value)
                {
                    case BlittableJsonReaderArray array:
                        foreach (var item in array)
                        {
                            if (item is BlittableJsonReaderObject json &&
                                Edge.Where?.IsMatchedBy(json, QueryParameters) != false)
                            {
                                AddEdgeAfterFiltering(left, json, Edge.Project.FieldValue);
                            }
                        }
                        break;
                    case BlittableJsonReaderObject json:
                        if (Edge.Where?.IsMatchedBy(json, QueryParameters) != false)
                        {
                            AddEdgeAfterFiltering(left, json, Edge.Project.FieldValue);
                        }
                        break;
                }
            }
            else
            {
                AddEdgeAfterFiltering(left, leftDoc, Edge.Path.FieldValue);
            }
        }


        private static unsafe bool ShouldUseFullObjectForEdge(BlittableJsonReaderObject src, BlittableJsonReaderObject json)
        {
            return json != null && (json != src || src.HasParent);
        }


        private void AddEdgeAfterFiltering(Match left, BlittableJsonReaderObject leftDoc, StringSegment path)
        {
            var edgeIncludeOp = new EdgeIncludeOp(IncludedEdges);
            IncludedEdges.Clear();
            IncludeUtil.GetDocIdFromInclude(leftDoc,
                 path,
                 edgeIncludeOp);

            if (IncludedEdges.Count == 0)
                return;

            foreach (var includedEdge in IncludedEdges)
            {
                Match rightMatch = default;
                if (Right != null &&
                    Right.TryGetById(includedEdge.Key, out rightMatch) == false)
                    continue;

                var clone = new Match(left);
                clone.Merge(rightMatch);

                if (ShouldUseFullObjectForEdge(leftDoc, includedEdge.Value))
                    clone.Set(EdgeAlias, includedEdge.Value);
                else
                    clone.Set(EdgeAlias, includedEdge.Key);

                Results.Add(clone);
            }
        }


        private struct EdgeIncludeOp : IncludeUtil.IIncludeOp
        {
            private readonly Dictionary<string, BlittableJsonReaderObject> _edges;

            public EdgeIncludeOp(Dictionary<string, BlittableJsonReaderObject> edges)
            {
                _edges = edges;
            }


            public void Include(BlittableJsonReaderObject parent, string id)
            {
                if (id == null)
                    return;

                _edges[id] = parent;
            }
        }
    }
}
