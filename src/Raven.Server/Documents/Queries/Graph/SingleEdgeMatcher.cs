using System;
using System.Collections.Generic;
using Microsoft.Extensions.Primitives;
using Raven.Client.Exceptions;
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


        public SingleEdgeMatcher(SingleEdgeMatcher step, IGraphQueryStep right)
        {
            Right = right;
            QueryParameters = step.QueryParameters;
            IncludedEdges = new Dictionary<string, Sparrow.Json.BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            Results = new List<Match>();
            Edge = step.Edge;
            EdgeAlias = step.EdgeAlias;
        }

        public void SingleMatch(Match left, string alias)
        {
            var dummy = left.GetSingleDocumentResult(alias);
            if (dummy == null)
                return;

            var leftDoc = dummy.Data;

            if (Edge.Where != null || Edge.Project != null)
            {
                if (BlittableJsonTraverser.Default.TryRead(leftDoc, Edge.Path.FieldValue, out var value, out _) == false)
                    return;

                if (Edge.Project == null)
                {
                    ThrowMissingEdgeProjection();
                }

                switch (value)
                {
                    case BlittableJsonReaderArray array:
                        foreach (var item in array)
                        {
                            if (item is BlittableJsonReaderObject json &&
                                Edge.Where?.IsMatchedBy(json, QueryParameters) != false)
                            {
                                AddEdgeAfterFiltering(left, json, Edge.Project?.FieldValue);
                            }
                        }
                        break;
                    case BlittableJsonReaderObject json:
                        if (Edge.Where?.IsMatchedBy(json, QueryParameters) != false)
                        {
                            AddEdgeAfterFiltering(left, json, Edge.Project?.FieldValue);
                        }
                        break;
                }
            }
            else
            {
                AddEdgeAfterFiltering(left, leftDoc, Edge.Path.FieldValue);
            }
        }

        private void ThrowMissingEdgeProjection()
        {
            throw new InvalidQueryException("An expression that selects an edge must have a projection with exactly one field.", Edge.ToString());
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
                if(Right == null)
                {
                    MergeAndAddResult(left, leftDoc, includedEdge.Key, includedEdge.Value, default);
                    continue;
                }
                foreach (var rightMatch in Right.GetById(includedEdge.Key))
                {
                    MergeAndAddResult(left, leftDoc, includedEdge.Key, includedEdge.Value, rightMatch);
                }
            }
        }

        private void MergeAndAddResult(Match left, BlittableJsonReaderObject leftDoc, string includeKey, BlittableJsonReaderObject includedValue, Match rightMatch)
        {
            var clone = new Match(left);
            clone.Merge(rightMatch);

            if (ShouldUseFullObjectForEdge(leftDoc, includedValue))
                clone.Set(EdgeAlias, includedValue);
            else
                clone.Set(EdgeAlias, includeKey);

            Results.Add(clone);
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
