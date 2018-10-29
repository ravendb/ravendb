using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Json;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class EdgeQueryStep : IQueryStep
    {
        private List<Match> _results = new List<Match>();
        private int _index;

        private HashSet<string> _aliases;

        public EdgeQueryStep(IQueryStep left, IQueryStep right, WithEdgesExpression edgesExpression, MatchPath edgePath, BlittableJsonReaderObject queryParameters)
        {
            _left = left;
            _right = right;

            _aliases = new HashSet<string>();

            _aliases.UnionWith(_left.GetAllAliases());
            _aliases.UnionWith(_right.GetAllAliases());
            _aliases.Add(edgePath.Alias);

            _edgePath = edgePath;
            _queryParameters = queryParameters;
            _edgesExpression = edgesExpression;
        }

        public ValueTask Initialize()
        {
            var leftTask = _left.Initialize();
            if(leftTask.IsCompleted)
            {
                var rightTask = _right.Initialize();
                if(rightTask.IsCompleted)
                {
                    CompleteInitialization();
                    return default;
                }
                return InitializeRightAsync(rightTask);
            }

            return InitializeLeftAsync(leftTask);
        }

        private async ValueTask InitializeRightAsync(ValueTask rightTask)
        {
            await rightTask;
            CompleteInitialization();
        }

        private async ValueTask InitializeLeftAsync(ValueTask leftTask)
        {
            await leftTask;
            await _right.Initialize();
            CompleteInitialization();
        }

        private static unsafe bool ShouldUseFullObjectForEdge(BlittableJsonReaderObject src, BlittableJsonReaderObject json)
        {
            return json != null && (json != src || src.HasParent);
        }

        private void CompleteInitialization()
        {
            var edgeAlias = _edgePath.Alias;
            var edge = _edgesExpression;
            edge.EdgeAlias = edgeAlias;

            var outputAlias = _left.GetOuputAlias();

            while (_left.GetNext(out var left))
            {
                var dummy = left.GetSingleDocumentResult(outputAlias);
                if (dummy == null)
                    throw new InvalidOperationException("Unable to find alias " + outputAlias + " in query results, this is probably a bug");

                var leftDoc = dummy.Data;

                if (edge.Where != null || edge.Project != null)
                {
                    if (BlittableJsonTraverser.Default.TryRead(leftDoc, edge.Path.FieldValue, out var value, out _) == false)
                        continue;

                    switch (value)
                    {
                        case BlittableJsonReaderArray array:
                            foreach (var item in array)
                            {
                                if (item is BlittableJsonReaderObject json &&
                                    edge.Where?.IsMatchedBy(json, _queryParameters) != false)
                                {
                                    AddEdgeAfterFiltering(left, json, edge, edgeAlias);
                                }
                            }
                            break;
                        case BlittableJsonReaderObject json:
                            if (edge.Where?.IsMatchedBy(json, _queryParameters) != false)
                            {
                                AddEdgeAfterFiltering(left, json, edge, edgeAlias);
                            }
                            break;
                    }
                }
                else
                {
                    AddEdgeAfterFiltering(left, leftDoc, edge, edgeAlias);
                }
            }
        }

        private void AddEdgeAfterFiltering(Match left, BlittableJsonReaderObject leftDoc, WithEdgesExpression edge, Sparrow.StringSegment edgeAlias)
        {
            var edgeIncludeOp = new EdgeIncludeOp(this);
            _includedEdges.Clear();
            IncludeUtil.GetDocIdFromInclude(leftDoc,
                 edge.Path.FieldValue,
                 edgeIncludeOp);

            if (_includedEdges.Count == 0)
                return;

            foreach (var includedEdge in _includedEdges)
            {
                if (_right.TryGetById(includedEdge.Key, out var rightMatch) == false)
                    continue;

                var clone = new Match(left);
                clone.Merge(rightMatch);

                if (ShouldUseFullObjectForEdge(leftDoc, includedEdge.Value))
                    clone.Set(edgeAlias, includedEdge.Value);
                else
                    clone.Set(edgeAlias, includedEdge.Key);

                _results.Add(clone);
            }
        }

        private void AddIncludes(Match left)
        {
            foreach (var kvp in _includedEdges)
            {
                if (kvp.Key == null)
                    continue;

                if (_right.TryGetById(kvp.Key, out var right))
                {
                    var clone = new Match(left);
                    clone.Merge(right);
                    //clone -> result row
                    _results.Add(clone);
                }
            }
        }

        public bool GetNext(out Match match)
        {
            if (_index >= _results.Count)
            {
                match = default;
                return false;
            }

            match = _results[_index++];
            return true;
        }

        public bool TryGetById(string id, out Match match)
        {
            throw new NotSupportedException("Cannot get a match by id from an edge");
        }

        public string GetOuputAlias()
        {
            return _right.GetOuputAlias();
        }

        public HashSet<string> GetAllAliases()
        {
            return _aliases;
        }

        private IQueryStep _left;
        private IQueryStep _right;
        private MatchPath _edgePath;
        private readonly BlittableJsonReaderObject _queryParameters;
        private WithEdgesExpression _edgesExpression;
        private Dictionary<string, BlittableJsonReaderObject> _includedEdges = new Dictionary<string, BlittableJsonReaderObject>();

        private struct EdgeIncludeOp : IncludeUtil.IIncludeOp
        {
            private EdgeQueryStep _parent;

            public EdgeIncludeOp(EdgeQueryStep parent)
            {
                _parent = parent;
            }

            public void Include(BlittableJsonReaderObject parent, string id)
            {
                _parent._includedEdges[id] = parent;
            }
        }
    }
}
