using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Json;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class EdgeFollowingRecursionQueryStep : IGraphQueryStep
    {
        private readonly RecursionQueryStep _left;
        private readonly IGraphQueryStep _right;
        private readonly BlittableJsonReaderObject _queryParameters;
        private readonly HashSet<string> _allAliases;
        private List<Match> _results = new List<Match>();
        private int _index = -1;

        public EdgeFollowingRecursionQueryStep(RecursionQueryStep left, IGraphQueryStep right, BlittableJsonReaderObject queryParameters)
        {
            _left = left;
            _right = right;
            _queryParameters = queryParameters;
            _allAliases = new HashSet<string>(left.GetAllAliases());
            _allAliases.UnionWith(right.GetAllAliases());
        }

        public HashSet<string> GetAllAliases()
        {
            return _allAliases;
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

        public string GetOuputAlias()
        {
            return _right.GetOuputAlias();
        }

        public ValueTask Initialize()
        {
            if (_index != -1)
                return default;

            var leftTask = _left.Initialize();
            if (leftTask.IsCompleted == false)
            {
                return new ValueTask(InitializeLeftAsync(leftTask));
            }

            return CompleteInitializationAfterLeft();
        }

        private async Task InitializeLeftAsync(ValueTask leftTask)
        {
            await leftTask;
            await CompleteInitializationAfterLeft();
        }

        private ValueTask CompleteInitializationAfterLeft()
        {
            var rightTask = _right.Initialize();
            if (rightTask.IsCompleted == false)
            {
                return new ValueTask(CompleteRightInitializationAsync(rightTask));
            }
            CompleteInitialization();
            return default;
        }

        private async Task CompleteRightInitializationAsync(ValueTask rightTask)
        {
            await rightTask;
            CompleteInitialization();
        }

        private void CompleteInitialization()
        {
            _index = 0;

            var (edge, edgeAlias, recursionAlias, sourceAlias) = _left.GetOutputEdgeInfo();

            edge.EdgeAlias = edgeAlias;

            var processor = new SingleEdgeMatcher
            {
                IncludedEdges = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase),
                QueryParameters = _queryParameters,
                Results = _results,
                Right = _right,
                Edge = edge,
                EdgeAlias = edgeAlias
            };

            while (_left.GetNext(out var left))
            {
                if (left.GetResult(recursionAlias) is List<Match> list)
                {
                    object val;
                    Match top;
                    StringSegment actualEdgeAlias;
                    if (list.Count == 0)
                    {
                        // need to handle the case of recursive(0, $num), where we can recurse zero times
                        top = left;
                        actualEdgeAlias = sourceAlias;
                        val = top.GetResult(sourceAlias);
                    }
                    else
                    {
                        actualEdgeAlias = edgeAlias;
                        top = list[list.Count - 1];

                        val = top.GetResult(edgeAlias);

                        // we project only if we had actual values to follow, otherwise
                        // the source of the query is fine for us (it would be filtered by something else otherwise)
                        if (edge.Project != null)
                        {
                            if (val is BlittableJsonReaderObject bjro)
                            {
                                if (BlittableJsonTraverser.Default.TryRead(bjro, edge.Project.FieldValue, out val, out _) == false)
                                    continue;
                            }
                            else if (val is BlittableJsonReaderArray bjra)
                            {
                                foreach (var item in bjra)
                                {
                                    if (item is BlittableJsonReaderObject itemJson)
                                        if (BlittableJsonTraverser.Default.TryRead(itemJson, edge.Project.FieldValue, out val, out _) == false)
                                            continue;

                                    ProcessValue(ref processor, left, top, actualEdgeAlias, val);
                                }
                                continue;
                            }
                            else
                            {
                                continue;
                            }
                        }
                    }


                    ProcessValue(ref processor, left, top, actualEdgeAlias, val);
                }
            }
        }

        private void ProcessValue(ref SingleEdgeMatcher processor, Match left, Match top, StringSegment actualEdgeAlias, object val)
        {
            if(val is LazyStringValue lsv)
            {
                val = lsv.ToString();
            }
            else if(val is LazyCompressedStringValue lscv)
            {
                val = lscv.ToString();
            }

            if (val is string id)
            {
                Match rightMatch = default;
                if (_right.TryGetById(id, out rightMatch) == false)
                    return;

                var clone = new Match(left);
                clone.Merge(rightMatch);

                _results.Add(clone);

                return;
            }


            processor.SingleMatch(top, actualEdgeAlias);
        }

        public bool TryGetById(string id, out GraphQueryRunner.Match match)
        {
            throw new System.NotSupportedException("Cannot pull records from an recursive step");
        }

        public void Analyze(Match match, Action<string, object> addNode, Action<object, string> addEdge)
        {
            _left.Analyze(match, addNode, addEdge);
            _right.Analyze(match, addNode, addEdge);
        }
    }
}
