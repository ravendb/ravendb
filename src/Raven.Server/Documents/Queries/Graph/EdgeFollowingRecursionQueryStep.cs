using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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

        public async ValueTask Initialize()
        {
            if (_index != -1)
                return;

            await _left.Initialize();
            await _right.Initialize();

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
                if(left.GetResult(recursionAlias) is List<Match> list)
                {
                    Match top;
                    StringSegment actualEdgeAlias;
                    if (list.Count == 0)
                    {
                        // need to handle the case of recursive(0, $num), where we can recurse zero times
                        top = left;
                        actualEdgeAlias = sourceAlias;
                    }
                    else
                    {
                        actualEdgeAlias = edgeAlias;
                        top = list[list.Count - 1];
                    }

                    if(top.GetResult(edgeAlias) is string id)
                    {
                        Match rightMatch = default;
                        if (_right.TryGetById(id, out rightMatch) == false)
                            continue;

                        var clone = new Match(left);
                        clone.Merge(rightMatch);

                        _results.Add(clone);


                        continue;
                    }

                    processor.SingleMatch(top, actualEdgeAlias);
                }
            }
        }

        public bool TryGetById(string id, out GraphQueryRunner.Match match)
        {
            throw new System.NotSupportedException("Cannot pull records from an recursive step");
        }
    }
}
