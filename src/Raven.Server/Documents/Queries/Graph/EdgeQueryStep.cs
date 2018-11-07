using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Json;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class EdgeQueryStep : IGraphQueryStep
    {
        private List<Match> _results = new List<Match>();
        private int _index = -1;

        private HashSet<string> _aliases;

        public EdgeQueryStep(IGraphQueryStep left, IGraphQueryStep right, WithEdgesExpression edgesExpression, MatchPath edgePath, BlittableJsonReaderObject queryParameters)
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
            if (_index != -1)
                return default;

            var leftTask = _left.Initialize();
            if(leftTask.IsCompleted)
            {
                var rightTask = _right.Initialize();
                if(rightTask.IsCompleted)
                {
                    CompleteInitialization();
                    return default;
                }
                return new ValueTask(InitializeRightAsync(rightTask));
            }

            return new ValueTask(InitializeLeftAsync(leftTask));
        }

        private async Task InitializeRightAsync(ValueTask rightTask)
        {
            await rightTask;
            CompleteInitialization();
        }

        private async Task InitializeLeftAsync(ValueTask leftTask)
        {
            await leftTask;
            var rightTask = _right.Initialize();
            if (rightTask.IsCompleted == false)
            {
                await rightTask;
            }
            CompleteInitialization();
        }

        private void CompleteInitialization()
        {
            _index = 0;
            var edgeAlias = _edgePath.Alias;
            var edge = _edgesExpression;
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
            string alias = _left.GetOutputAlias();

            while (_left.GetNext(out var left))
            {
                processor.SingleMatch(left, alias);
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

        public string GetOutputAlias()
        {
            return _right.GetOutputAlias();
        }

        public HashSet<string> GetAllAliases()
        {
            return _aliases;
        }

        public void Analyze(Match match, Action<string, object> addNode, Action<object, string> addEdge)
        {
            _left.Analyze(match, addNode, addEdge);
            _right.Analyze(match, addNode, addEdge);

            var prev = match.GetResult(_left.GetOutputAlias());

            AnalyzeEdge(_edgesExpression, _edgePath.Alias, match, prev, addEdge);
        }

        public static string AnalyzeEdge(WithEdgesExpression _edgesExpression, StringSegment edgeAlias, Match match, object prev, Action<object, string> addEdge)
        {
            var edge = match.GetResult(edgeAlias);

            if (edge == null || prev == null)
                return null;

            string result = null;

            if (edge is string s)
            {
                result = s;
            }
            else if (edge is LazyStringValue lsv)
            {
                result = lsv.ToString();
            }
            else if (edge is BlittableJsonReaderObject bjro)
            {
                if (_edgesExpression.Project != null)
                {
                    if (BlittableJsonTraverser.Default.TryRead(bjro, _edgesExpression.Project.FieldValue, out var id, out _))
                    {
                        if (id != null)
                        {
                            result = id.ToString();
                        }
                    }
                }
            }

            if (result == null)
                return null;

            addEdge(prev, result);

            return result;
        }

        private IGraphQueryStep _left;
        private IGraphQueryStep _right;
        private MatchPath _edgePath;
        private readonly BlittableJsonReaderObject _queryParameters;
        private WithEdgesExpression _edgesExpression;

    }
}
