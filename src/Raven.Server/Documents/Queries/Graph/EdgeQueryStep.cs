using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class EdgeQueryStep : IGraphQueryStep
    {
        private List<Match> _results = new List<Match>();
        private int _index = 0;

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
            string alias = _left.GetOuputAlias();

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

        public string GetOuputAlias()
        {
            return _right.GetOuputAlias();
        }

        public HashSet<string> GetAllAliases()
        {
            return _aliases;
        }

        private IGraphQueryStep _left;
        private IGraphQueryStep _right;
        private MatchPath _edgePath;
        private readonly BlittableJsonReaderObject _queryParameters;
        private WithEdgesExpression _edgesExpression;

    }
}
