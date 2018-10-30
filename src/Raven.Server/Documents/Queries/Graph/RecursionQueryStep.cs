using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Graph
{
    public class RecursionQueryStep : IGraphQueryStep
    {
        private readonly IGraphQueryStep _inner;
        private readonly List<SingleEdgeMatcher> _steps;
        public int _index;
        private List<GraphQueryRunner.Match> _results = new List<GraphQueryRunner.Match>();

        public RecursionQueryStep(IGraphQueryStep inner, List<SingleEdgeMatcher> steps)
        {
            _inner = inner;
            _steps = steps;
        }

        public HashSet<string> GetAllAliases()
        {
            return _inner.GetAllAliases();
        }

        public bool GetNext(out GraphQueryRunner.Match match)
        {
            if(_index>= _results.Count)
            {
                match = default;
                return false;
            }
            match = _results[_index++];
            return true;
        }

        public string GetOuputAlias()
        {
            return _inner.GetOuputAlias();
        }

        public async ValueTask Initialize()
        {
            //TODO: fix this
            await _inner.Initialize();

            foreach (var item in _steps)
            {
                await item.Right.Initialize();
            }

            var last = _steps[_steps.Count - 1];
            while(_inner.GetNext(out var match))
            {
                _steps[0].Results.Clear();
                _steps[0].SingleMatch(match);

                if(_steps[0].Results.Count == 0)
                    continue;

                for (int i = 1; i < _steps.Count; i++)
                {
                    _steps[i].Results.Clear();

                    foreach (var prevMatch in _steps[i-1].Results)
                    {
                        _steps[i].SingleMatch(prevMatch);
                    }

                    if (_steps[i].Results.Count == 0)
                        break;
                }
                _results.AddRange(last.Results);
            }
        }

        public bool TryGetById(string id, out GraphQueryRunner.Match match)
        {
            throw new System.NotSupportedException("Cannot get matches by id from recursive portion");
        }
    }
}
