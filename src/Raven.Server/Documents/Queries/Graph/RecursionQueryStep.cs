using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Sparrow.Json;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class RecursionQueryStep : IGraphQueryStep
    {
        private readonly IGraphQueryStep _left;
        private ISingleGraphStep _next;
        private readonly List<SingleEdgeMatcher> _steps;
        private readonly RecursiveMatch _recursive;
        private readonly RecursiveMatch.RecursiveOptions _options;
        private string _outputAlias;
        private readonly List<string> _stepAliases = new List<string>();
        private List<Match> _results = new List<Match>();
        private List<Match> _temp = new List<Match>();
        private readonly HashSet<string> _allLliases = new HashSet<string>();
        private readonly HashSet<long> _visited = new HashSet<long>();
        private readonly Stack<RecursionState> _path = new Stack<RecursionState>();
        public int _index = -1;
        private bool _skipMaterialization;

        private class RecursionState
        {
            public BlittableJsonReaderObject Src;
            public List<Match> Matches;
            public Match Match;
            public bool AlreadyAdded;
        }

        public RecursionQueryStep(IGraphQueryStep left, List<SingleEdgeMatcher> steps, RecursiveMatch recursive, RecursiveMatch.RecursiveOptions options)
        {
            _left = left;
            _steps = steps;
            _recursive = recursive;
            _options = options;

            _stepAliases.Add(left.GetOuputAlias());

            foreach (var step in _steps)
            {
                if (step.Right == null)
                    continue;
                _stepAliases.Add(step.Right.GetOuputAlias());
            }

            _outputAlias = _stepAliases.Last();
            _allLliases.UnionWith(_left.GetAllAliases());
            _allLliases.Add(_recursive.Alias);
        }

        public void SetNext(ISingleGraphStep next)
        {
            _next = next;
            _next.AddAliases(_allLliases);
        }

        public HashSet<string> GetAllAliases()
        {
            return _allLliases;
        }

        public bool GetNext(out Match match)
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
            return _outputAlias;
        }

        public ValueTask Initialize()
        {
            if (_index != -1)
                return default;

            _index = 0;

            var leftTask = _left.Initialize();
            if (leftTask.IsCompleted == false)
            {
                return new ValueTask(CompleteLeftInitializationAsync(leftTask));
            }


            return CompleteInitializationAfterLeft(0);
        }

        private ValueTask CompleteInitializationAfterLeft(int position)
        {           
            for (var i = position; i < _steps.Count; i++)
            {
                var item = _steps[i];
                if (item.Right == null)
                    continue;

                var stepTask = item.Right.Initialize();
                if (stepTask.IsCompleted == false)
                {
                    return new ValueTask(CompleteInitializationForStepAsyc(position, stepTask));
                }
            }
            if(_next != null)
            {
                var nextTask = _next.Initialize();
                if(nextTask.IsCompleted == false)
                {
                    return CompleteNextStepTaskAsync(nextTask);
                }
            }

            CompleteInitialization();
            return default;
        }

        private async ValueTask CompleteNextStepTaskAsync(ValueTask nextTask)
        {
            await nextTask;
            CompleteInitialization();
        }

        private void CompleteInitialization()
        {
            if (_skipMaterialization)
            {
                return;
            }
            
            var matches = new List<Match>();
            while (_left.GetNext(out var match))
            {
                matches.Clear();
                ProcessSingleResultRecursive(match, matches);
                if (matches.Count > 0)
                    _results.AddRange(matches);
            }
        }

        private class RecursionSingleStep : ISingleGraphStep
        {
            private readonly RecursionQueryStep _parent;
            private List<Match> _matches = new List<Match>();

            public RecursionSingleStep(RecursionQueryStep parent)
            {
                _parent = parent;
            }

            public bool GetAndClearResults(List<Match> matches)
            {
                if (_matches.Count == 0)
                    return false;

                matches.AddRange(_matches);
                _matches.Clear();

                return true;
            }

            public ValueTask Initialize()
            {
                _parent._skipMaterialization = true;
                var task = _parent.Initialize();
                if (task.IsCompleted)
                {
                    _parent._skipMaterialization = false;
                    return default;
                }
                return CompleteInit(task);
            }

            private async ValueTask CompleteInit(ValueTask task)
            {
                await task;
                _parent._skipMaterialization = false;
            }

            public void Run(Match src, string alias)
            {
                _parent.ProcessSingleResultRecursive(src, _matches);
            }

            public void AddAliases(HashSet<string> aliases)
            {
                aliases.UnionWith(_parent.GetAllAliases());
            }
        }

        private async Task CompleteInitializationForStepAsyc(int position, ValueTask stepTask)
        {
            await stepTask;
            await CompleteInitializationAfterLeft(position + 1);
        }

        private async Task CompleteLeftInitializationAsync(ValueTask leftTask)
        {
            await leftTask;
            await CompleteInitializationAfterLeft(0);
        }

        private void ProcessSingleResultRecursive(Match currentMatch, List<Match> matches)
        {
            _visited.Clear();
            _path.Clear();
            int? bestPathLength = null;

            var originalMatch = currentMatch;
            var startingPoint = currentMatch.GetSingleDocumentResult(_left.GetOuputAlias());
            if (startingPoint == null)
                return;

            _visited.Add(startingPoint.Data.Location);
            _path.Push(new RecursionState { Src = startingPoint.Data, Match = currentMatch });

            int aliasBaseIndex = 0;

            Document cur = startingPoint;
            while (true)
            {
                // the first item is always the root
                if (_path.Count - 1 == _options.Max)
                {
                    if (AddMatch(cur))
                        return;
                    _path.Pop();
                }
                else
                {
                    if (ProcessSingleResult(currentMatch, aliasBaseIndex, out var currentMatches) == false)
                    {
                        if (AddMatch(cur))
                        {
                            return ;
                        }
                        _path.Pop();
                    }
                    else
                    {
                        _path.Peek().Matches = currentMatches;
                    }
                }

                if (aliasBaseIndex == 0)
                    aliasBaseIndex = 1;

                while (true)
                {
                    if (_path.Count == 0)
                        return ;

                    if (_options.Type == RecursiveMatchType.Lazy &&
                        AddMatch(cur))
                    {
                        return ;
                    }

                    var top = _path.Peek();
                    if (top.Matches == null || top.Matches.Count == 0)
                    {
                        var current = top.Match.GetSingleDocumentResult(_outputAlias);
                        if(current == null && _options.Min == 0)
                        {
                            current = top.Match.GetSingleDocumentResult(_left.GetOuputAlias());
                        }
                        if (current != null && AddMatch(current))
                        {
                            return;
                        }

                        _path.Pop();
                        _visited.Remove(top.Src.Location);
                        continue;
                    }
                    currentMatch = top.Matches[top.Matches.Count - 1];
                    cur = currentMatch.GetSingleDocumentResult(_outputAlias);
                    top.Matches.RemoveAt(top.Matches.Count - 1);
                    if (_visited.Add(cur.Data.Location) == false)
                    {
                        continue;
                    }
                    _path.Push(new RecursionState { Src = cur.Data, Match = currentMatch });
                    break;
                }
            }

            bool AddMatch(Document current)
            {
                var top = _path.Peek();
                if (top.AlreadyAdded)
                    return false;

                if (_path.Count <= _options.Min)
                    return false;

                if (bestPathLength != null)
                {
                    switch (_options.Type)
                    {
                        case RecursiveMatchType.Longest:
                            if (_path.Count <= bestPathLength.Value)
                                return false;
                            matches.RemoveAt(0);
                            break;
                        case RecursiveMatchType.Shortest:
                            if (_path.Count >= bestPathLength.Value)
                                return false;
                            matches.RemoveAt(0);
                            break;
                    }
                }



                var match = new Match(originalMatch);
                var list = new List<Match>();
                foreach (var item in _path)
                {
                    var one = new Match();
                    foreach (var alias in _recursive.Aliases)
                    {
                        var v = item.Match.GetResult(alias);
                        if (v == null)
                            continue;
                        one.Set(alias, v);
                    }
                    if (one.Empty)
                        continue;

                    list.Add(one);
                }
                list.Reverse();

                match.Set(_recursive.Alias, list);
                match.Set(_outputAlias, current);

                if (_next != null)
                {
                    _next.Run(match, _outputAlias);
                    if (_next.GetAndClearResults(matches) == false)
                        return false;
                }
                else
                {
                    matches.Add(match);
                }
                top.AlreadyAdded = true;
                if (_options.Type == RecursiveMatchType.Longest)
                {
                    foreach (var item in _path)
                    {
                        item.AlreadyAdded = true;
                    }
                }
                bestPathLength = _path.Count;

                return _options.Type == RecursiveMatchType.Lazy;
            }
        }


        private bool ProcessSingleResult(Match match, int aliasBaseIndex , out List<Match> results)
        {
            _steps[0].Results.Clear();
            _steps[0].SingleMatch(match, _stepAliases[aliasBaseIndex]);

            results = _steps[0].Results;
            if (_steps[0].Results.Count == 0)
            {
                return false;
            }

            for (int i = 1; i < _steps.Count; i++)
            {
                _steps[i].Results.Clear();

                foreach (var prevMatch in _steps[i - 1].Results)
                {
                    _steps[i].SingleMatch(prevMatch, _stepAliases[aliasBaseIndex + i]);
                }
                results = _steps[i].Results;
                if (_steps[i].Results.Count == 0)
                    return false;
            }

            results = results.ToList(); //TODO: Pool these
            return true;
        }

        public List<Match> GetById(string id)
        {
            _temp.Clear();
            var matches = _left.GetById(id);

            foreach (var item in matches)
            {
                ProcessSingleResultRecursive(item, _temp);
            }

            return _temp;
        }

        public void Analyze(Match match, Action<string, object> addNode, Action<object, string> addEdge)
        {
            _left.Analyze(match, addNode, addEdge);

            var prev = match.GetResult(_left.GetOuputAlias());

            var result = match.GetResult(_recursive.Alias);
            if (!(result is List<Match> matches))
                return;

            foreach (var singleMatch in matches)
            {
                foreach (var step in _steps)
                {
                    if (step.Edge != null)
                    {
                        var next = EdgeQueryStep.AnalyzeEdge(step.Edge, step.EdgeAlias, singleMatch, prev, addEdge);
                        if (next != null)
                            prev = next;
                    }
                    step.Right?.Analyze(singleMatch, addNode, addEdge);
                }
            }

        }

        public ISingleGraphStep GetSingleGraphStepExecution()
        {
            return new RecursionSingleStep(this);
        }
    }
}
