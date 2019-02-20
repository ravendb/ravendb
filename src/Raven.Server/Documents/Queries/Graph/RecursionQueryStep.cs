using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
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
        private readonly string _outputAlias;
        private readonly List<string> _stepAliases = new List<string>();
        private readonly List<Match> _results = new List<Match>();
        private List<Match> _temp = new List<Match>();
        private readonly HashSet<Match> _traversedPaths = new HashSet<Match>();
        private readonly HashSet<string> _allLliases = new HashSet<string>();

        private readonly HashSet<PathSegment> _visited = new HashSet<PathSegment>();
        private readonly Stack<RecursionState> _path = new Stack<RecursionState>();
        public int _index = -1;
        private bool _skipMaterialization;
        private OperationCancelToken _token;

        public IGraphQueryStep Left => _left;
        public List<SingleEdgeMatcher> Steps => _steps;

        private class RecursionState
        {
            public BlittableJsonReaderObject Src;
            public List<Match> Matches;
            public Match Match;
            public bool AlreadyAdded;
        }

        public RecursionQueryStep(IGraphQueryStep left, List<SingleEdgeMatcher> steps, RecursiveMatch recursive, RecursiveMatch.RecursiveOptions options, OperationCancelToken token)
        {
            _left = left;
            _steps = steps;
            _recursive = recursive;
            _options = options;

            _stepAliases.Add(left.GetOutputAlias());

            foreach (var step in _steps)
            {
                if (step.Right == null)
                    continue;
                _stepAliases.Add(step.Right.GetOutputAlias());
            }

            _outputAlias = _stepAliases.Last();
            _allLliases.UnionWith(_left.GetAllAliases());
            _allLliases.Add(_recursive.Alias.Value);
            _token = token;
        }

        public RecursionQueryStep(IGraphQueryStep left, RecursionQueryStep rqs, OperationCancelToken token)
        {
            _left = left;
            _steps = rqs._steps;
            _recursive = rqs._recursive;
            _options = rqs._options;

            _stepAliases.Add(left.GetOutputAlias());

            foreach (var step in _steps)
            {
                if (step.Right == null)
                    continue;
                _stepAliases.Add(step.Right.GetOutputAlias());
            }

            _outputAlias = _stepAliases.Last();
            _token = token;
        }

        public void SetAliases(HashSet<string> aliases)
        {
            _allLliases.UnionWith(aliases);
        }

        public RecursionQueryStep(RecursionQueryStep rqs, IGraphQueryStep left, List<SingleEdgeMatcher> steps, OperationCancelToken token)
        {
            _left = left;
            _steps = steps;
            _recursive = rqs._recursive;
            _options = rqs._options;
            _next = rqs._next;

            _stepAliases.Add(left.GetOutputAlias());

            foreach (var step in _steps)
            {
                if (step.Right == null)
                    continue;
                _stepAliases.Add(step.Right.GetOutputAlias());
            }

            _outputAlias = _stepAliases.Last();
            _token = token;
        }

        public bool IsEmpty()
        {
            return _results.Count == 0;
        }

        public bool CollectIntermediateResults { get; set; }

        public List<Match> IntermediateResults => CollectIntermediateResults ? _results : new List<Match>();

        public IGraphQueryStep Clone()
        {
            return new RecursionQueryStep(_left.Clone(), new List<SingleEdgeMatcher>(_steps), _recursive, _options, _token)
            {
                CollectIntermediateResults = CollectIntermediateResults
            };
        }

        internal (WithEdgesExpression Edge, StringSegment EdgeAlias, StringSegment RecursionAlias, string SourceAlias) GetOutputEdgeInfo()
        {
            var match = _steps[_steps.Count - 1];
            return (match.Edge, match.EdgeAlias, _recursive.Alias, _left.GetOutputAlias());
        }

        public void SetNext(ISingleGraphStep next)
        {
            _next = next;
            _next.AddAliases(_allLliases);
        }

        public ISingleGraphStep GetNextStep()
        {
            return _next;
        }

        public HashSet<string> GetAllAliases()
        {
            return _allLliases;
        }

        public bool GetNext(out Match match)
        {
            _token.ThrowIfCancellationRequested();
            if (_index >= _results.Count)
            {
                match = default;
                return false;
            }
            match = _results[_index++];
            return true;
        }

        public string GetOutputAlias()
        {
            return _outputAlias;
        }

        public ValueTask Initialize()
        {
            if (_index != -1)
                return default;

            _index = 0;

            _token.ThrowIfCancellationRequested();
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

                _token.ThrowIfCancellationRequested();

                var stepTask = item.Right.Initialize();
                if (stepTask.IsCompleted == false)
                {
                    return new ValueTask(CompleteInitializationForStepAsync(position, stepTask));
                }
            }
            if (_next != null)
            {
                var nextTask = _next.Initialize();
                if (nextTask.IsCompleted == false)
                {
                    return CompleteNextStepTaskAsync(nextTask);
                }
            }

            CompleteInitialization();
            return default;
        }

        private async ValueTask CompleteNextStepTaskAsync(ValueTask nextTask)
        {
            _token.ThrowIfCancellationRequested();
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
                _token.ThrowIfCancellationRequested();
                matches.Clear();
                ProcessSingleResultRecursive(match, matches);
                if (matches.Count > 0)
                {
                    foreach (var item in matches)
                    {
                        item.Remove(_outputAlias);
                        _results.Add(item);
                    }
                }
            }
        }

        internal class RecursionSingleStep : ISingleGraphStep
        {
            private OperationCancelToken _token;
            private readonly RecursionQueryStep _parent;
            private List<Match> _matches = new List<Match>();

            public RecursionSingleStep(RecursionQueryStep parent, OperationCancelToken token)
            {
                _token = token;
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
                _token.ThrowIfCancellationRequested();
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
                _token.ThrowIfCancellationRequested();
                await task;
                _parent._skipMaterialization = false;
            }

            public void Run(Match src, string alias)
            {
                _token.ThrowIfCancellationRequested();
                _parent.ProcessSingleResultRecursive(src, _matches);
            }

            public void AddAliases(HashSet<string> aliases)
            {
                aliases.UnionWith(_parent.GetAllAliases());
            }

            public void SetPrev(IGraphQueryStep prev)
            {

            }
        }

        private async Task CompleteInitializationForStepAsync(int position, ValueTask stepTask)
        {
            _token.ThrowIfCancellationRequested();
            await stepTask;
            await CompleteInitializationAfterLeft(position + 1);
        }

        private async Task CompleteLeftInitializationAsync(ValueTask leftTask)
        {
            _token.ThrowIfCancellationRequested();
            await leftTask;
            await CompleteInitializationAfterLeft(0);
        }

        private void ProcessSingleResultRecursive(Match currentMatch, List<Match> matches)
        {
            _token.ThrowIfCancellationRequested();
            _visited.Clear();
            _path.Clear();
            int? bestPathLength = null;

            var originalMatch = currentMatch;
            var startingPoint = currentMatch.GetSingleDocumentResult(_left.GetOutputAlias());
            if (startingPoint == null)
                return;

            _visited.Add(new PathSegment(0, startingPoint.Data.Location));
            _path.Push(new RecursionState { Src = startingPoint.Data, Match = currentMatch });

            int aliasBaseIndex = 0;
            Document cur = startingPoint;

            var alreadyTraversedPaths = new HashSet<MatchCollection>();

            //this function is needed for "documentation" purposes
            bool AddMatchToResultsAndCheckIfNeedToStop(Document current)
            {
                if (AddMatchToResults(current))
                    return true;

                _path.Pop();
                return false;
            }

            while (true)
            {
                _token.ThrowIfCancellationRequested();
                // the first item is always the root
                if (_path.Count - 1 == _options.Max)
                {
                    if (AddMatchToResultsAndCheckIfNeedToStop(cur))
                        return;
                }
                else
                {
                    //get relevant nodes to continue traversal over them
                    //so if 'dogs/1' likes 'dogs/2' and 'dogs/1' likes 'dogs/3', _currentMatches_ will contain 'dogs/2' and 'dogs/3'
                    if (TryGetNextNodesForTraversal(currentMatch, aliasBaseIndex, out var currentMatches) == false)
                    {
                        //if we don't have any nodes to continue traversal, and we have "lazy" recursive strategy stop
                        //recursive traversal because we found at least one matching path and it is enough.
                        //(AddMatch returns 'true' if we have "lazy" strategy selected)
                        //otherwise, just start back-tracking so we can add to results all permutations of a path - that is what _path.Pop() is doing
                        if (AddMatchToResultsAndCheckIfNeedToStop(cur))
                            return;
                    }
                    else
                    {
                        //store next traversal nodes in the last node of the path
                        _path.Peek().Matches = currentMatches;
                    }
                }

                if (aliasBaseIndex == 0)
                    aliasBaseIndex = 1;

                while (true)
                {
                    _token.ThrowIfCancellationRequested();
                    if (_path.Count == 0)
                        return;

                    if (_options.Type == RecursiveMatchType.Lazy &&
                        AddMatchToResults(cur))
                    {
                        return;
                    }

                    var top = _path.Peek();

                    //we have reached the end of traversal, so we backtrack.
                    //this is needed if there are multiple possible paths that need to be traversed
                    if (top.Matches == null || top.Matches.Count == 0)
                    {
                        var current = top.Match.GetSingleDocumentResult(_outputAlias);
                        if (current == null && _options.Min == 0)
                        {
                            current = top.Match.GetSingleDocumentResult(_left.GetOutputAlias());
                        }
                        if (current != null && AddMatchToResults(current))
                        {
                            return;
                        }

                        _path.Pop();

                        //since we are backtracking, remove the node from the top of the path stack
                        _visited.Remove(new PathSegment(top.Src.Location, cur.Data.Location));

                        continue;
                    }

                    //currentMatch - next step in recursive traversal.
                    //if we have "branching" path, start from last of them and remove it so we won't evaluate 
                    //certain path twice
                    currentMatch = top.Matches[top.Matches.Count - 1];
                    cur = currentMatch.GetSingleDocumentResult(_outputAlias);
                    top.Matches.RemoveAt(top.Matches.Count - 1);

                    if (_visited.Add(new PathSegment(top.Src.Location, cur.Data.Location)) == false)
                    {
                        continue;
                    }

                    //now, we add the currentMatch to "discovered" path and "jump" to resolution of the next step.
                    //resolving next step of traversal is this line:  if (ProcessSingleResult(currentMatch, aliasBaseIndex, out var currentMatches) == false)
                    var state = new RecursionState { Src = cur.Data, Match = currentMatch };
                    _path.Push(state);
                    break;
                }
            }

            bool AddMatchToResults(Document current)
            {
                var top = _path.Peek();
                if (top.AlreadyAdded)
                    return false;

                if (_path.Count <= _options.Min)
                    return false;

                var match = new Match(originalMatch);
                var list = new MatchCollection();
                foreach (var item in _path)
                {
                    var one = new Match();
                    foreach (var alias in _recursive.Aliases)
                    {
                        var v = item.Match.GetResult(alias.Value);
                        if (v == null)
                            continue;
                        one.Set(alias, v);
                    }
                    if (one.Empty)
                        continue;

                    list.Add(one);
                }

                list.Reverse();

                //add each distinct path only once
                if (alreadyTraversedPaths.Add(list) == false)
                    return false;

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


        private bool TryGetNextNodesForTraversal(Match match, int aliasBaseIndex, out List<Match> results)
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

        public void Analyze(Match match, GraphQueryRunner.GraphDebugInfo graphDebugInfo)
        {
            _left.Analyze(match, graphDebugInfo);

            var prev = match.GetResult(_left.GetOutputAlias());

            var result = match.GetResult(_recursive.Alias.Value);
            if (!(result is MatchCollection matches))
                return;

            foreach (var singleMatch in matches)
            {
                foreach (var step in _steps)
                {
                    if (step.Edge != null)
                    {
                        var next = EdgeQueryStep.AnalyzeEdge(step.Edge, step.EdgeAlias, singleMatch, prev, graphDebugInfo);
                        if (next != null)
                            prev = next;
                    }
                    step.Right?.Analyze(singleMatch, graphDebugInfo);
                }
            }

        }

        public ISingleGraphStep GetSingleGraphStepExecution()
        {
            return new RecursionSingleStep(this, _token);
        }
    }
}
