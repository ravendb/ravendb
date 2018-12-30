using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;

namespace Raven.Server.Documents.Queries.Graph
{
    public class ForwardedQueryStep : IGraphQueryStep
    {
        public ForwardedQueryStep(IGraphQueryStep forwardedStep, string alias)
        {
            _forwardedStep = forwardedStep;
            _aliasStr = alias;
            var originalAliases = _forwardedStep.GetAllAliases();
            if (originalAliases.Count != 1)
            {
                throw new NotSupportedException("Currently 'ForwardedQueryStep' doesn't support steps with multiple aliases associated with them");
            }

            _originalAlias = originalAliases.Single();
            CollectIntermediateResults = _forwardedStep.CollectIntermediateResults;
        }
        private IGraphQueryStep _forwardedStep;
        public IGraphQueryStep ForwardedStep => _forwardedStep;
        private bool _initialized;
        private string _aliasStr;
        private string _originalAlias;
        private HashSet<string> _allAliases;

        public ValueTask Initialize()
        {
            if (_forwardedStep.IsInitialized == false)
            {
                _forwardedStep.Initialize();
            }
            else
            {
                //TODO:Need to verify that we won't step on our own toes when optimizing a recursive step
                _forwardedStep.Reset();
            }

            _initialized = true;
            return default;
        }

        HashSet<string> IGraphQueryStep.GetAllAliases()
        {
            if (_allAliases != null)
                return _allAliases;
            //We don't want to modify the original step aliases 
            _allAliases = new HashSet<string> { _aliasStr };
            foreach (var alias in _forwardedStep.GetAllAliases())
            {
                if (alias == _originalAlias)
                    continue;
                _allAliases.Add(alias);
            }
            return _allAliases;
        }

        public string GetOutputAlias()
        {
            var originalOutputAlias = _forwardedStep.GetOutputAlias();
            if (originalOutputAlias  == _originalAlias)
                return _aliasStr;
            return originalOutputAlias;
        }

        public bool GetNext(out GraphQueryRunner.Match match)
        {
            if (_forwardedStep.GetNext(out GraphQueryRunner.Match m) == false)
            {
                match = default;
                return false;
            }
            //TODO:We might need to clone here so not to dirtify the match
            m.ReplaceAlias(_originalAlias, _aliasStr);
            match = m;
            return true;
        }

        public List<GraphQueryRunner.Match> GetById(string id)
        {
            var res = _forwardedStep.GetById(id);
            foreach (var match in res)
            {
                match.ReplaceAlias(_originalAlias, _aliasStr);
            }

            return res;
        }

        public void Analyze(GraphQueryRunner.Match match, GraphQueryRunner.GraphDebugInfo graphDebugInfo)
        {
            //TODO:Verify this yields the expected result
            match.ReplaceAlias(_originalAlias, _aliasStr);
            _forwardedStep.Analyze(match, graphDebugInfo);
        }

        public bool IsEmpty()
        {
            return _forwardedStep.IsEmpty();
        }

        public bool CollectIntermediateResults { get; set; }

        private List<GraphQueryRunner.Match> _intermediateResults;
        public List<GraphQueryRunner.Match> IntermediateResults
        {
            get
            {
                if (_intermediateResults != null)
                    return _intermediateResults;
                //TODO: here we for sure need to clone the match
                var resList = _forwardedStep.IntermediateResults;
                foreach (var res in resList)
                {
                    res.ReplaceAlias(_originalAlias, _aliasStr);
                }

                return resList;
            }
        }
        public IGraphQueryStep Clone()
        {
            var res = new ForwardedQueryStep(_forwardedStep.Clone(), _aliasStr);
            return res;
        }

        public ISingleGraphStep GetSingleGraphStepExecution()
        {
            return _forwardedStep.GetSingleGraphStepExecution();
        }

        public bool IsInitialized => _initialized;
        public void Reset()
        {
            _forwardedStep.Reset();
        }
    }
}
