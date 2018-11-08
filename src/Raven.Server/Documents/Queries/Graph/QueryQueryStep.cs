using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class QueryQueryStep : IGraphQueryStep
    {
        private Query _query;
        private Sparrow.StringSegment _alias;
        private HashSet<string> _aliases;
        private DocumentsOperationContext _context;
        private long? _resultEtag;
        private OperationCancelToken _token;
        private QueryRunner _queryRunner;
        private QueryMetadata _queryMetadata;
        private List<Match> _temp = new List<Match>();


        private int _index = -1;
        private List<Match> _results = new List<Match>();
        private Dictionary<string, Match> _resultsById = new Dictionary<string, Match>(StringComparer.OrdinalIgnoreCase);


        public QueryQueryStep(QueryRunner queryRunner, Sparrow.StringSegment alias,Query query, QueryMetadata queryMetadata, DocumentsOperationContext documentsContext, long? existingResultEtag,
            OperationCancelToken token)
        {
            _query = query;
            _alias = alias;
            _aliases = new HashSet<string> { _alias };
            _queryRunner = queryRunner;
            _queryMetadata = queryMetadata;
            _context = documentsContext;
            _resultEtag = existingResultEtag;
            _token = token;
        }

        public bool IsCollectionQuery => _queryMetadata.IsCollectionQuery;
        public bool HasWhereClause => _query.Where != null;

        public static CollectionDestinationQueryStep ToCollectionDestinationQueryStep(DocumentsStorage documentsStorage, QueryQueryStep qqs)
        {
            return new CollectionDestinationQueryStep(qqs._alias, qqs._context, documentsStorage);
        }

        public IGraphQueryStep Clone()
        {
            return new QueryQueryStep(_queryRunner, _alias, _query, _queryMetadata, _context, _resultEtag, _token);
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

        public ValueTask Initialize()
        {
            if (_index != -1)
                return default;

            var results = _queryRunner.ExecuteQuery(new IndexQueryServerSide(_queryMetadata),
                  _context, _resultEtag, _token);

            if (results.IsCompleted)
            {
                // most of the time, we complete in a sync fashion
                CompleteInitialization(results.Result);
                return default;
            }
            
            return new ValueTask(CompleteInitializeAsync(results));
        }

        private async Task CompleteInitializeAsync(Task<DocumentQueryResult> results)
        {
            CompleteInitialization(await results);
        }

        private void CompleteInitialization(DocumentQueryResult results)
        {
            _index = 0;
            foreach (var result in results.Results)
            {
                var match = new Match();
                match.Set(_alias, result);
                _results.Add(match);
                if (result.Id == null)
                    continue;
                _resultsById[result.Id] = match;
            }
        }

        public List<Match> GetById(string id)
        {
            if(_results.Count != 0 && _resultsById.Count == 0)// only reason is that we are projecting non documents here
                throw new InvalidOperationException("Target vertices in a pattern match that originate from map/reduce WITH clause are not allowed. (pattern match has multiple statements in the form of (a)-[:edge]->(b) ==> in such pattern, 'b' must not originate from map/reduce index query)");
              
            _temp.Clear();
            if (_resultsById.TryGetValue(id, out var match))
                _temp.Add(match);
            return _temp;

        }

        public string GetOutputAlias()
        {
            return _alias;
        }

        public HashSet<string> GetAllAliases()
        {
            return _aliases;
        }

        public void Analyze(Match match, Action<string, object> addNode, Action<object, string> addEdge)
        {
            var result = match.GetResult(_alias);
            if (result == null)
                return;

            if(result is Document d && d.Id != null)
            {
                addNode(d.Id.ToString(), d);
            }
            else
            {
                addNode(null, result);
            }
        }

        public ISingleGraphStep GetSingleGraphStepExecution()
        {
            return new QuerySingleStep(this);
    }

        private class QuerySingleStep : ISingleGraphStep
        {
            private QueryQueryStep _parent;
            private List<Match> _temp = new List<Match>(1);

            public QuerySingleStep(QueryQueryStep queryQueryStep)
            {
                _parent = queryQueryStep;
}


            public void AddAliases(HashSet<string> aliases)
            {
                aliases.UnionWith(_parent.GetAllAliases());
            }

            public bool GetAndClearResults(List<Match> matches)
            {
                if (_temp.Count == 0)
                    return false;

                matches.AddRange(_temp);

                _temp.Clear();

                return true;
            }

            public ValueTask Initialize()
            {
                return _parent.Initialize();
            }

            public void Run(Match src, string alias)
            {
                // here we already get the right match, and we do nothing with it.
                var clone = new Match(src);
                clone.Remove(alias);
                clone.Set(_parent.GetOuputAlias(), src.GetResult(alias));
                _temp.Add(clone);
            }
        }
    }
}
