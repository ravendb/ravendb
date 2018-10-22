using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries
{
    public partial class GraphQueryRunner
    {
        public class IntermediateResults
        {
            private Dictionary<string, Dictionary<string, Match>> _matchesByAlias;
            private Dictionary<string, IndexQueryServerSide> _queryPerAlias;
            private DocumentDatabase _database;
            private DocumentsOperationContext _documentsContext;
            private long? _existingResultEtag;
            private OperationCancelToken _token;

            public IntermediateResults(DocumentDatabase database, IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag,
                OperationCancelToken token)
            {
                var q = query.Metadata.Query;
                if (q.GraphQuery == null)
                    throw new InvalidOperationException($"None graph queries can't run through the GraphQueryRunner, query = {q}.");
                _database = database;
                _documentsContext = documentsContext;
                _existingResultEtag = existingResultEtag;
                _token = token;
                _matchesByAlias = new Dictionary<string, Dictionary<string, Match>>();
                _queryPerAlias  = new Dictionary<string, IndexQueryServerSide>();

                //Here we only populate the queries to be run but we might decide to not run them (in case of optimizations).
                foreach (var documentQuery in q.GraphQuery.WithDocumentQueries)
                {
                    var queryMetadata = new QueryMetadata(documentQuery.Value, query.QueryParameters, 0);
                    _queryPerAlias.Add(documentQuery.Key, new IndexQueryServerSide(queryMetadata));
                }
            }

            public void Add(Match match)
            {
                foreach (var alias in match.Aliases)
                {
                    Add(alias, match, match.Get(alias));
                }
            }

            public void Add(string alias, Match match, Document instance)
            {
                //TODO: need to handle map/reduce results?
                _matchesByAlias[alias][instance.Id] = match;
            }

            public bool TryGetByAlias(string alias, out Dictionary<string, Match> value)
            {
                if (_matchesByAlias.TryGetValue(alias, out value) == false)
                {
                    if (_queryPerAlias.TryGetValue(alias, out var query) == false)
                        return false;

                    _matchesByAlias[alias] = new Dictionary<string, Match>();
                    //TODO: should we move everything to be async?
                    var results = AsyncHelpers.RunSync( ()=>_database.QueryRunner.ExecuteQuery(query, _documentsContext, _existingResultEtag, _token));
                    foreach (var result in results.Results)
                    {
                        var match = new Match();
                        match.Set(alias, result);
                        match.PopulateVertices(this);
                    }

                    return _matchesByAlias.TryGetValue(alias, out value);
                }

                return true;
            }

        }
    }
}
