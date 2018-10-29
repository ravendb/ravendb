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
    public class QueryQueryStep : IQueryStep
    {
        private Query _query;
        private Sparrow.StringSegment _alias;
        private DocumentsOperationContext _context;
        private long? _resultEtag;
        private OperationCancelToken _token;
        private QueryRunner _queryRunner;
        private QueryMetadata _queryMetadata;

        public QueryQueryStep(QueryRunner queryRunner, Sparrow.StringSegment alias,Query query, QueryMetadata queryMetadata, DocumentsOperationContext documentsContext, long? existingResultEtag,
            OperationCancelToken token)
        {
            _query = query;
            _alias = alias;
            _queryRunner = queryRunner;
            _queryMetadata = queryMetadata;
            _context = documentsContext;
            _resultEtag = existingResultEtag;
            _token = token;
        }

        public async ValueTask<IEnumerable<Match>> Execute(Dictionary<IQueryStep, IEnumerable<Match>> matches)
        {           
            var results = await _queryRunner.ExecuteQuery(new IndexQueryServerSide(_queryMetadata),
                _context, _resultEtag, _token);

            var res = new List<Match>();
            foreach (var result in results.Results)
            {
                var match = new Match();
                match.Set(_alias, result);
                res.Add(match);
            }

            return res;
        }

        public IEnumerable<IQueryStep> Dependencies => Enumerable.Empty<IQueryStep>();
    }
}
