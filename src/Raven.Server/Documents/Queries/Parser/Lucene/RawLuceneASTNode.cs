using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    internal class RawLuceneASTNode : LuceneASTNodeBase
    {
        private readonly global::Lucene.Net.Search.Query _query;

        public RawLuceneASTNode(global::Lucene.Net.Search.Query query)
        {
            _query = query;
        }

        public override IEnumerable<LuceneASTNodeBase> Children => Enumerable.Empty<LuceneASTNodeBase>();

        public override global::Lucene.Net.Search.Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            return _query;
        }
    }
}