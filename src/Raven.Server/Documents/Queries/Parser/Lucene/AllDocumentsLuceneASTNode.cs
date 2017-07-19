using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public class AllDocumentsLuceneASTNode : LuceneASTNodeBase
    {
        public override IEnumerable<LuceneASTNodeBase> Children => Enumerable.Empty<LuceneASTNodeBase>();

        public override global::Lucene.Net.Search.Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            return new MatchAllDocsQuery();
        }

        public override string ToString()
        {
            return GetPrefixString() + "*:*";
        }
    }
}