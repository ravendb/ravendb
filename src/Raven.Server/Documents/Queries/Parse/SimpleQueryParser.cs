using System.Collections.Generic;
using Lucene.Net.Analysis;
using Lucene.Net.Search;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Queries.LuceneIntegration;

namespace Raven.Server.Documents.Queries.Parse
{
    public class SimpleQueryParser // TODO arek - remove me
    {
        private static readonly Analyzer QueryAnalyzer = new RavenPerFieldAnalyzerWrapper(new KeywordAnalyzer());

        public static IEnumerable<string> GetTermValuesForField(IndexQueryServerSide query, string fieldName)
        {
            if (string.IsNullOrWhiteSpace(query.Query))
                yield break;

            var q = QueryBuilder.BuildQuery(query.Metadata, query.QueryParameters, QueryAnalyzer);
            var termQuery = q as TermQuery;
            if (termQuery != null)
            {
                if (termQuery.Term.Field != fieldName)
                    yield break;

                yield return termQuery.Term.Text;
                yield break;
            }

            var termsMatchQuery = q as TermsMatchQuery;
            if (termsMatchQuery != null)
            {
                if (termsMatchQuery.Field != fieldName)
                    yield break;

                var hashSet = new HashSet<string>(termsMatchQuery.Matches);
                foreach (var match in hashSet)
                    yield return match;
            }
        }
    }
}