using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Search;
using Lucene.Net.Search.Function;
using Lucene.Net.Search.Spans;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Queries.LuceneIntegration;

namespace Raven.Server.Documents.Queries.Parse
{
    public class SimpleQueryParser
    {
        private static readonly Analyzer QueryAnalyzer = new RavenPerFieldAnalyzerWrapper(new KeywordAnalyzer());

        public static IEnumerable<string> GetTermValuesForField(IndexQueryServerSide query, string fieldName)
        {
            if (string.IsNullOrWhiteSpace(query.Query))
                yield break;

            var q = QueryBuilder.BuildQuery(query.Parsed, QueryAnalyzer);
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