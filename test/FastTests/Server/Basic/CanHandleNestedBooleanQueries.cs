using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Parser.Lucene;
using Xunit;

namespace FastTests.Server.Basic
{
    public class CanParseNestedBooleanQueries : RavenTestBase
    {
        private static readonly LuceneASTQueryConfiguration Config = new LuceneASTQueryConfiguration
        {
            Analyzer = new RavenPerFieldAnalyzerWrapper(new LowerCaseKeywordAnalyzer()),
            DefaultOperator = QueryOperator.Or,
            FieldName = new FieldName("foo")
        };

        [Fact]
        public void CanParseThreeTermsWithDiffrentOperators()
        {
            var query = LegacyQueryBuilder.BuildQuery("foo:a AND foo:b foo:c", Config.DefaultOperator, Config.FieldName.Field, Config.Analyzer);
            Assert.Equal("+foo:a +foo:b foo:c", query.ToString());
        }

        [Fact]
        public void CanParseComplexedBooleanQuery()
        {
            var query = LegacyQueryBuilder.BuildQuery("(foo:a foo:b) (foo:b +d) AND (foo:(e -c) OR g)", Config.DefaultOperator, Config.FieldName.Field, Config.Analyzer);
            Assert.Equal("(foo:a foo:b) +(foo:b +foo:d) +((foo:e -foo:c) foo:g)", query.ToString());
        }
    }
}