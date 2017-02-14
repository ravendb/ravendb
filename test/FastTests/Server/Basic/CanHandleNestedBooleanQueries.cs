using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Queries.Parse;
using Xunit;

namespace FastTests.Server.Basic
{
    public class CanParseNestedBooleanQueries : RavenTestBase
    {
        private static readonly LuceneASTQueryConfiguration Config = new LuceneASTQueryConfiguration
        {
            Analayzer = new RavenPerFieldAnalyzerWrapper(new LowerCaseKeywordAnalyzer()),
            DefaultOperator = QueryOperator.Or,
            FieldName = "foo"
        };

        [Fact]
        public void CanParseThreeTermsWithDiffrentOperators()
        {
            var parser = new LuceneQueryParser();
            parser.Parse("foo:a AND foo:b foo:c");
            var query = parser.LuceneAST.ToQuery(Config);
            Assert.Equal("+foo:a +foo:b foo:c", query.ToString());
        }

        [Fact]
        public void CanParseThreeTermsWithDiffrentOperators2()
        {
            var parser = new LuceneQueryParser();
            parser.Parse("foo:a AND foo:b foo:-c");
            var query = parser.LuceneAST.ToQuery(Config);
            Assert.Equal("+foo:a +foo:b foo:c", query.ToString());
        }

        [Fact]
        public void CanParseParenthesisInsideNextedBooleanQuery()
        {
            var parser = new LuceneQueryParser();
            parser.Parse("foo:a AND foo:(b -d) foo:-c");
            var query = parser.LuceneAST.ToQuery(Config);
            Assert.Equal("+foo:a +(foo:b -foo:d) foo:c", query.ToString());
        }

        [Fact]
        public void CanParseParenthesisInsideNextedBooleanQuery2()
        {
            var parser = new LuceneQueryParser();
            parser.Parse("foo:a AND (foo:b -d) foo:-c");
            var query = parser.LuceneAST.ToQuery(Config);
            Assert.Equal("+foo:a +(foo:b -foo:d) foo:c", query.ToString());
        }

        [Fact]
        public void CanParseComplexedBooleanQuery()
        {
            var parser = new LuceneQueryParser();
            parser.Parse("(foo:a foo:b) (foo:b +d) AND (foo:(e -c) OR g)");
            var query = parser.LuceneAST.ToQuery(Config);
            Assert.Equal("(foo:a foo:b) +(foo:b +foo:d) +((foo:e -foo:c) foo:g)", query.ToString());
        }
    }
}