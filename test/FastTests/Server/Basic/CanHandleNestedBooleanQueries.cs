using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Queries.Parse;
using Xunit;

namespace FastTests.Server.Basic
{
    public class CanParseNestedBooleanQueries : RavenTestBase
    {
        private static LuceneASTQueryConfiguration config = new LuceneASTQueryConfiguration
        {
            Analayzer = new RavenPerFieldAnalyzerWrapper( new LowerCaseKeywordAnalyzer()),
            DefaultOperator = Raven.Client.Data.QueryOperator.Or,
            FieldName = "foo"
        };

        [Fact]
        void CanParseThreeTermsWithDiffrentOperators()
        {
            var parser = new LuceneQueryParser();
            parser.Parse("foo:a AND foo:b foo:c");
            var query = parser.LuceneAST.ToQuery(config);
            Assert.Equal("+foo:a +foo:b foo:c", query.ToString());
        }

        [Fact]
        void CanParseThreeTermsWithDiffrentOperators2()
        {
            var parser = new LuceneQueryParser();
            parser.Parse("foo:a AND foo:b foo:-c");
            var query = parser.LuceneAST.ToQuery(config);
            Assert.Equal("+foo:a +foo:b foo:c", query.ToString());
        }

        [Fact]
        void CanParseParenthesisInsideNextedBooleanQuery()
        {
            var parser = new LuceneQueryParser();
            parser.Parse("foo:a AND foo:(b -d) foo:-c");
            var query = parser.LuceneAST.ToQuery(config);
            Assert.Equal("+foo:a +(foo:b -foo:d) foo:c", query.ToString());
        }

        [Fact]
        void CanParseParenthesisInsideNextedBooleanQuery2()
        {
            var parser = new LuceneQueryParser();
            parser.Parse("foo:a AND (foo:b -d) foo:-c");
            var query = parser.LuceneAST.ToQuery(config);
            Assert.Equal("+foo:a +(foo:b -foo:d) foo:c", query.ToString());
        }

        [Fact]
        void CanParseComplexedBooleanQuery()
        {
            var parser = new LuceneQueryParser();
            parser.Parse("(foo:a foo:b) (foo:b +d) AND (foo:(e -c) OR g)");
            var query = parser.LuceneAST.ToQuery(config);
            Assert.Equal("(foo:a foo:b) +(foo:b +foo:d) +((foo:e -foo:c) foo:g)", query.ToString());
        }
    }
}