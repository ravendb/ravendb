using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Indexing;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Querying
{
    public class CanParseNestedBooleanQueries : RavenTest
    {
        private static LuceneASTQueryConfiguration config = new LuceneASTQueryConfiguration
        {
            Analayzer = new RavenPerFieldAnalyzerWrapper(new KeywordAnalyzer()),
            DefaultOperator = QueryOperator.Or,
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
