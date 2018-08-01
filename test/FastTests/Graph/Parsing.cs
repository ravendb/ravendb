using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Graph;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;
using Xunit;

namespace FastTests.Graph
{
    public class Parsing : RavenLowLevelTestBase
    {
        [Fact]
        public async Task ParseBasicGraphQuery()
        {
            using (var database = CreateDocumentDatabase())
            {
                var graphQueryRunner = new GraphQueryRunner(database);
                var ast = CreateAst();

                var result = await graphQueryRunner.RunAsync(ast);
            }            
        }

        private static GraphQuery CreateAst()
        {
            var queryParser = new QueryParser();
            queryParser.Init("from Movies where Name = 'Star Wars Episode 1'");
            var firstWithClause = queryParser.Parse();

            queryParser = new QueryParser();
            queryParser.Init("from Movies");
            var secondWithClause = queryParser.Parse();

            queryParser = new QueryParser();
            queryParser.Init("from Users where Age between 18 and 35");
            var thirdWithClause = queryParser.Parse();

            return new GraphQuery
            {
                WithDocumentQueries = new Dictionary<StringSegment, Query>
                {
                    {"lovedMovie", firstWithClause},
                    {"recommendedMovie", secondWithClause},
                    {"usersWhoRated", thirdWithClause},
                },
                WithEdgePredicates = new Dictionary<StringSegment, WithEdgesExpression>
                {
                    {
                        "dominantGenre",
                        new WithEdgesExpression(
                            null,
                            "HasGenre",
                            new List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)>
                                {(new FieldExpression(new List<StringSegment> {"Weight"}), OrderByFieldType.Double, false)}
                        )
                    },
                    {
                        "minimumRecommendWeight",
                        new WithEdgesExpression(
                            new BinaryExpression(new FieldExpression(new List<StringSegment> {"Weight"}),
                                new ValueExpression("0.8", ValueTokenType.Double),
                                OperatorType.GreaterThan), "HasGenre", null
                        )
                    },
                    {
                        "highRating",
                        new WithEdgesExpression(
                            new BinaryExpression(new FieldExpression(new List<StringSegment> {"Rating"}),
                                new ValueExpression("4", ValueTokenType.Long),
                                OperatorType.GreaterThanEqual), "HasRating", null
                        )
                    },
                    {
                        "alias1",
                        new WithEdgesExpression(
                            new BinaryExpression(new FieldExpression(new List<StringSegment> {"Rating"}),
                                new ValueExpression("0.8", ValueTokenType.Double),
                                OperatorType.GreaterThan), null, null
                        )
                    }
                },
                MatchClause = new PatternMatchBinaryExpression(
                    new PatternMatchElementExpression
                    {
                        From = new PatternMatchVertexExpression("lovedMovie", null),
                        To = new PatternMatchVertexExpression(null, "Genre"),
                        EdgeAlias = "dominantGenre"
                    },
                    new PatternMatchBinaryExpression(
                        new PatternMatchElementExpression
                        {
                            From = new PatternMatchVertexExpression("recommendedMovie", null),
                            To = new PatternMatchVertexExpression(null, "Genre"),
                            EdgeAlias = "alias1"
                        },
                        new PatternMatchElementExpression
                        {
                            From = new PatternMatchVertexExpression("usersWhoRated", null),
                            To = new PatternMatchVertexExpression(null, "recommendedMovie"),
                        }, PatternMatchBinaryExpression.Operator.And),
                    PatternMatchBinaryExpression.Operator.And
                )
            };
        }
    }
}
