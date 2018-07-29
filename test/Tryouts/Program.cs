using System;
using System.Collections.Generic;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;
using Xunit.Sdk;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            /*
                The AST for 

                with { from Movies where Name = “Star Wars Episode 1” } as lovedMovie
                with { from Movies } as recommendedMovie
                with edges(HasGenre) { order by Weight desc limit 1 } as dominantGenre
                match (lovedMovie)-[dominantGenre]->(Genre)<-[HasGenre(Weight > 0.8)]-(recommendedMovie)
                select recommendedMovie           
                
             */
            var queryParser = new QueryParser();
            queryParser.Init("from Movies where Name = 'Star Wars Episode 1'");
            var firstWithClause = queryParser.Parse();

            queryParser = new QueryParser();
            queryParser.Init("from Movies");
            var secondWithClause = queryParser.Parse();

            queryParser = new QueryParser();
            queryParser.Init("from Users where Age between 18 and 35");
            var thirdWithClause = queryParser.Parse();

            var graphQuery = new GraphQuery
            {
                WithDocumentQueries = new Dictionary<StringSegment, Query>
                {
                    {"lovedMovie",firstWithClause},
                    {"recommendedMovie",secondWithClause},
                    {"usersWhoRated",thirdWithClause},
                },
                WithEdgePredicates = new Dictionary<StringSegment, WithEdgesExpression>
                {
                    {"dominantGenre",
                        new WithEdgesExpression(
                            null,
                            "HasGenre",
                            new List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)>
                            {
                                (new FieldExpression(new List<StringSegment>{"Weight"}),OrderByFieldType.Double,false)
                            }
                        )
                    },
                    {"minimumRecommendWeight",
                        new WithEdgesExpression(
                            new BinaryExpression(new FieldExpression(new List<StringSegment>{"Weight"}),
                                                 new ValueExpression("0.8",ValueTokenType.Double),
                                                 OperatorType.GreaterThan),"HasGenre", null
                        )
                    },
                    {"highRating",
                        new WithEdgesExpression(
                            new BinaryExpression(new FieldExpression(new List<StringSegment>{"Rating"}),
                                new ValueExpression("4",ValueTokenType.Long),
                                OperatorType.GreaterThanEqual), "HasRating",null
                        )
                    }

                },
                MatchClause = new PatternMatchElementExpression
                {
                    From = new PatternMatchVertexExpression("lovedMovie", null),
                    Edge = new PatternMatchExpressEdge("dominantGenre",null),
                    EdgeDirection = PatternMatchElementExpression.Direction.Right,
                    To = new PatternMatchElementExpression
                    {
                        To = new PatternMatchVertexExpression(null,"Genre"),
                        Edge = new PatternMatchExpressEdge("minimumRecommendWeight",null),
                        EdgeDirection = PatternMatchElementExpression.Direction.Left,
                        From = new PatternMatchElementExpression
                        {
                            To = new PatternMatchVertexExpression("recommendedMovie",null),
                            EdgeDirection = PatternMatchElementExpression.Direction.Left,
                            Edge = new PatternMatchExpressEdge("highRating",null),
                            From = new PatternMatchVertexExpression("usersWhoRated",null)
                        }
                    }
                }
            };

            Console.WriteLine(graphQuery.ToString());
        }
    }
}
