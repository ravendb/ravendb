using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Graph;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;
using Xunit;

namespace FastTests.Graph
{
    public class Parsing : RavenTestBase
    {
        [Fact]
        public void CanParseSimpleGraph()
        {
            const string text = @"
with { from Users } as u
match (u)
";
            var queryParser = new QueryParser();
            queryParser.Init(text);

            Query query = queryParser.Parse(QueryType.Select);
            if (query.GraphQuery.MatchClause is PatternMatchElementExpression p)
            {
                Assert.Equal(1, p.Path.Length);
                Assert.Equal("u", p.Path[0].Alias);
                Assert.Equal(EdgeType.Outgoing, p.Path[0].EdgeType);
            }
            else
            {
                Assert.False(true, "Exepcted to get proper match expr");

            }
        }

        [Fact]
        public void CanParseComplexGraph()
        {
            const string text = @"
with { from Users } as src
match (src)-[r1:Rated]->(m:Movie)<-[r2:Rated]-(dst:Users) and (dst:Users)-[a:PaidFor]->(m)
";
            var queryParser = new QueryParser();
            queryParser.Init(text);

            Query query = queryParser.Parse(QueryType.Select);
            if (query.GraphQuery.MatchClause is BinaryExpression be)
            {
                Assert.Equal(OperatorType.And, be.Operator);
                var left = (PatternMatchElementExpression)be.Left;
                Assert.Equal(5, left.Path.Length);
                var right = (PatternMatchElementExpression)be.Right;
                Assert.Equal(3, right.Path.Length);
            }
            else
            {
                Assert.False(true, "Exepcted to get proper match expr");

            }
        }

        [Fact]
        public void CanParsePath()
        {
            const string text = @"
with { from Users } as u
with edges(Rated)  as r
with { from Movies } as m
match (u)-[r]->(m)
";
            var queryParser = new QueryParser();
            queryParser.Init(text);

            Query query = queryParser.Parse(QueryType.Select);
            if (query.GraphQuery.MatchClause is PatternMatchElementExpression p)
            {
                Assert.Equal(3, p.Path.Length);
                Assert.Equal("u", p.Path[0].Alias);
                Assert.Equal(EdgeType.Outgoing, p.Path[0].EdgeType);
                Assert.Equal("r", p.Path[1].Alias);
                Assert.Equal(EdgeType.Outgoing, p.Path[1].EdgeType);
                Assert.Equal("m", p.Path[2].Alias);
                Assert.Equal(EdgeType.Outgoing, p.Path[2].EdgeType);
            }
            else
            {
                Assert.False(true, "Exepcted to get proper match expr");

            }
        }

        [Fact]
        public void CanParseIncomingPaths()
        {
            const string text = @"
with { from Users } as u
with edges(Rated) { } as r
with { from Movies } as m
match (m)<-[r]<-(u)
";
            var queryParser = new QueryParser();
            queryParser.Init(text);

            Query query = queryParser.Parse(QueryType.Select);
            if (query.GraphQuery.MatchClause is PatternMatchElementExpression p)
            {
                Assert.Equal(3, p.Path.Length);
                Assert.Equal("m", p.Path[0].Alias);
                Assert.Equal(EdgeType.Incoming, p.Path[0].EdgeType);
                Assert.Equal("r", p.Path[1].Alias);
                Assert.Equal(EdgeType.Incoming, p.Path[1].EdgeType);
                Assert.Equal("u", p.Path[2].Alias);
                Assert.Equal(EdgeType.Outgoing, p.Path[2].EdgeType);
            }
            else
            {
                Assert.False(true, "Exepcted to get proper match expr");

            }
        }

        [Fact]
        public void CanRewriteQuery()
        {
            const string text = @"
match (m:Movies)<-[r:Rated]<-( u:Users(City='Hadera') )
";
            var queryParser = new QueryParser();
            queryParser.Init(text);

            Query query = queryParser.Parse(QueryType.Select);
            if (query.GraphQuery.MatchClause is PatternMatchElementExpression p)
            {
                Assert.Equal(3, p.Path.Length);
                Assert.Equal("m", p.Path[0].Alias);
                Assert.Equal(EdgeType.Incoming, p.Path[0].EdgeType);
                Assert.Equal("r", p.Path[1].Alias);
                Assert.Equal(EdgeType.Incoming, p.Path[1].EdgeType);
                Assert.Equal("u", p.Path[2].Alias);
                Assert.Equal(EdgeType.Outgoing, p.Path[2].EdgeType);

                Assert.Equal("FROM Users WHERE City = 'Hadera'", query.GraphQuery.WithDocumentQueries["u"].ToString().Trim());
                Assert.Equal("FROM Movies", query.GraphQuery.WithDocumentQueries["m"].ToString().Trim());
                Assert.Equal("WITH EDGES(Rated)", query.GraphQuery.WithEdgePredicates["r"].ToString());
            }
            else
            {
                Assert.False(true, "Exepcted to get proper match expr");

            }
        }

        [Fact]
        public async Task ParseBasicGraphQuery()
        {
            using (DocumentStore store = GetDocumentStore())
            using (Raven.Server.Documents.DocumentDatabase database = await GetDocumentDatabaseInstanceFor(store))
            {

                CreateGraphData(store);

                GraphQuery ast = CreateAst();
                var graphQueryRunner = new GraphQueryRunner(database, ast);
                Raven.Server.Documents.Queries.DocumentQueryResult result = await graphQueryRunner.RunAsync();
            }
        }

        private static GraphQuery CreateAst()
        {
            var queryParser = new QueryParser();
            queryParser.Init("from Movies where Name = 'Star Wars Episode 1'");
            Query firstWithClause = queryParser.Parse();

            queryParser = new QueryParser();
            queryParser.Init("from Movies");
            Query secondWithClause = queryParser.Parse();

            queryParser = new QueryParser();
            queryParser.Init("from Users where Age between 18 and 35");
            Query thirdWithClause = queryParser.Parse();

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
                //MatchClause = new PatternMatchBinaryExpression(
                //    new PatternMatchElementExpression
                //    {
                //        FromAlias = "lovedMovie",
                //        ToAlias = "Genre",
                //        EdgeAlias = "dominantGenre"
                //    },
                //    new PatternMatchBinaryExpression(
                //        new PatternMatchElementExpression
                //        {
                //            FromAlias = "recommendedMovie",
                //            ToAlias = "Genre",
                //            EdgeAlias = "alias1"
                //        },
                //        new PatternMatchElementExpression
                //        {
                //            FromAlias = "usersWhoRated",
                //            ToAlias = "recommendedMovie"
                //        }, PatternMatchBinaryExpression.Operator.And),
                //    PatternMatchBinaryExpression.Operator.And
                //)
            };
        }

        private void CreateGraphData(DocumentStore store)
        {
            using (Raven.Client.Documents.Session.IDocumentSession session = store.OpenSession())
            {
                var starwars = new Movie
                {
                    Name = "Star Wars Episode 1"
                };

                var scifi = new Genre
                {
                    Name = "Sci-Fi"
                };

                var fantasy = new Genre
                {
                    Name = "Fantasy"
                };

                var adventure = new Genre
                {
                    Name = "Adventure"
                };

                var postApocalypse = new Genre
                {
                    Name = "Post-apocalypse"
                };


                var firefly = new Movie
                {
                    Name = "Firefly Serenity"
                };

                var thePostman = new Movie
                {
                    Name = "The Postman"
                };

                User[] users = new[]
                {
                    new User
                    {
                        Name = "John Dow",
                        Age = 19
                    },
                    new User
                    {
                        Name = "Jake Dow",
                        Age = 15
                    },
                    new User
                    {
                        Name = "Jane Dow"
                        ,Age = 33
                    },
                    new User
                    {
                        Name = "Jeff Dow"
                        ,Age = 31
                    },
                    new User
                    {
                        Name = "Jenn Dow"
                        ,Age = 23
                    },
                    new User
                    {
                        Name = "Jeri Dow"
                        ,Age = 15
                    },
                    new User
                    {
                        Name = "July Dow"
                        ,Age = 48
                    },
                    new User
                    {
                        Name = "Jyll Dow"
                        ,Age = 44
                    },
                    new User
                    {
                        Name = "Jace Dow"
                        ,Age = 30
                    },
                    new User
                    {
                        Name = "Jared Dow"
                        ,Age = 32
                    },
                };

                foreach (User u in users)
                {
                    session.Store(u);
                }

                session.Store(starwars);
                session.Store(firefly);
                session.Store(thePostman);

                session.Store(scifi);
                session.Store(fantasy);
                session.Store(adventure);
                session.Store(postApocalypse);

                session.Advanced.AddEdgeBetween(starwars, scifi, "HasGenre", new Dictionary<string, object> { { "Weight", 3 } });
                session.Advanced.AddEdgeBetween(starwars, fantasy, "HasGenre", new Dictionary<string, object> { { "Weight", 6 } });
                session.Advanced.AddEdgeBetween(starwars, adventure, "HasGenre", new Dictionary<string, object> { { "Weight", 1 } });

                session.Advanced.AddEdgeBetween(firefly, scifi, "HasGenre", new Dictionary<string, object> { { "Weight", 7 } });
                session.Advanced.AddEdgeBetween(firefly, adventure, "HasGenre", new Dictionary<string, object> { { "Weight", 3 } });

                session.Advanced.AddEdgeBetween(thePostman, postApocalypse, "HasGenre", new Dictionary<string, object> { { "Weight", 4 } });
                session.Advanced.AddEdgeBetween(thePostman, adventure, "HasGenre", new Dictionary<string, object> { { "Weight", 6 } });

                session.Advanced.AddEdgeBetween(users[0], starwars, "Rated", new Dictionary<string, object> { { "Rating", 5 } });
                session.Advanced.AddEdgeBetween(users[0], firefly, "Rated", new Dictionary<string, object> { { "Rating", 7 } });
                session.Advanced.AddEdgeBetween(users[0], thePostman, "Rated", new Dictionary<string, object> { { "Rating", 2 } });

                session.Advanced.AddEdgeBetween(users[1], starwars, "Rated", new Dictionary<string, object> { { "Rating", 8 } });
                session.Advanced.AddEdgeBetween(users[1], firefly, "Rated", new Dictionary<string, object> { { "Rating", 4 } });
                session.Advanced.AddEdgeBetween(users[1], thePostman, "Rated", new Dictionary<string, object> { { "Rating", 3 } });

                session.Advanced.AddEdgeBetween(users[2], starwars, "Rated", new Dictionary<string, object> { { "Rating", 3 } });
                session.Advanced.AddEdgeBetween(users[2], thePostman, "Rated", new Dictionary<string, object> { { "Rating", 10 } });

                session.Advanced.AddEdgeBetween(users[3], starwars, "Rated", new Dictionary<string, object> { { "Rating", 7 } });
                session.Advanced.AddEdgeBetween(users[3], thePostman, "Rated", new Dictionary<string, object> { { "Rating", 4 } });

                session.Advanced.AddEdgeBetween(users[4], firefly, "Rated", new Dictionary<string, object> { { "Rating", 6 } });
                session.Advanced.AddEdgeBetween(users[4], starwars, "Rated", new Dictionary<string, object> { { "Rating", 6 } });

                session.Advanced.AddEdgeBetween(users[5], firefly, "Rated", new Dictionary<string, object> { { "Rating", 5 } });
                session.Advanced.AddEdgeBetween(users[5], thePostman, "Rated", new Dictionary<string, object> { { "Rating", 5 } });

                session.Advanced.AddEdgeBetween(users[7], firefly, "Rated", new Dictionary<string, object> { { "Rating", 9 } });

                session.Advanced.AddEdgeBetween(users[8], starwars, "Rated", new Dictionary<string, object> { { "Rating", 8 } });
                session.Advanced.AddEdgeBetween(users[8], thePostman, "Rated", new Dictionary<string, object> { { "Rating", 6 } });

                session.Advanced.AddEdgeBetween(users[9], thePostman, "Rated", new Dictionary<string, object> { { "Rating", 3 } });
                session.SaveChanges();
            }
        }

    }
}
