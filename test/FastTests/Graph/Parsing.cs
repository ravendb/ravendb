using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Extensions;
using Sparrow;
using Xunit;

namespace FastTests.Graph
{
    public class Parsing : RavenTestBase
    {
        [Theory]
        [InlineData("with { from Users} as u match (u)", @"WITH {
    FROM Users
} AS u
MATCH (u)")]
        [InlineData("with { from Users} as u match (u)-[r:Rated(Rating > 4)]->(m:Movies)", @"WITH {
    FROM Users
} AS u
WITH {
    FROM Movies
} AS m
WITH EDGES(Rated) {
    WHERE Rating > 4
} AS r
MATCH (u)-[r]->(m)")]
        [InlineData("match (u:Users(id() == 'users/1-A'))-[r:Rated(Rating > 4)]->(m:Movies(Genre = $genre))", @"WITH {
    FROM Users WHERE id() = 'users/1-A'
} AS u
WITH {
    FROM Movies WHERE Genre = $genre
} AS m
WITH EDGES(Rated) {
    WHERE Rating > 4
} AS r
MATCH (u)-[r]->(m)")]
        [InlineData("match (u:Users)<-[r:Rated]-(m:Movies)", @"WITH {
    FROM Users
} AS u
WITH {
    FROM Movies
} AS m
WITH EDGES(Rated) AS r
MATCH (u)<-[r]-(m)")]
        [InlineData(@"
with { from Movies where Genre = $genre } as m
match (u:Users)<-[r:Rated]-(m) and (actor:Actors)-[:ActedOn]->(m) and (u)-[:Likes]->(actor)", @"WITH {
    FROM Movies WHERE Genre = $genre
} AS m
WITH {
    FROM Users
} AS u
WITH {
    FROM Actors
} AS actor
WITH EDGES(Rated) AS r
WITH EDGES(ActedOn) AS __alias1
WITH EDGES(Likes) AS __alias2
MATCH ((u)<-[r]-(m) AND ((actor)-[__alias1]->(m) AND (u)-[__alias2]->(actor)))")]
        [InlineData(@"
with { from Movies where Genre = $genre } as m
match ((u:Users)<-[r:Rated]-(m) and not (actor:Actors)-[:ActedOn]->(m)) or (u)-[:Likes]->(actor)", @"WITH {
    FROM Movies WHERE Genre = $genre
} AS m
WITH {
    FROM Users
} AS u
WITH {
    FROM Actors
} AS actor
WITH EDGES(Rated) AS r
WITH EDGES(ActedOn) AS __alias1
WITH EDGES(Likes) AS __alias2
MATCH (((u)<-[r]-(m) AND NOT ((actor)-[__alias1]->(m))) OR (u)-[__alias2]->(actor))")]
        [InlineData(@"with { from Movies where Genre = $genre } as m
match (u:Users)<-[r:Rated]-(m)", @"WITH {
    FROM Movies WHERE Genre = $genre
} AS m
WITH {
    FROM Users
} AS u
WITH EDGES(Rated) AS r
MATCH (m)-[r]->(u)")]
        [InlineData(@"with { from Movies where Genre = $genre } as m
match (u:Users)<-[r:Rated]-(m)->(a:Actor)", @"WITH {
    FROM Movies WHERE Genre = $genre
} AS m
WITH {
    FROM Users
} AS u
WITH {
    FROM Actor
} AS a
WITH EDGES(Rated) AS r
MATCH ((m)-[r]->(u) AND (m)->(a))")]
        public void CanRoundTripQueries(string q, string expected)
        {
            var queryParser = new QueryParser();
            queryParser.Init(q);
            var query = queryParser.Parse();
            var result = query.ToString();
            System.Console.WriteLine(result);
            Assert.Equal(expected.NormalizeLineEnding(), result.NormalizeLineEnding());

        }


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
                Assert.Equal(EdgeType.Right, p.Path[0].EdgeType);
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
                Assert.Equal(EdgeType.Right, p.Path[0].EdgeType);
                Assert.Equal("r", p.Path[1].Alias);
                Assert.Equal(EdgeType.Right, p.Path[1].EdgeType);
                Assert.Equal("m", p.Path[2].Alias);
                Assert.Equal(EdgeType.Right, p.Path[2].EdgeType);
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
match (m)<-[r]-(u)
";
            var queryParser = new QueryParser();
            queryParser.Init(text);

            Query query = queryParser.Parse(QueryType.Select);
            if (query.GraphQuery.MatchClause is PatternMatchElementExpression p)
            {
                Assert.Equal(3, p.Path.Length);
                Assert.Equal("m", p.Path[0].Alias);
                Assert.Equal(EdgeType.Left, p.Path[0].EdgeType);
                Assert.Equal("r", p.Path[1].Alias);
                Assert.Equal(EdgeType.Left, p.Path[1].EdgeType);
                Assert.Equal("u", p.Path[2].Alias);
                Assert.Equal(EdgeType.Left, p.Path[2].EdgeType);
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
match (m:Movies)<-[r:Rated]-( u:Users(City='Hadera') )
";
            var queryParser = new QueryParser();
            queryParser.Init(text);

            Query query = queryParser.Parse(QueryType.Select);
            if (query.GraphQuery.MatchClause is PatternMatchElementExpression p)
            {
                Assert.Equal(3, p.Path.Length);
                Assert.Equal("m", p.Path[0].Alias);
                Assert.Equal(EdgeType.Left, p.Path[0].EdgeType);
                Assert.Equal("r", p.Path[1].Alias);
                Assert.Equal(EdgeType.Left, p.Path[1].EdgeType);
                Assert.Equal("u", p.Path[2].Alias);
                Assert.Equal(EdgeType.Left, p.Path[2].EdgeType);

                Assert.Equal("FROM Users WHERE City = 'Hadera'", query.GraphQuery.WithDocumentQueries["u"].ToString().Trim());
                Assert.Equal("FROM Movies", query.GraphQuery.WithDocumentQueries["m"].ToString().Trim());
                Assert.Equal("WITH EDGES(Rated)", query.GraphQuery.WithEdgePredicates["r"].ToString());
            }
            else
            {
                Assert.False(true, "Exepcted to get proper match expr");

            }
        }
    }
}
