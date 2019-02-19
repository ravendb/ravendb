using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Extensions;
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
        [InlineData("with { from Users} as u match (u)-[Rated as r where Rating > 4]->(Movies as m)", @"WITH {
    FROM Users
} AS u
WITH {
    FROM Movies
} AS m
WITH EDGES (Rated) {
    WHERE Rating > 4
} AS r
MATCH (u)-[r]->(m)")]
        [InlineData("match (Users as u where id() == 'users/1-A')-[Rated as r where Rating > 4]->(Movies as m where Genre = $genre)", @"WITH {
    FROM Users WHERE id() = 'users/1-A'
} AS u
WITH {
    FROM Movies WHERE Genre = $genre
} AS m
WITH EDGES (Rated) {
    WHERE Rating > 4
} AS r
MATCH (u)-[r]->(m)")]
        [InlineData("match (Users as u)<-[Rated as r]-(Movies as m)", @"WITH {
    FROM Users
} AS u
WITH {
    FROM Movies
} AS m
WITH EDGES (Rated) AS r
MATCH (m)-[r]->(u)")]
        [InlineData(@"
with { from Movies where Genre = $genre } as m
match (Users as u)<-[Rated as r]-(m) and (Actors as actor)-[ActedOn]->(m) and (u)-[Likes]->(actor)", @"WITH {
    FROM Movies WHERE Genre = $genre
} AS m
WITH {
    FROM Users
} AS u
WITH {
    FROM Actors
} AS actor
WITH EDGES (Rated) AS r
WITH EDGES (ActedOn) AS actor_ActedOn
WITH EDGES (Likes) AS u_Likes
MATCH ((m)-[r]->(u) AND ((actor)-[actor_ActedOn]->(m) AND (u)-[u_Likes]->(actor)))")]
        [InlineData(@"
with { from Movies where Genre = $genre } as m
match ((Users as u)<-[Rated as r]-(m) and not (Actors as actor)-[ActedOn]->(m)) or (u)-[Likes]->(actor)", @"WITH {
    FROM Movies WHERE Genre = $genre
} AS m
WITH {
    FROM Users
} AS u
WITH {
    FROM Actors
} AS actor
WITH EDGES (Rated) AS r
WITH EDGES (ActedOn) AS actor_ActedOn
WITH EDGES (Likes) AS u_Likes
MATCH (((m)-[r]->(u) AND NOT ((actor)-[actor_ActedOn]->(m))) OR (u)-[u_Likes]->(actor))")]
        [InlineData(@"with { from Movies where Genre = $genre } as m
match (Users as u)<-[Rated as r]-(m)", @"WITH {
    FROM Movies WHERE Genre = $genre
} AS m
WITH {
    FROM Users
} AS u
WITH EDGES (Rated) AS r
MATCH (m)-[r]->(u)")]
        [InlineData(@"with { from Movies where Genre = $genre } as m
match (Users as u)<-[Rated as r]-(m)-[Starred as s]->(Actor as a)", @"WITH {
    FROM Movies WHERE Genre = $genre
} AS m
WITH {
    FROM Users
} AS u
WITH {
    FROM Actor
} AS a
WITH EDGES (Rated) AS r
WITH EDGES (Starred) AS s
MATCH ((m)-[s]->(a) AND (m)-[r]->(u))
")]
        public void CanRoundTripQueries(string q, string expected)
        {
            var queryParser = new QueryParser();
            queryParser.Init(q);
            var a = q.Contains("((");
            if (a)
            {
                System.Console.WriteLine();
            }
            var query = queryParser.Parse();
            var result = query.ToString();
            //System.Console.WriteLine(result);
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
match (src)-[Rated as r1]->(Movie as m)<-[Rated as r2]-(Users as dst) and (Users as dst)-[PaidFor as a]->(m)
";
            var queryParser = new QueryParser();
            queryParser.Init(text);

            Query query = queryParser.Parse(QueryType.Select);
            if (query.GraphQuery.MatchClause is BinaryExpression be)
            {
                Assert.Equal(OperatorType.And, be.Operator);
                var left = (BinaryExpression)be.Left;
                Assert.Equal(3, ((PatternMatchElementExpression)left.Left).Path.Length);
                Assert.Equal(3, ((PatternMatchElementExpression)left.Right).Path.Length);
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
with EDGES (Rated)  as r
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
with EDGES (Rated) { } as r
with { from Movies } as m
match (m)<-[r]-(u)
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
        public void CanRewriteQuery()
        {
            const string text = @"
match (Movies as m)<-[Rated as r]-( Users as u where City='Hadera' )
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

                Assert.Equal("FROM Users WHERE City = 'Hadera'", query.GraphQuery.WithDocumentQueries["u"].withQuery.ToString().Trim());
                Assert.Equal("FROM Movies", query.GraphQuery.WithDocumentQueries["m"].withQuery.ToString().Trim());
                Assert.Equal("WITH EDGES (Rated)", query.GraphQuery.WithEdgePredicates["r"].ToString().Trim());
            }
            else
            {
                Assert.False(true, "Exepcted to get proper match expr");

            }
        }
    }
}
