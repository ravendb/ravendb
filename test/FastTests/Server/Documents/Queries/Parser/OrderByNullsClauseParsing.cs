using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Queries.Parser
{
    public class OrderByNullsClauseParsing : NoDisposalNeeded
    {
        public OrderByNullsClauseParsing(ITestOutputHelper output) : base(output)
        {
        }
        
        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData("FROM Users ORDER BY Name NULLS FIRST", OrderByFieldType.Implicit, true, true)]
        [InlineData("FROM Users ORDER BY Name NULLS LAST", OrderByFieldType.Implicit, true, false)]
        [InlineData("FROM Users ORDER BY Name ASC NULLS FIRST", OrderByFieldType.Implicit, true, true)]
        [InlineData("FROM Users ORDER BY Name DESC NULLS LAST", OrderByFieldType.Implicit, false, false)]
        [InlineData("FROM Users ORDER BY Age AS long NULLS FIRST", OrderByFieldType.Long, true, true)]
        [InlineData("FROM Users ORDER BY Age AS long DESC NULLS FIRST", OrderByFieldType.Long, false, true)]
        [InlineData("FROM Users ORDER BY Score AS double NULLS LAST", OrderByFieldType.Double, true, false)]
        [InlineData("FROM Users ORDER BY Name AS alphaNumeric ASC NULLS FIRST", OrderByFieldType.AlphaNumeric, true, true)]
        [InlineData("FROM Users ORDER BY Name AS string DESC NULLS LAST", OrderByFieldType.String, false, false)]
        [InlineData("FROM Users ORDER BY Name nulls first", OrderByFieldType.Implicit, true, true)]
        [InlineData("FROM Users ORDER BY Name Nulls Last", OrderByFieldType.Implicit, true, false)]
        public void NullsClauseIsParsedIntoOrderByTuple(string q, OrderByFieldType expectedType, bool expectedAscending, bool expectedNullFirst)
        {
            var query = Parse(q);

            Assert.NotNull(query.OrderBy);
            var clause = Assert.Single(query.OrderBy);
            Assert.Equal(expectedType, clause.FieldType);
            Assert.Equal(expectedAscending, clause.Ascending);
            Assert.True(clause.NullFirst.HasValue);
            Assert.Equal(expectedNullFirst, clause.NullFirst.Value);
            Assert.IsType<FieldExpression>(clause.Expression);
        }
        
        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData("FROM Users ORDER BY Name")]
        [InlineData("FROM Users ORDER BY Name DESC")]
        [InlineData("FROM Users ORDER BY Age AS long")]
        [InlineData("FROM Users ORDER BY Score AS double DESC")]
        public void OrderByWithoutNullsClauseHasNullFirstUnset(string q)
        {
            var query = Parse(q);

            Assert.NotNull(query.OrderBy);
            var clause = Assert.Single(query.OrderBy);
            Assert.False(clause.NullFirst.HasValue);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void MultipleClausesEachCarryTheirOwnNullsValue()
        {
            var query = Parse("FROM Users ORDER BY Name NULLS FIRST, Age DESC, Score AS double NULLS LAST");

            Assert.NotNull(query.OrderBy);
            Assert.Equal(3, query.OrderBy.Count);

            Assert.True(query.OrderBy[0].Ascending);
            Assert.True(query.OrderBy[0].NullFirst);

            Assert.False(query.OrderBy[1].Ascending);
            Assert.False(query.OrderBy[1].NullFirst.HasValue); // omitted clause -> null

            Assert.Equal(OrderByFieldType.Double, query.OrderBy[2].FieldType);
            Assert.True(query.OrderBy[2].Ascending);
            Assert.True(query.OrderBy[2].NullFirst.HasValue);
            Assert.False(query.OrderBy[2].NullFirst.Value);
        }
        
        [RavenFact(RavenTestCategory.Querying)]
        public void MixedAscDescAndMixedNullsFirstLastInSameOrderBy()
        {
            var query = Parse("FROM Users ORDER BY Name DESC NULLS FIRST, Age ASC NULLS LAST");

            Assert.Equal(2, query.OrderBy.Count);

            Assert.False(query.OrderBy[0].Ascending);
            Assert.True(query.OrderBy[0].NullFirst);

            Assert.True(query.OrderBy[1].Ascending);
            Assert.False(query.OrderBy[1].NullFirst);
        }
        
        [RavenFact(RavenTestCategory.Querying)]
        public void MethodOrderByAcceptsNullsClause()
        {
            var query = Parse("FROM Users ORDER BY id() AS string NULLS LAST");

            var clause = Assert.Single(query.OrderBy);
            Assert.IsType<MethodExpression>(clause.Expression);
            Assert.Equal(OrderByFieldType.String, clause.FieldType);
            Assert.False(clause.NullFirst);
        }
        
        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData("FROM Users ORDER BY spatial.distance(Location, spatial.point(0, 0)) NULLS FIRST", true, true)]
        [InlineData("FROM Users ORDER BY spatial.distance(Location, spatial.point(0, 0)) NULLS LAST", true, false)]
        [InlineData("FROM Users ORDER BY spatial.distance(Location, spatial.point(0, 0)) ASC NULLS FIRST", true, true)]
        [InlineData("FROM Users ORDER BY spatial.distance(Location, spatial.point(0, 0)) DESC NULLS LAST", false, false)]
        [InlineData("FROM Users ORDER BY spatial.distance(Location, spatial.wkt('POINT(0 0)')) DESC NULLS FIRST", false, true)]
        public void SpatialDistanceOrderByAcceptsNullsClause(string q, bool expectedAscending, bool expectedNullFirst)
        {
            var query = Parse(q);

            var clause = Assert.Single(query.OrderBy);
            var method = Assert.IsType<MethodExpression>(clause.Expression);
            Assert.Equal("spatial.distance", method.Name.Value);
            Assert.Equal(expectedAscending, clause.Ascending);
            Assert.True(clause.NullFirst.HasValue);
            Assert.Equal(expectedNullFirst, clause.NullFirst.Value);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void MixedSpatialAndFieldOrderByCarryIndependentNullsValues()
        {
            var query = Parse("FROM Users ORDER BY spatial.distance(Location, spatial.point(0, 0)) NULLS LAST, Name DESC NULLS FIRST");

            Assert.Equal(2, query.OrderBy.Count);

            Assert.IsType<MethodExpression>(query.OrderBy[0].Expression);
            Assert.True(query.OrderBy[0].Ascending);
            Assert.False(query.OrderBy[0].NullFirst);

            Assert.IsType<FieldExpression>(query.OrderBy[1].Expression);
            Assert.False(query.OrderBy[1].Ascending);
            Assert.True(query.OrderBy[1].NullFirst);
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData("FROM Users ORDER BY Name NULLS FIRTS")]
        [InlineData("FROM Users ORDER BY Name NULLS")]
        [InlineData("FROM Users ORDER BY Name NULLS BLAH")]
        public void InvalidNullsKeywordThrows(string q)
        {
            Assert.Throws<QueryParser.ParseException>(() =>
            {
                var parser = new QueryParser();
                parser.Init(q);
                parser.Parse();
            });
        }

        private static Query Parse(string q)
        {
            var parser = new QueryParser();
            parser.Init(q);
            return parser.Parse();
        }
    }
}
