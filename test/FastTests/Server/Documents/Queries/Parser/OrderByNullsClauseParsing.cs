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
            Assert.NotEqual(NullsOrderingType.Implicit, clause.NullsOrdering);
            Assert.Equal(expectedNullFirst ? NullsOrderingType.First : NullsOrderingType.Last, clause.NullsOrdering);
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
            Assert.Equal(NullsOrderingType.Implicit, clause.NullsOrdering);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void MultipleClausesEachCarryTheirOwnNullsValue()
        {
            var query = Parse("FROM Users ORDER BY Name NULLS FIRST, Age DESC, Score AS double NULLS LAST");

            Assert.NotNull(query.OrderBy);
            Assert.Equal(3, query.OrderBy.Count);

            Assert.True(query.OrderBy[0].Ascending);
            Assert.Equal(NullsOrderingType.First, query.OrderBy[0].NullsOrdering);

            Assert.False(query.OrderBy[1].Ascending);
            Assert.Equal(NullsOrderingType.Implicit, query.OrderBy[1].NullsOrdering); // omitted clause -> Implicit

            Assert.Equal(OrderByFieldType.Double, query.OrderBy[2].FieldType);
            Assert.True(query.OrderBy[2].Ascending);
            Assert.Equal(NullsOrderingType.Last, query.OrderBy[2].NullsOrdering);
        }
        
        [RavenFact(RavenTestCategory.Querying)]
        public void MixedAscDescAndMixedNullsFirstLastInSameOrderBy()
        {
            var query = Parse("FROM Users ORDER BY Name DESC NULLS FIRST, Age ASC NULLS LAST");

            Assert.Equal(2, query.OrderBy.Count);

            Assert.False(query.OrderBy[0].Ascending);
            Assert.Equal(NullsOrderingType.First, query.OrderBy[0].NullsOrdering);

            Assert.True(query.OrderBy[1].Ascending);
            Assert.Equal(NullsOrderingType.Last, query.OrderBy[1].NullsOrdering);
        }
        
        [RavenFact(RavenTestCategory.Querying)]
        public void MethodOrderByAcceptsNullsClause()
        {
            var query = Parse("FROM Users ORDER BY id() AS string NULLS LAST");

            var clause = Assert.Single(query.OrderBy);
            Assert.IsType<MethodExpression>(clause.Expression);
            Assert.Equal(OrderByFieldType.String, clause.FieldType);
            Assert.Equal(NullsOrderingType.Last, clause.NullsOrdering);
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
            Assert.NotEqual(NullsOrderingType.Implicit, clause.NullsOrdering);
            Assert.Equal(expectedNullFirst ? NullsOrderingType.First : NullsOrderingType.Last, clause.NullsOrdering);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void MixedSpatialAndFieldOrderByCarryIndependentNullsValues()
        {
            var query = Parse("FROM Users ORDER BY spatial.distance(Location, spatial.point(0, 0)) NULLS LAST, Name DESC NULLS FIRST");

            Assert.Equal(2, query.OrderBy.Count);

            Assert.IsType<MethodExpression>(query.OrderBy[0].Expression);
            Assert.True(query.OrderBy[0].Ascending);
            Assert.Equal(NullsOrderingType.Last, query.OrderBy[0].NullsOrdering);

            Assert.IsType<FieldExpression>(query.OrderBy[1].Expression);
            Assert.False(query.OrderBy[1].Ascending);
            Assert.Equal(NullsOrderingType.First, query.OrderBy[1].NullsOrdering);
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData("FROM Users ORDER BY Name NULLS FIRTS")]      // typo of FIRST
        [InlineData("FROM Users ORDER BY Name NULLS LASTS")]      // typo of LAST
        [InlineData("FROM Users ORDER BY Name NULLS FIR")]        // partial FIRST
        [InlineData("FROM Users ORDER BY Name NULLS LAS")]        // partial LAST
        [InlineData("FROM Users ORDER BY Name NULLS")]            // missing keyword
        [InlineData("FROM Users ORDER BY Name NULLS BLAH")]       // random keyword
        [InlineData("FROM Users ORDER BY Name NULLS 1")]          // numeric
        [InlineData("FROM Users ORDER BY Name NULLS FIRSTLAST")]  // concatenated
        [InlineData("FROM Users ORDER BY Name NULLS DEFAULT")]    // not a valid clause
        [InlineData("FROM Users ORDER BY Name NULLS ASC")]        // direction keyword
        public void InvalidNullsKeywordThrowsWithMeaningfulMessage(string q)
        {
            var ex = Assert.Throws<QueryParser.ParseException>(() =>
            {
                var parser = new QueryParser();
                parser.Init(q);
                parser.Parse();
            });

            Assert.Contains("Expected FIRST or LAST after NULLS", ex.Message);
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData("FROM Users ORDER BY Name NULLS FIRST", NullsOrderingType.First)]
        [InlineData("FROM Users ORDER BY Name NULLS LAST", NullsOrderingType.Last)]
        [InlineData("FROM Users ORDER BY Name NULLS first", NullsOrderingType.First)]
        [InlineData("FROM Users ORDER BY Name NULLS last", NullsOrderingType.Last)]
        [InlineData("FROM Users ORDER BY Name NULLS FiRsT", NullsOrderingType.First)]
        [InlineData("FROM Users ORDER BY Name NULLS lAsT", NullsOrderingType.Last)]
        public void NullsKeywordMapsExactlyToFirstOrLast(string q, NullsOrderingType expected)
        {
            var query = Parse(q);
            var clause = Assert.Single(query.OrderBy);
            Assert.Equal(expected, clause.NullsOrdering);
        }

        private static Query Parse(string q)
        {
            var parser = new QueryParser();
            parser.Init(q);
            return parser.Parse();
        }
    }
}
