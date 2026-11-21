using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Timings;
using Raven.Server.Config;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27571 : RavenTestBase
    {
        private readonly ITestOutputHelper _output;

        public RavenDB_27571(ITestOutputHelper output) : base(output)
        {
            _output = output;
        }

        private class Coin
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public string EntityType { get; set; }
            public DateTime CreatedAt { get; set; }
            public decimal Amount { get; set; }
        }

        private class CoinIndex : AbstractIndexCreationTask<Coin>
        {
            public CoinIndex()
            {
                Map = coins => from coin in coins
                               select new
                               {
                                   coin.CustomerId,
                                   coin.EntityType,
                                   coin.CreatedAt,
                                   coin.Amount
                               };
            }
        }

        // What the .NET LINQ provider emits today.
        private const string ClientShape = "((CustomerId in ($p0) and CreatedAt >= $p1) and CreatedAt < $p2) and EntityType in ($p3)";

        // Same tree as the client shape after the parser's precedence rewrite.
        private const string ProposedShape = "(CustomerId in ($p0) and CreatedAt >= $p1 and CreatedAt < $p2) and EntityType in ($p3)";

        // Fully flat, no parentheses at all, range pair in the middle.
        private const string FlatShape = "CustomerId in ($p0) and CreatedAt >= $p1 and CreatedAt < $p2 and EntityType in ($p3)";

        // Range pair explicitly grouped.
        private const string GroupedShape = "CustomerId in ($p0) and (CreatedAt >= $p1 and CreatedAt < $p2) and EntityType in ($p3)";

        // Range pair first, no parentheses at all, four clauses.
        private const string RangeFirstShape = "CreatedAt >= $p1 and CreatedAt < $p2 and CustomerId in ($p0) and EntityType in ($p3)";

        // Range pair with reversed directions, separated by other clauses.
        private const string ReversedSeparatedShape = "EntityType in ($p3) and CreatedAt < $p2 and CustomerId in ($p0) and CreatedAt >= $p1";

        // Parenthesized sub-chains, each holding one half of the pair.
        private const string SplitAcrossGroupsShape = "(CreatedAt >= $p1 and CustomerId in ($p0)) and (EntityType in ($p3) and CreatedAt < $p2)";

        // A duplicated same-direction clause next to the pair.
        private const string DuplicateDirectionShape = "CreatedAt >= $p1 and CreatedAt >= $p1 and CreatedAt < $p2 and CustomerId in ($p0) and EntityType in ($p3)";

        // A negated operand inside the chain.
        private const string NegatedOperandShape = "CustomerId in ($p0) and CreatedAt >= $p1 and not EntityType = 'Withdrawal' and CreatedAt < $p2";

        // The chain sits under an 'or'.
        private const string UnderOrShape = "CustomerId in ($p0) and CreatedAt >= $p1 and CreatedAt < $p2 and EntityType in ($p3) or CustomerId = 'customers/none'";

        // The pair sits inside exact(), which wraps a where clause of its own.
        private const string InsideExactShape = "CustomerId in ($p0) and exact(CreatedAt >= $p1 and CreatedAt < $p2) and EntityType in ($p3)";

        // The pair sits inside when(), which wraps a where clause of its own.
        private const string InsideWhenShape = "CustomerId in ($p0) and when($p1 != null, CreatedAt >= $p1 and CreatedAt < $p2) and EntityType in ($p3)";

        // The pair sits inside boost(), nested the way the LINQ provider nests it.
        private const string InsideBoostShape = "CustomerId in ($p0) and boost((EntityType in ($p3) and CreatedAt >= $p1) and CreatedAt < $p2, 2)";

        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData(ClientShape, "AND[ AND[ CustomerId in ($p0) , CreatedAt between $p1 and $p2 ] , EntityType in ($p3) ]")]
        [InlineData(ProposedShape, "AND[ AND[ CustomerId in ($p0) , CreatedAt between $p1 and $p2 ] , EntityType in ($p3) ]")]
        [InlineData(FlatShape, "AND[ AND[ CustomerId in ($p0) , CreatedAt between $p1 and $p2 ] , EntityType in ($p3) ]")]
        [InlineData(GroupedShape, "AND[ AND[ CustomerId in ($p0) , CreatedAt between $p1 and $p2 ] , EntityType in ($p3) ]")]
        [InlineData(RangeFirstShape, "AND[ AND[ CreatedAt between $p1 and $p2 , CustomerId in ($p0) ] , EntityType in ($p3) ]")]
        [InlineData(ReversedSeparatedShape, "AND[ AND[ EntityType in ($p3) , CreatedAt between $p1 and $p2 ] , CustomerId in ($p0) ]")]
        [InlineData(SplitAcrossGroupsShape, "AND[ AND[ CreatedAt between $p1 and $p2 , CustomerId in ($p0) ] , EntityType in ($p3) ]")]
        // the second 'CreatedAt >= $p1' is the same bound twice and is dropped
        [InlineData(DuplicateDirectionShape, "AND[ AND[ CreatedAt between $p1 and $p2 , CustomerId in ($p0) ] , EntityType in ($p3) ]")]
        [InlineData(NegatedOperandShape, "AND[ AND[ CustomerId in ($p0) , CreatedAt between $p1 and $p2 ] , NOT[ EntityType = 'Withdrawal' ] ]")]
        [InlineData(InsideExactShape, "AND[ AND[ CustomerId in ($p0) , exact( CreatedAt between $p1 and $p2 ) ] , EntityType in ($p3) ]")]
        [InlineData(InsideBoostShape, "AND[ CustomerId in ($p0) , boost( AND[ EntityType in ($p3) , CreatedAt between $p1 and $p2 ] , 2 ) ]")]
        public void FoldRewritesAnyAndChainIntoBetween(string where, string expectedTree)
        {
            var query = Parse(where);
            var before = Dump(query.Where);

            Assert.True(WhereClauseNormalizer.TryNormalize(query.Where, out var rewritten));

            var after = Dump(rewritten);
            _output.WriteLine($"RQL    : {where}");
            _output.WriteLine($"BEFORE : {before}");
            _output.WriteLine($"AFTER  : {after}");

            Assert.Equal(expectedTree, after);

            // the input tree is never mutated, the caller decides what to do with the rewrite
            Assert.Equal(before, Dump(query.Where));

            // normalizing is idempotent
            Assert.False(WhereClauseNormalizer.TryNormalize(rewritten, out var again));
            Assert.Same(rewritten, again);
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData("CreatedAt > $p1 and CreatedAt > $p2 and CustomerId in ($p0)")]
        [InlineData("CreatedAt < $p1 and Amount < $p2")]
        [InlineData("CreatedAt > $p1 and CustomerId = 'customers/1'")]
        [InlineData("not (CreatedAt > $p1) and CreatedAt < $p2")]
        [InlineData("CreatedAt > $p1 and CreatedAt < Amount")]
        [InlineData("CustomerId > 'a' and CustomerId > 'b'")]
        // a long and a double bound query different terms of the field, so neither can replace the other
        [InlineData("Amount > 1 and Amount > 1.5")]
        [InlineData("Amount > 3 and Amount > 2.5")]
        [InlineData("CustomerId = 'a' or CustomerId = 'b'")]
        [InlineData("CustomerId = 'a'")]
        public void NormalizerLeavesChainsWithNothingToRewriteAlone(string where)
        {
            var query = Parse(where);

            Assert.False(WhereClauseNormalizer.TryNormalize(query.Where, out var rewritten));
            Assert.Same(query.Where, rewritten);
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData("CustomerId = 'x' or (Amount > 1 and Amount < 5)", "OR[ CustomerId = 'x' , Amount between 1 and 5 ]")]
        [InlineData("CustomerId = 'x' and not (Amount > 1 and Amount < 5)", "AND[ CustomerId = 'x' , NOT[ Amount between 1 and 5 ] ]")]
        public void NormalizerReachesIntoOrAndNotGroups(string where, string expectedTree)
        {
            var query = Parse(where);

            Assert.True(WhereClauseNormalizer.TryNormalize(query.Where, out var rewritten));

            var after = Dump(rewritten);
            _output.WriteLine($"RQL    : {where}");
            _output.WriteLine($"AFTER  : {after}");

            Assert.Equal(expectedTree, after);
            Assert.False(WhereClauseNormalizer.TryNormalize(rewritten, out _));
        }

        [RavenTheory(RavenTestCategory.Querying)]
        // the very same bound twice is dropped, whatever the parameter turns out to be
        [InlineData("Amount > $a and Amount > $a and CustomerId = 'x'", "AND[ Amount > $a , CustomerId = 'x' ]")]
        [InlineData("Amount >= 1 and CustomerId = 'x' and Amount >= 1", "AND[ Amount >= 1 , CustomerId = 'x' ]")]
        // a different operator or a different value is a different bound
        [InlineData("Amount > $a and Amount >= $a and Amount < 5", "AND[ Amount between $a and 5 , Amount >= $a ]")]
        public void NormalizerDropsDuplicateBounds(string where, string expectedTree)
        {
            var query = Parse(where);

            Assert.True(WhereClauseNormalizer.TryNormalize(query.Where, out var rewritten));

            var after = Dump(rewritten);
            _output.WriteLine($"RQL    : {where}");
            _output.WriteLine($"AFTER  : {after}");

            Assert.Equal(expectedTree, after);
            Assert.False(WhereClauseNormalizer.TryNormalize(rewritten, out _));
        }

        [RavenTheory(RavenTestCategory.Querying)]
        // lower bounds keep the greater value, upper bounds the lesser
        [InlineData("Amount > 1 and CustomerId = 'x' and Amount > 2", "AND[ Amount > 2 , CustomerId = 'x' ]")]
        [InlineData("Amount < 5 and Amount < 7", "Amount < 5")]
        // on a tie the strict operator wins
        [InlineData("Amount >= 2 and Amount > 2", "Amount > 2")]
        [InlineData("Amount <= 2 and Amount < 2", "Amount < 2")]
        // two doubles compare as doubles
        [InlineData("Amount > 1.5 and Amount > 2.5", "Amount > 2.5")]
        // and the survivor still pairs with the opposite bound
        [InlineData("Amount > 1 and Amount > 2 and Amount < 5", "Amount between 2 and 5")]
        public void NormalizerKeepsTheTighterLiteralBound(string where, string expectedTree)
        {
            var query = Parse(where);

            Assert.True(WhereClauseNormalizer.TryNormalize(query.Where, out var rewritten));

            var after = Dump(rewritten);
            _output.WriteLine($"RQL    : {where}");
            _output.WriteLine($"AFTER  : {after}");

            Assert.Equal(expectedTree, after);
            Assert.False(WhereClauseNormalizer.TryNormalize(rewritten, out _));
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void MergedLiteralBoundsKeepTheirInclusivity()
        {
            var query = Parse("(Amount > 1 and Amount >= 2) and (Amount <= 5 and Amount < 7)");

            Assert.True(WhereClauseNormalizer.TryNormalize(query.Where, out var rewritten));

            var between = Assert.IsType<BetweenExpression>(rewritten);
            Assert.Equal("2", between.Min.Token.Value);
            Assert.Equal("5", between.Max.Token.Value);
            Assert.True(between.MinInclusive);
            Assert.True(between.MaxInclusive);
        }

        [RavenTheory(RavenTestCategory.Querying)]
        // parameter bounds cannot be compared here, they are grouped next to each other for the query builders
        [InlineData("Amount > $a and CustomerId = 'x' and Amount > $b", "AND[ AND[ Amount > $a , Amount > $b ] , CustomerId = 'x' ]")]
        [InlineData("Amount > $a and CustomerId = 'x' and Amount > $b and EntityType = 'y' and Amount >= $c", "AND[ AND[ AND[ AND[ Amount > $a , Amount > $b ] , Amount >= $c ] , CustomerId = 'x' ] , EntityType = 'y' ]")]
        // a long and a double literal are grouped the same way, and the builders keep both
        [InlineData("Amount > 1 and CustomerId = 'x' and Amount > 1.5", "AND[ AND[ Amount > 1 , Amount > 1.5 ] , CustomerId = 'x' ]")]
        // opposite directions pair into a between first, and the bound left over is placed next to it for the builders
        [InlineData("Amount > $a and CustomerId = 'x' and Amount < $b and Amount > $c", "AND[ AND[ Amount between $a and $b , Amount > $c ] , CustomerId = 'x' ]")]
        [InlineData("Amount > $a and Amount < $b and CustomerId = 'x' and Amount > $c and Amount < $d", "AND[ AND[ Amount between $a and $b , Amount between $c and $d ] , CustomerId = 'x' ]")]
        [InlineData("Amount > $a and Amount < $b and CustomerId = 'x' and Amount > $c and Amount > $d", "AND[ AND[ AND[ Amount between $a and $b , Amount > $c ] , Amount > $d ] , CustomerId = 'x' ]")]
        public void NormalizerGroupsSameDirectionBoundsItCannotCompare(string where, string expectedTree)
        {
            var query = Parse(where);

            Assert.True(WhereClauseNormalizer.TryNormalize(query.Where, out var rewritten));

            var after = Dump(rewritten);
            _output.WriteLine($"RQL    : {where}");
            _output.WriteLine($"AFTER  : {after}");

            Assert.Equal(expectedTree, after);
            Assert.False(WhereClauseNormalizer.TryNormalize(rewritten, out _));
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        // lower bounds keep the greater value
        [RavenData("Amount > $a and Amount > $b", 3, 6, "coins/1,coins/3,coins/4,coins/5", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Amount > $a and Amount > $b", 6, 3, "coins/1,coins/3,coins/4,coins/5", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Amount > $a and CustomerId in ($p0) and Amount >= $b", 3, 7, "coins/1,coins/3,coins/4,coins/5", SearchEngineMode = RavenSearchEngineMode.All)]
        // upper bounds keep the lesser value
        [RavenData("Amount < $a and Amount < $b", 8, 6, "coins/2,coins/6,coins/7", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Amount < $a and Amount < $b", 6, 8, "coins/2,coins/6,coins/7", SearchEngineMode = RavenSearchEngineMode.All)]
        // two doubles compare as doubles
        [RavenData("Amount > $a and Amount > $b", 3.5, 6.5, "coins/1,coins/3,coins/4,coins/5", SearchEngineMode = RavenSearchEngineMode.All)]
        // three bounds on one field
        [RavenData("Amount > $a and Amount > $b and Amount > 4", 3, 6, "coins/1,coins/3,coins/4,coins/5", SearchEngineMode = RavenSearchEngineMode.All)]
        // a bound left over after the pair folded tightens the side of the between it belongs to
        [RavenData("Amount > $a and Amount < 50 and Amount > $b", 3, 6, "coins/1,coins/3", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Amount > $a and CustomerId in ($p0) and Amount < 50 and Amount > $b", 6, 3, "coins/1,coins/3", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Amount > $a and Amount < $b and Amount < 8", 3, 50, "coins/2,coins/3", SearchEngineMode = RavenSearchEngineMode.All)]
        // two pairs on one field fold into two betweens, which tighten into one
        [RavenData("Amount >= $a and Amount < 50 and Amount > $b and Amount <= 1000", 3, 6, "coins/1,coins/3", SearchEngineMode = RavenSearchEngineMode.All)]
        public void TheTighterOfSameDirectionParameterBoundsWins(Options options, string where, object a, object b, string expectedIds)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var (results, rangeScansOnAmount) = QueryWithTwoBounds(session, where, a, b);

                    Assert.Equal(expectedIds.Split(','), results);

                    if (options.SearchEngineMode == RavenSearchEngineMode.Corax)
                        Assert.Equal(1, rangeScansOnAmount);
                }
            }
        }

        // a long bound queries the long term of the field and a double bound its double term, so neither can replace the
        // other and both are applied; the probes below show where the two disagree
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("Amount > $a and Amount > $b", 3, 6.5, "coins/1,coins/3,coins/4,coins/5", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Amount > $a and Amount > $b", 6.5, 3, "coins/1,coins/3,coins/4,coins/5", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Amount < $a and CustomerId in ($p0) and Amount < $b", 8, 6.5, "coins/2,coins/7", SearchEngineMode = RavenSearchEngineMode.All)]
        public void SameDirectionBoundsOfDifferentKindsAreBothApplied(Options options, string where, object a, object b, string expectedIds)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var (results, rangeScansOnAmount) = QueryWithTwoBounds(session, where, a, b);

                    Assert.Equal(expectedIds.Split(','), results);

                    if (options.SearchEngineMode == RavenSearchEngineMode.Corax)
                        Assert.Equal(2, rangeScansOnAmount);
                }
            }
        }

        private (string[] Ids, int RangeScansOnAmount) QueryWithTwoBounds(Raven.Client.Documents.Session.IDocumentSession session, string where, object a, object b)
        {
            var results = session.Advanced
                .RawQuery<Coin>($"from index 'CoinIndex' where {where} include timings()")
                .AddParameter("a", a)
                .AddParameter("b", b)
                .AddParameter("p0", new[] { "customers/1", "customers/2" })
                .Timings(out QueryTimings timings)
                .ToList();

            var rangeScans = new List<QueryInspectionNode>();
            if (timings.QueryPlan is QueryInspectionNode plan)
            {
                _output.WriteLine($"RQL  : {where} (a={a}, b={b})");
                _output.WriteLine("PLAN :");
                Print(plan, 1);

                Collect(plan, rangeScans, "Amount");
            }

            return (results.Select(x => x.Id).OrderBy(x => x).ToArray(), rangeScans.Count);
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void DateParameterBoundsCompareAsDates(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    // the later lower bound wins: coins/1 sits on From and is excluded, coins/2 is ten days in and stays
                    var results = session.Advanced
                        .RawQuery<Coin>("from index 'CoinIndex' where CreatedAt >= $p1 and CustomerId in ($p0) and CreatedAt >= $later")
                        .AddParameter("p0", new[] { "customers/1", "customers/2" })
                        .AddParameter("p1", From)
                        .AddParameter("later", From.AddDays(5))
                        .ToList();

                    Assert.Equal(new[] { "coins/2", "coins/3", "coins/4" }, results.Select(x => x.Id).OrderBy(x => x).ToArray());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void StringBoundsThatAreNotDatesAreLeftAlone(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    // two string bounds, no date in sight: nothing is merged and both still apply
                    var results = session.Advanced
                        .RawQuery<Coin>("from index 'CoinIndex' where CustomerId > $a and CustomerId > $b")
                        .AddParameter("a", "customers/1")
                        .AddParameter("b", "customers/2")
                        .ToList();

                    Assert.Equal(new[] { "coins/6" }, results.Select(x => x.Id).ToArray());
                }
            }
        }

        // a between assembled from two comparisons may mix a literal with a parameter or a long with a double; the builders
        // translate such a between as the two comparisons it came from
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("Amount > $a and Amount < 8", 6, "coins/3", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Amount < 8 and Amount > 6.5", 0, "coins/3", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Amount >= 5 and Amount < $a", 7.5, "coins/2,coins/3", SearchEngineMode = RavenSearchEngineMode.All)]
        public void BoundsOfMixedValueKindsStillFold(Options options, string where, object a, string expectedIds)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced
                        .RawQuery<Coin>($"from index 'CoinIndex' where {where}")
                        .AddParameter("a", a)
                        .ToList();

                    Assert.Equal(expectedIds.Split(','), results.Select(x => x.Id).OrderBy(x => x).ToArray());
                }
            }

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var parameters = context.ReadObject(new DynamicJsonValue { ["a"] = a }, "parameters");
                var metadata = new QueryMetadata($"from index 'CoinIndex' where {where}", parameters, cacheKey: 1);

                Assert.IsType<BetweenExpression>(metadata.Query.Where);
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void QueryMetadataCarriesTheNormalizedWhereClause()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var parameters = context.ReadObject(new DynamicJsonValue
                {
                    ["p0"] = new DynamicJsonArray { "customers/1" },
                    ["p1"] = From,
                    ["p2"] = To,
                    ["p3"] = new DynamicJsonArray { "Deposit" }
                }, "parameters");

                var metadata = new QueryMetadata($"from index 'CoinIndex' where {ClientShape}", parameters, cacheKey: 1);

                Assert.Equal("AND[ AND[ CustomerId in ($p0) , CreatedAt between $p1 and $p2 ] , EntityType in ($p3) ]", Dump(metadata.Query.Where));
                Assert.Equal($"from index 'CoinIndex' where {ClientShape}", metadata.QueryText);
            }
        }

        // a folded between prints as the comparisons it came from; a sharded query is re-rendered from the cached tree and
        // each shard parses what the user wrote and folds it again
        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData("CreatedAt between $p1 and $p2", "CreatedAt BETWEEN $p1 AND $p2")]
        [InlineData("CreatedAt >= $p1 and CreatedAt <= $p2", "(CreatedAt >= $p1 AND CreatedAt <= $p2)")]
        [InlineData("Amount >= 1 and Amount <= 2.5", "(Amount >= 1 AND Amount <= 2.5)")]
        [InlineData("CreatedAt >= $p1 and CreatedAt < $p2", "(CreatedAt >= $p1 AND CreatedAt < $p2)")]
        [InlineData("CreatedAt > $p1 and CreatedAt <= $p2", "(CreatedAt > $p1 AND CreatedAt <= $p2)")]
        [InlineData("CustomerId = 'x' and CreatedAt > $p1 and CreatedAt < $p2", "(CustomerId = 'x' AND (CreatedAt > $p1 AND CreatedAt < $p2))")]
        [InlineData("CustomerId = 'x' or (Amount > 1 and Amount < 5)", "(CustomerId = 'x' OR (Amount > 1 AND Amount < 5))")]
        [InlineData("CustomerId = 'x' and not (Amount > 1 and Amount < 5)", "(CustomerId = 'x' AND NOT ((Amount > 1 AND Amount < 5)))")]
        public void FoldedBetweenPrintsAsTheComparisonsItCameFrom(string where, string expectedText)
        {
            var normalized = WhereClauseNormalizer.Normalize(Parse(where).Where);

            var sb = new StringBuilder();
            new StringQueryVisitor(sb).VisitExpression(normalized);
            var printed = sb.ToString();

            _output.WriteLine($"RQL     : {where}");
            _output.WriteLine($"PRINTED : {printed}");

            Assert.Equal(expectedText, printed);

            // parsed again, the text folds to the very same tree
            Assert.Equal(Dump(normalized), Dump(WhereClauseNormalizer.Normalize(Parse(printed).Where)));
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData("CreatedAt >= $p1 and CreatedAt <= $p2", true, true)]
        [InlineData("CreatedAt >= $p1 and CreatedAt < $p2", true, false)]
        [InlineData("CreatedAt > $p1 and CreatedAt <= $p2", false, true)]
        [InlineData("CreatedAt > $p1 and CreatedAt < $p2", false, false)]
        [InlineData("CreatedAt < $p2 and CreatedAt >= $p1", true, false)]
        [InlineData("CreatedAt <= $p2 and CreatedAt > $p1", false, true)]
        public void FoldKeepsBoundInclusivity(string where, bool minInclusive, bool maxInclusive)
        {
            var query = Parse(where);

            Assert.True(WhereClauseNormalizer.TryNormalize(query.Where, out var rewritten));

            var between = Assert.IsType<BetweenExpression>(rewritten);
            Assert.True(between.Folded);
            Assert.Equal("p1", between.Min.Token.Value);
            Assert.Equal("p2", between.Max.Token.Value);
            Assert.Equal(minInclusive, between.MinInclusive);
            Assert.Equal(maxInclusive, between.MaxInclusive);
        }

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(ClientShape, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(ProposedShape, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(FlatShape, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(GroupedShape, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(RangeFirstShape, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(ReversedSeparatedShape, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(SplitAcrossGroupsShape, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(NegatedOperandShape, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(UnderOrShape, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(InsideExactShape, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(InsideWhenShape, SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void CoraxRunsOneBoundedScanOnCreatedAtRegardlessOfShape(Options options, string where)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var results = Query(session, where, out var timings);

                    var plan = (QueryInspectionNode)timings.QueryPlan;
                    _output.WriteLine($"RQL  : {where}");
                    _output.WriteLine("PLAN :");
                    Print(plan, 1);

                    var rangeScans = new List<QueryInspectionNode>();
                    Collect(plan, rangeScans);

                    var scan = Assert.Single(rangeScans);
                    Assert.False(string.IsNullOrEmpty(scan.Parameters["LowValue"]));
                    Assert.False(string.IsNullOrEmpty(scan.Parameters["HighValue"]));
                    Assert.Equal("Inclusive", scan.Parameters["LowOption"]);
                    Assert.Equal("Exclusive", scan.Parameters["HighOption"]);

                    Assert.Equal(ExpectedIds, results.Select(x => x.Id).OrderBy(x => x).ToArray());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(ClientShape, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(FlatShape, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(GroupedShape, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(RangeFirstShape, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(ReversedSeparatedShape, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(SplitAcrossGroupsShape, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(DuplicateDirectionShape, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(NegatedOperandShape, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(UnderOrShape, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(InsideExactShape, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(InsideBoostShape, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(InsideWhenShape, SearchEngineMode = RavenSearchEngineMode.All)]
        public void FoldedQueriesKeepHalfOpenRangeSemantics(Options options, string where)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var results = Query(session, where, out _);

                    Assert.Equal(ExpectedIds, results.Select(x => x.Id).OrderBy(x => x).ToArray());
                }
            }
        }

        // 'not (...)' around a chain the normalizer rewrites: without its parentheses the builders read it as '(not A) and B'
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("EntityType = 'Deposit' and not (Amount > 3 and Amount < 8 and CustomerId = 'customers/1')", "coins/1,coins/3,coins/4,coins/5,coins/6", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("EntityType = 'Deposit' and not (Amount > 3 and Amount > 3 and CustomerId = 'customers/1')", "coins/3,coins/4,coins/5,coins/6", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("EntityType = 'Deposit' and not (Amount > 3 and Amount > 6 and CustomerId = 'customers/1')", "coins/2,coins/3,coins/4,coins/5,coins/6", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("EntityType = 'Deposit' and not (Amount > $a and CustomerId = 'customers/1' and Amount > $b)", "coins/2,coins/3,coins/4,coins/5,coins/6", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("EntityType = 'Withdrawal' or not (Amount > 3 and Amount < 8 and CustomerId = 'customers/1')", "coins/1,coins/3,coins/4,coins/5,coins/6,coins/7", SearchEngineMode = RavenSearchEngineMode.All)]
        public void NegatedChainKeepsItsParentheses(Options options, string where, string expectedIds)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced
                        .RawQuery<Coin>($"from index 'CoinIndex' where {where}")
                        .AddParameter("a", 3)
                        .AddParameter("b", 6)
                        .ToList();

                    Assert.Equal(expectedIds.Split(','), results.Select(x => x.Id).OrderBy(x => x).ToArray());
                }
            }
        }

        // when the score decides the order every clause must score as it did, so only a pair is folded and nothing moves
        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData("(CustomerId = 'x' and Amount >= 1) and Amount < 5", "AND[ AND[ CustomerId = 'x' , Amount >= 1 ] , Amount < 5 ]")]
        [InlineData("CustomerId = 'x' and (Amount >= 1 and Amount < 5)", "AND[ CustomerId = 'x' , Amount between 1 and 5 ]")]
        [InlineData("Amount > 1 and CustomerId = 'x' and Amount > 2", "AND[ AND[ Amount > 1 , CustomerId = 'x' ] , Amount > 2 ]")]
        [InlineData("CustomerId = 'x' or (Amount > 3 and (Amount >= 1 and Amount < 5))", "OR[ CustomerId = 'x' , AND[ Amount > 3 , Amount between 1 and 5 ] ]")]
        public void QueryOrderedByScoreFoldsOnlyAPair(string where, string expectedTree)
        {
            var metadata = new QueryMetadata($"from index 'CoinIndex' where {where} order by score()", null, cacheKey: 1);

            Assert.True(metadata.ScoresMayOrderResults(index: null));
            Assert.Equal(expectedTree, Dump(metadata.WhereFor(index: null)));

            var unordered = new QueryMetadata($"from index 'CoinIndex' where {where}", null, cacheKey: 1);
            Assert.False(unordered.ScoresMayOrderResults(index: null));
            Assert.Same(unordered.Query.Where, unordered.WhereFor(index: null));
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void PairWrittenWithAndWithoutTheAliasIsOnePair()
        {
            var metadata = new QueryMetadata("from index 'CoinIndex' as c where c.CustomerId = 'x' and (c.Amount >= 1 and Amount < 5)", null, cacheKey: 1);
            Assert.Equal(1, CountNodes(metadata.Query.Where, x => x is BetweenExpression { FoldedFromSiblings: true }));

            var nested = new QueryMetadata("from index 'CoinIndex' as c where c.CustomerId = 'x' and (c.Origin.X >= 1 and c.Destination.X < 5)", null, cacheKey: 1);
            Assert.Equal(0, CountNodes(nested.Query.Where, x => x is BetweenExpression));
        }

        // 'or' mixed into the chain. The fold must never pair range clauses across an 'or' boundary.

        // Both range clauses sit inside one 'or': a disjunction, not a range.
        private const string OrBetweenRangeClauses = "CustomerId in ($p0) and (CreatedAt >= $p1 or CreatedAt < $p2)";

        // Each side of the 'or' carries one half of the pair.
        private const string OrSplitsThePair = "(CustomerId in ($p0) and CreatedAt >= $p1) or (EntityType in ($p3) and CreatedAt < $p2)";

        // An 'or' group sits between the two halves of the pair, as an opaque operand of the 'and' chain.
        private const string OrOperandBetweenPair = "CreatedAt >= $p1 and (CustomerId in ($p0) or EntityType in ($p3)) and CreatedAt < $p2";

        // The lower bound is trapped inside an 'or' group, the upper bound is outside.
        private const string LowerBoundInsideOr = "(CreatedAt >= $p1 or CustomerId = 'customers/3') and CreatedAt < $p2 and EntityType in ($p3)";

        // The upper bound is trapped inside an 'or' group, the lower bound is outside.
        private const string UpperBoundInsideOr = "CreatedAt >= $p1 and (CustomerId in ($p0) or CreatedAt < $p2)";

        // 'and' chain with a pair, followed by 'or' and another 'and' chain, no parentheses.
        private const string PairThenOrChain = "CustomerId in ($p0) and CreatedAt >= $p1 and CreatedAt < $p2 or EntityType = 'Withdrawal' and CustomerId = 'customers/3'";

        // Two 'and' chains under one 'or', each with its own pair.
        private const string PairOnBothSidesOfOr = "(CreatedAt >= $p1 and CustomerId = 'customers/1' and CreatedAt < $p2) or (CreatedAt >= $p1 and CustomerId = 'customers/2' and CreatedAt < $p2)";

        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData(OrBetweenRangeClauses, 0, 2)]
        [InlineData(OrSplitsThePair, 0, 2)]
        [InlineData(OrOperandBetweenPair, 1, 0)]
        [InlineData(LowerBoundInsideOr, 0, 2)]
        [InlineData(UpperBoundInsideOr, 0, 2)]
        [InlineData(PairThenOrChain, 1, 0)]
        [InlineData(PairOnBothSidesOfOr, 2, 0)]
        public void FoldNeverCrossesAnOr(string where, int expectedBetweens, int expectedLooseRangeClauses)
        {
            var query = Parse(where);
            var before = Dump(query.Where);

            var folded = WhereClauseNormalizer.Normalize(query.Where);
            var after = Dump(folded);

            _output.WriteLine($"RQL    : {where}");
            _output.WriteLine($"BEFORE : {before}");
            _output.WriteLine($"AFTER  : {after}");

            Assert.Equal(expectedBetweens, CountNodes(folded, e => e is BetweenExpression));
            Assert.Equal(expectedLooseRangeClauses, CountNodes(folded, e => e is BinaryExpression { IsRangeOperation: true }));
            Assert.Equal(before, Dump(query.Where));
        }

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(OrBetweenRangeClauses, 2, "coins/1,coins/2,coins/3,coins/4,coins/5,coins/7", SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(OrSplitsThePair, 2, "coins/1,coins/2,coins/3,coins/4,coins/5,coins/6,coins/7", SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(OrOperandBetweenPair, 1, "coins/1,coins/2,coins/3,coins/6,coins/7", SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(LowerBoundInsideOr, 2, "coins/1,coins/2,coins/3,coins/6", SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(UpperBoundInsideOr, 2, "coins/1,coins/2,coins/3,coins/4,coins/6,coins/7", SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData(PairThenOrChain, 1, "coins/1,coins/2,coins/3,coins/7", SearchEngineMode = RavenSearchEngineMode.Corax)]
        // both sides fold (see FoldNeverCrossesAnOr), but Corax then applies each between as a unary filter over the
        // tiny CustomerId term set instead of a range scan, so no range provider shows up in the plan
        [RavenData(PairOnBothSidesOfOr, 0, "coins/1,coins/2,coins/3,coins/7", SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void CoraxPlanWithOrMixedIntoTheChain(Options options, string where, int expectedRangeScansOnCreatedAt, string expectedIds)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var results = Query(session, where, out var timings);

                    var plan = (QueryInspectionNode)timings.QueryPlan;
                    _output.WriteLine($"RQL  : {where}");
                    _output.WriteLine("PLAN :");
                    Print(plan, 1);

                    var rangeScans = new List<QueryInspectionNode>();
                    Collect(plan, rangeScans);

                    Assert.Equal(expectedRangeScansOnCreatedAt, rangeScans.Count);
                    Assert.Equal(expectedIds.Split(','), results.Select(x => x.Id).OrderBy(x => x).ToArray());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(OrBetweenRangeClauses, "coins/1,coins/2,coins/3,coins/4,coins/5,coins/7", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(OrSplitsThePair, "coins/1,coins/2,coins/3,coins/4,coins/5,coins/6,coins/7", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(OrOperandBetweenPair, "coins/1,coins/2,coins/3,coins/6,coins/7", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(LowerBoundInsideOr, "coins/1,coins/2,coins/3,coins/6", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(UpperBoundInsideOr, "coins/1,coins/2,coins/3,coins/4,coins/6,coins/7", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(PairThenOrChain, "coins/1,coins/2,coins/3,coins/7", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(PairOnBothSidesOfOr, "coins/1,coins/2,coins/3,coins/7", SearchEngineMode = RavenSearchEngineMode.All)]
        public void OrMixedIntoTheChainKeepsItsSemantics(Options options, string where, string expectedIds)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var results = Query(session, where, out _);

                    Assert.Equal(expectedIds.Split(','), results.Select(x => x.Id).OrderBy(x => x).ToArray());
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData(true)]
        [InlineData(false)]
        public void DeepAndChainsDoNotOverflowTheStack(bool leftNested)
        {
            const int clauses = 20_000;

            var lower = Parse("CreatedAt >= $p1").Where;
            var upper = Parse("CreatedAt < $p2").Where;
            var filler = Parse("CustomerId = 'customers/1'").Where;

            // the same filler instance many times over is fine, the tree is only read
            var operands = new List<QueryExpression> { lower };
            for (var i = 0; i < clauses; i++)
                operands.Add(filler);
            operands.Add(upper);

            QueryExpression chain;
            if (leftNested)
            {
                chain = operands[0];
                for (var i = 1; i < operands.Count; i++)
                    chain = new BinaryExpression(chain, operands[i], OperatorType.And);
            }
            else
            {
                chain = operands[^1];
                for (var i = operands.Count - 2; i >= 0; i--)
                    chain = new BinaryExpression(operands[i], chain, OperatorType.And);
            }

            var folded = WhereClauseNormalizer.Normalize(chain);

            var (betweens, looseBounds, total) = CountIteratively(folded);
            Assert.Equal(1, betweens);
            Assert.Equal(0, looseBounds);
            Assert.Equal(clauses + 1, total);
        }

        private static (int Betweens, int LooseBounds, int Total) CountIteratively(QueryExpression root)
        {
            int betweens = 0, looseBounds = 0, total = 0;
            var pending = new Stack<QueryExpression>();
            pending.Push(root);

            while (pending.Count > 0)
            {
                var current = pending.Pop();
                switch (current)
                {
                    case BinaryExpression { Operator: OperatorType.And or OperatorType.Or } binary:
                        pending.Push(binary.Left);
                        pending.Push(binary.Right);
                        break;
                    case BinaryExpression { IsRangeOperation: true }:
                        looseBounds++;
                        total++;
                        break;
                    case BetweenExpression:
                        betweens++;
                        total++;
                        break;
                    default:
                        total++;
                        break;
                }
            }

            return (betweens, looseBounds, total);
        }

        private static int CountNodes(QueryExpression expression, Func<QueryExpression, bool> predicate)
        {
            var count = predicate(expression) ? 1 : 0;

            switch (expression)
            {
                case BinaryExpression be:
                    return count + CountNodes(be.Left, predicate) + CountNodes(be.Right, predicate);
                case NegatedExpression not:
                    return count + CountNodes(not.Expression, predicate);
                default:
                    return count;
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void LinqQueryWithHalfOpenRangeReturnsCorrectResults(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Seed(store);

                using (var session = store.OpenSession())
                {
                    var customerIds = new[] { "customers/1", "customers/2" };
                    var entityTypes = new[] { "Deposit" };

                    var query = session.Query<Coin, CoinIndex>()
                        .Where(x => x.CustomerId.In(customerIds)
                                    && x.CreatedAt >= From
                                    && x.CreatedAt < To
                                    && x.EntityType.In(entityTypes));

                    // the provider wraps every nested && in its own subclause, this is the shape the server has to fold
                    Assert.Equal($"from index 'CoinIndex' where {ClientShape}", query.ToString());

                    Assert.Equal(ExpectedIds, query.ToList().Select(x => x.Id).OrderBy(x => x).ToArray());
                }
            }
        }

        private static readonly DateTime From = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private static readonly DateTime To = new DateTime(2024, 2, 1, 0, 0, 0, DateTimeKind.Utc);

        // coins/1 sits on the lower bound (included), coins/3 just below the upper bound (included),
        // coins/4 sits on the upper bound (excluded by '<'), the rest are filtered by the other clauses.
        private static readonly string[] ExpectedIds = { "coins/1", "coins/2", "coins/3" };

        private void Seed(IDocumentStore store)
        {
            new CoinIndex().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Coin { Id = "coins/1", CustomerId = "customers/1", EntityType = "Deposit", CreatedAt = From, Amount = 10 });
                session.Store(new Coin { Id = "coins/2", CustomerId = "customers/1", EntityType = "Deposit", CreatedAt = From.AddDays(10), Amount = 5 });
                session.Store(new Coin { Id = "coins/3", CustomerId = "customers/2", EntityType = "Deposit", CreatedAt = To.AddTicks(-1), Amount = 7 });
                session.Store(new Coin { Id = "coins/4", CustomerId = "customers/2", EntityType = "Deposit", CreatedAt = To, Amount = 100 });
                session.Store(new Coin { Id = "coins/5", CustomerId = "customers/2", EntityType = "Deposit", CreatedAt = From.AddTicks(-1), Amount = 100 });
                session.Store(new Coin { Id = "coins/6", CustomerId = "customers/3", EntityType = "Deposit", CreatedAt = From.AddDays(1), Amount = 1 });
                session.Store(new Coin { Id = "coins/7", CustomerId = "customers/1", EntityType = "Withdrawal", CreatedAt = From.AddDays(1), Amount = 1 });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
        }

        private static List<Coin> Query(Raven.Client.Documents.Session.IDocumentSession session, string where, out QueryTimings timings)
        {
            return session.Advanced
                .RawQuery<Coin>($"from index 'CoinIndex' where {where} include timings()")
                .AddParameter("p0", new[] { "customers/1", "customers/2" })
                .AddParameter("p1", From)
                .AddParameter("p2", To)
                .AddParameter("p3", new[] { "Deposit" })
                .Timings(out timings)
                .ToList();
        }

        private static Query Parse(string where)
        {
            var parser = new QueryParser();
            parser.Init($"from index 'CoinIndex' where {where}");
            return parser.Parse(QueryType.Select);
        }

        private void Print(QueryInspectionNode node, int depth)
        {
            if (node == null)
                return;

            var parameters = node.Parameters == null
                ? string.Empty
                : string.Join(", ", node.Parameters
                    .Where(p => p.Key is "FieldName" or "LowValue" or "HighValue" or "LowOption" or "HighOption")
                    .Select(p => $"{p.Key}={p.Value}"));

            _output.WriteLine($"{new string(' ', depth * 2)}{node.Operation} {parameters}");

            foreach (var child in node.Children ?? new List<QueryInspectionNode>())
                Print(child, depth + 1);
        }

        private static void Collect(QueryInspectionNode node, List<QueryInspectionNode> rangeScansOnField, string fieldName = "CreatedAt")
        {
            if (node == null)
                return;

            if (node.Operation.Contains("RangeProvider") &&
                node.Parameters != null &&
                node.Parameters.TryGetValue("FieldName", out var field) &&
                field.Contains(fieldName))
            {
                rangeScansOnField.Add(node);
            }

            foreach (var child in node.Children ?? new List<QueryInspectionNode>())
                Collect(child, rangeScansOnField, fieldName);
        }

        private static string Dump(QueryExpression expression)
        {
            switch (expression)
            {
                case BinaryExpression { Operator: OperatorType.And } and:
                    return $"AND[ {Dump(and.Left)} , {Dump(and.Right)} ]";
                case BinaryExpression { Operator: OperatorType.Or } or:
                    return $"OR[ {Dump(or.Left)} , {Dump(or.Right)} ]";
                case NegatedExpression not:
                    return $"NOT[ {Dump(not.Expression)} ]";
                case BinaryExpression be:
                    return $"{Dump(be.Left)} {Operator(be.Operator)} {Dump(be.Right)}";
                case BetweenExpression between:
                    return $"{Dump(between.Source)} between {Dump(between.Min)} and {Dump(between.Max)}";
                case InExpression @in:
                    return $"{Dump(@in.Source)} in ({string.Join(", ", @in.Values.Select(Dump))})";
                case MethodExpression method:
                    return $"{method.Name}( {string.Join(" , ", method.Arguments.Select(Dump))} )";
                case FieldExpression field:
                    return field.FieldValue;
                case ValueExpression value:
                    return value.Value switch
                    {
                        ValueTokenType.Parameter => $"${value.Token.Value}",
                        ValueTokenType.String => $"'{value.Token.Value}'",
                        _ => value.Token.Value
                    };
                default:
                    return expression.ToString();
            }
        }

        private static string Operator(OperatorType type)
        {
            return type switch
            {
                OperatorType.Equal => "=",
                OperatorType.NotEqual => "!=",
                OperatorType.GreaterThan => ">",
                OperatorType.GreaterThanEqual => ">=",
                OperatorType.LessThan => "<",
                OperatorType.LessThanEqual => "<=",
                _ => type.ToString()
            };
        }

        // Shapes from the review rounds, each asserting what the unfolded chain returned before the fold existed. A "twin" writes
        // one bound as '(F < b or F < b)': the fold never descends into 'or', so the twin stays unfolded.

        private class Item
        {
            public string Id { get; set; }
            public string Tag { get; set; }
            public DateTime At { get; set; }
            public double Price { get; set; }
            public long[] Values { get; set; }
            public string Name { get; set; }
        }

        private class Items_WithSeveralTerms : AbstractIndexCreationTask<Item>
        {
            public Items_WithSeveralTerms()
            {
                Map = items => from item in items
                               select new
                               {
                                   item.Tag,
                                   Words = item.Name,
                                   _ = item.Values.Select(value => CreateField("Sizes", value))
                               };

                Index("Words", FieldIndexing.Search);
            }
        }

        private class Items_ByTag : AbstractIndexCreationTask<Item>
        {
            public Items_ByTag()
            {
                Map = items => from item in items
                               select new
                               {
                                   item.Tag,
                                   item.At,
                                   item.Price,
                                   item.Values
                               };
            }
        }

        // Lucene splits date ranges into day segments only when the query clause cache is on, and it is off by default
        private class Items_ByTagWithClauseCache : Items_ByTag
        {
            public Items_ByTagWithClauseCache()
            {
                Configuration[RavenConfiguration.GetKey(x => x.Indexing.QueryClauseCacheDisabled)] = "false";
            }
        }

        private class Shipment
        {
            public string Id { get; set; }
            public string Tag { get; set; }
            public Point Origin { get; set; }
            public Point Destination { get; set; }
        }

        private class Point
        {
            public long X { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }

        // B1 (query clause cache on): 'from' mid-day, 'to' on the next midnight, '<' must exclude the document sitting on 'to'
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void HalfOpenDateRangeEndingOnTheNextMidnight(Options options)
        {
            var from = new DateTime(2024, 1, 10, 12, 0, 0, DateTimeKind.Utc);
            var to = new DateTime(2024, 1, 11, 0, 0, 0, DateTimeKind.Utc);

            using var store = Seed(options, new Items_ByTagWithClauseCache(),
                new Item { Id = "items/inside", Tag = "t", At = from.AddHours(1) },
                new Item { Id = "items/on-upper", Tag = "t", At = to });

            using var session = store.OpenSession();
            var linq = session.Query<Item, Items_ByTagWithClauseCache>()
                .Where(x => x.Tag == "t" && x.At >= from && x.At < to);

            Output.WriteLine(linq.ToString());
            Assert.Equal(new[] { "items/inside" }, linq.ToList().Select(x => x.Id).OrderBy(x => x).ToArray());
        }

        // B2 (query clause cache on): 'from' after 'to', on different days, nothing can match
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void InvertedDateRangeMatchesNothing(Options options)
        {
            var from = new DateTime(2024, 1, 10, 12, 0, 0, DateTimeKind.Utc);
            var to = new DateTime(2024, 1, 5, 12, 0, 0, DateTimeKind.Utc);

            using var store = Seed(options, new Items_ByTagWithClauseCache(),
                new Item { Id = "items/after-from", Tag = "t", At = from.AddHours(1) },
                new Item { Id = "items/before-to", Tag = "t", At = to.AddHours(-6) });

            Assert.Empty(Query(store, "from index 'Items/ByTagWithClauseCache' where (Tag = 't' and At >= $from) and At < $to", ("from", from), ("to", to)));
        }

        // a range spanning several days with both ends mid-day: head, cached middle days and tail, the documents on the
        // day boundaries must be counted exactly once each and the one sitting on 'to' must be excluded
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void DateRangeAcrossSeveralDaysWithUnalignedEnds(Options options)
        {
            var from = new DateTime(2024, 1, 10, 12, 0, 0, DateTimeKind.Utc);
            var to = new DateTime(2024, 1, 13, 6, 0, 0, DateTimeKind.Utc);

            using var store = Seed(options, new Items_ByTagWithClauseCache(),
                new Item { Id = "items/before", Tag = "t", At = from.AddHours(-1) },
                new Item { Id = "items/on-from", Tag = "t", At = from },
                new Item { Id = "items/first-midnight", Tag = "t", At = new DateTime(2024, 1, 11, 0, 0, 0, DateTimeKind.Utc) },
                new Item { Id = "items/middle", Tag = "t", At = new DateTime(2024, 1, 12, 15, 0, 0, DateTimeKind.Utc) },
                new Item { Id = "items/last-midnight", Tag = "t", At = new DateTime(2024, 1, 13, 0, 0, 0, DateTimeKind.Utc) },
                new Item { Id = "items/on-to", Tag = "t", At = to },
                new Item { Id = "items/after", Tag = "t", At = to.AddHours(1) });

            Assert.Equal(new[] { "items/first-midnight", "items/last-midnight", "items/middle", "items/on-from" },
                Query(store, "from index 'Items/ByTagWithClauseCache' where (Tag = 't' and At >= $from) and At < $to", ("from", from), ("to", to)));
        }

        // B3: an integer lower bound and a fractional upper bound on the same field
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MixedIntegerAndFractionalBounds(Options options)
        {
            using var store = Seed(options,
                new Item { Id = "items/cheap", Tag = "t", Price = 12.5 },
                new Item { Id = "items/pricey", Tag = "t", Price = 25 });

            Assert.Equal(new[] { "items/cheap" }, Query(store, "from index 'Items/ByTag' where (Tag = 't' and Price >= 10) and Price < 20.5"));

            // the same through parameters, typed the way a JSON client sends 10 and 20.5
            Assert.Equal(new[] { "items/cheap" }, Query(store, "from index 'Items/ByTag' where (Tag = 't' and Price >= $lo) and Price < $hi", ("lo", 10), ("hi", 20.5)));
        }

        // B4: two different nested fields that share the last path segment
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void RangeOnDifferentNestedFieldsWithTheSameLeaf(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                session.Store(new Shipment { Id = "shipments/1", Tag = "t", Origin = new Point { X = 1 }, Destination = new Point { X = 100 } });
                session.SaveChanges();
            }

            // Destination.X = 100 fails 'Destination.X < 5'
            Assert.Empty(QueryDynamic<Shipment>(store, "from Shipments where (Tag = 't' and Origin.X >= 0) and (Destination.X < 5 or Destination.X < 5)"));
            Assert.Empty(QueryDynamic<Shipment>(store, "from Shipments where (Tag = 't' and Origin.X >= 0) and Destination.X < 5"));
        }

        // B5: exact() range pair on an auto index, 'Bob' is in ['A', 'M') case-sensitively, 'alice' is not
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ExactRangePairOnAutoIndex(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                session.Store(new User { Id = "users/1", Name = "Bob", Age = 30 });
                session.Store(new User { Id = "users/2", Name = "alice", Age = 30 });
                session.SaveChanges();
            }

            Assert.Equal(new[] { "users/1" }, QueryDynamic<User>(store, "from Users where exact((Age >= 0 and Name >= 'A') and (Name < 'M' or Name < 'M'))"));
            Assert.Equal(new[] { "users/1" }, QueryDynamic<User>(store, "from Users where exact((Age >= 0 and Name >= 'A') and Name < 'M')"));
        }

        // B6: multi-valued field, one value above the lower bound and another below the upper bound. Bounds written apart may
        // each hold for a different element; only the two sides of one 'and', or a between, ask for one element in the range
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void RangePairOnMultiValuedField(Options options)
        {
            // the extra documents give each term two entries, a single-entry term trips an unrelated Corax range issue
            using var store = Seed(options,
                new Item { Id = "items/1", Tag = "t", Values = new long[] { 0, 10 } },
                new Item { Id = "items/in-range", Tag = "t", Values = new long[] { 0, 3 } },
                new Item { Id = "items/zero", Tag = "x", Values = new long[] { 0 } },
                new Item { Id = "items/ten", Tag = "x", Values = new long[] { 10 } },
                new Item { Id = "items/three", Tag = "x", Values = new long[] { 3 } });

            Assert.Equal(new[] { "items/1", "items/in-range" }, Query(store, "from index 'Items/ByTag' where (Tag = 't' and Values >= 1) and Values < 5"));
            Assert.Equal(new[] { "items/1", "items/in-range" }, Query(store, "from index 'Items/ByTag' where when($flag != null, (Tag = 't' and Values >= 1) and Values < 5)", ("flag", 1)));
            Assert.Equal(new[] { "items/in-range" }, Query(store, "from index 'Items/ByTag' where Tag = 't' and (Values >= 1 and Values < 5)"));
            Assert.Equal(new[] { "items/in-range" }, Query(store, "from index 'Items/ByTag' where Tag = 't' and Values between 1 and 4"));
        }

        // B7: a third bound on a multi-valued field. 2 is in [1, 5) and 7 >= 3, so 'items/apart' matches although none of its
        // elements is in [3, 5); written as one pair the two bounds ask for a single element
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("Tag = 't' and Values >= 1 and Values < 5 and Values >= 3", "items/apart,items/inside", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Tag = 't' and Values >= $a and Values < $b and Values >= $c", "items/apart,items/inside", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Tag = 't' and Values >= 3 and Values < 5", "items/apart,items/inside", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Tag = 't' and (Values >= 1 and Values < 5) and Values >= 3", "items/apart,items/inside", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Tag = 't' and (Values >= 3 and Values < 5)", "items/inside", SearchEngineMode = RavenSearchEngineMode.All)]
        public void BoundsWrittenApartOnMultiValuedFieldMayHoldForDifferentElements(Options options, string where, string expectedIds)
        {
            using var store = Seed(options,
                new Item { Id = "items/apart", Tag = "t", Values = new long[] { 2, 7 } },
                new Item { Id = "items/inside", Tag = "t", Values = new long[] { 2, 4 } },
                new Item { Id = "items/two", Tag = "x", Values = new long[] { 2 } },
                new Item { Id = "items/four", Tag = "x", Values = new long[] { 4 } },
                new Item { Id = "items/seven", Tag = "x", Values = new long[] { 7 } });

            Assert.Equal(expectedIds.Split(','), Query(store, $"from index 'Items/ByTag' where {where}", ("a", 1), ("b", 5), ("c", 3)));
        }

        // B8: two Any() calls on a collection are two predicates, each free to hold for a different element; the LINQ provider
        // writes them apart, and one Any() with both bounds as a pair
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void TwoAnyOnACollectionMayHoldForDifferentElements(Options options)
        {
            using var store = Seed(options,
                new Item { Id = "items/mixed", Tag = "t", Values = new long[] { 2, 150 } },
                new Item { Id = "items/small", Tag = "t", Values = new long[] { 2, 3 } },
                new Item { Id = "items/large", Tag = "t", Values = new long[] { 150, 200 } },
                new Item { Id = "items/other", Tag = "x", Values = new long[] { 3, 200 } });

            using var session = store.OpenSession();

            var twoAny = session.Query<Item, Items_ByTag>().Where(x => x.Tag == "t" && x.Values.Any(v => v >= 100) && x.Values.Any(v => v < 5));
            Output.WriteLine(twoAny.ToString());
            Assert.Equal(new[] { "items/mixed" }, twoAny.ToList().Select(x => x.Id).ToArray());

            var oneAny = session.Query<Item, Items_ByTag>().Where(x => x.Tag == "t" && x.Values.Any(v => v >= 3 && v < 100));
            Output.WriteLine(oneAny.ToString());
            Assert.Equal(new[] { "items/small" }, oneAny.ToList().Select(x => x.Id).ToArray());
        }

        // B9: the same on a nested collection through an auto index: an order with a line of 100 or more and a line under 5
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void TwoAnyOnOrderLinesMayHoldForDifferentLines(Options options)
        {
            using var store = GetDocumentStore(options);
            Store(store,
                new Order { Id = "orders/mixed", Company = "companies/1", Lines = Lines(2, 150) },
                new Order { Id = "orders/small", Company = "companies/1", Lines = Lines(2, 3) },
                new Order { Id = "orders/large", Company = "companies/1", Lines = Lines(150, 200) },
                new Order { Id = "orders/other", Company = "companies/2", Lines = Lines(3, 200) });

            using var session = store.OpenSession();
            var query = session.Query<Order>().Customize(x => x.WaitForNonStaleResults())
                .Where(x => x.Company == "companies/1" && x.Lines.Any(l => l.Quantity >= 100) && x.Lines.Any(l => l.Quantity < 5));

            Output.WriteLine(query.ToString());
            Assert.Equal(new[] { "orders/mixed" }, query.ToList().Select(x => x.Id).ToArray());

            static List<OrderLine> Lines(params long[] quantities) => quantities.Select(q => new OrderLine { Quantity = q }).ToList();
        }

        // B10: a field written through CreateField and a tokenized text field hold several terms too
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("(Tag = 't' and Sizes >= 100) and Sizes < 5", "items/mixed", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Tag = 't' and (Sizes >= 3 and Sizes < 100)", "items/small", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(Tag = 't' and Words >= 'b') and Words < 'c'", "items/mixed,items/small", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Tag = 't' and (Words >= 'b' and Words < 'c')", "items/small", SearchEngineMode = RavenSearchEngineMode.All)]
        public void DynamicAndTokenizedFieldsHoldSeveralTerms(Options options, string where, string expectedIds)
        {
            var store = GetDocumentStore(options);
            new Items_WithSeveralTerms().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Id = "items/mixed", Tag = "t", Name = "alice zed", Values = new long[] { 2, 150 } });
                session.Store(new Item { Id = "items/small", Tag = "t", Name = "bob bill", Values = new long[] { 2, 3 } });
                session.Store(new Item { Id = "items/large", Tag = "t", Name = "dan zed", Values = new long[] { 150, 200 } });
                session.Store(new Item { Id = "items/other", Tag = "x", Name = "alice bill", Values = new long[] { 3, 200 } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            Assert.Equal(expectedIds.Split(','), Query(store, $"from index 'Items/WithSeveralTerms' where {where}"));
        }

        // B11: the arguments of intersect() are where clauses of their own, and a pair in one keeps asking for one element
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void PairInsideIntersectAsksForOneElement(Options options)
        {
            using var store = Seed(options,
                new Item { Id = "items/mixed", Tag = "t", Values = new long[] { 2, 150 } },
                new Item { Id = "items/inside", Tag = "t", Values = new long[] { 2, 50 } },
                new Item { Id = "items/other", Tag = "x", Values = new long[] { 50, 150 } });

            Assert.Equal(new[] { "items/inside" }, Query(store, "from index 'Items/ByTag' where intersect(Values >= 10 and Values < 100, Tag = 't')"));
        }

        // N1: on a sharded database a query with skip > 0 is re-rendered from the cached AST and sent to the shards; the
        // exclusive bounds of '>' and '<' must survive that
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
        public void ExclusiveBoundsSurviveTheShardedRewrite(Options options)
        {
            using var store = Seed(options,
                new Item { Id = "items/10", Tag = "t", Price = 10 },
                new Item { Id = "items/12", Tag = "t", Price = 12 },
                new Item { Id = "items/15", Tag = "t", Price = 15 },
                new Item { Id = "items/20", Tag = "t", Price = 20 });

            const string rql = "from index 'Items/ByTag' where Tag = 't' and Price > 10 and Price < 20 order by Price";

            using var session = store.OpenSession();
            Assert.Equal(new[] { "items/12", "items/15" }, session.Advanced.RawQuery<Item>(rql).ToList().Select(x => x.Id).ToArray());
            Assert.Equal(new[] { "items/15" }, session.Advanced.RawQuery<Item>(rql).Skip(1).ToList().Select(x => x.Id).ToArray());
        }

        // N4: an inclusive pair of mixed kinds folds into a between a hand-written between is not allowed to be. Re-rendered
        // for the shards it must print as the comparisons the user wrote, or the shard rejects it
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        [RavenData("Price >= 10 and Price <= 15.5", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
        [RavenData("Price >= $lo and Price <= $hi", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
        public void InclusiveBoundsOfMixedKindsSurviveTheShardedRewrite(Options options, string where)
        {
            using var store = Seed(options,
                new Item { Id = "items/10", Tag = "t", Price = 10 },
                new Item { Id = "items/12", Tag = "t", Price = 12 },
                new Item { Id = "items/15", Tag = "t", Price = 15 },
                new Item { Id = "items/20", Tag = "t", Price = 20 });

            using var session = store.OpenSession();
            Assert.Equal(new[] { "items/10", "items/12", "items/15" }, Query(0));
            Assert.Equal(new[] { "items/12", "items/15" }, Query(skip: 1));

            string[] Query(int skip) => session.Advanced.RawQuery<Item>($"from index 'Items/ByTag' where Tag = 't' and {where} order by Price")
                .AddParameter("lo", 10)
                .AddParameter("hi", 15.5)
                .Skip(skip)
                .ToList()
                .Select(x => x.Id)
                .ToArray();
        }

        // N5: a hand-written between with a null side is an open-ended range on Corax; it must not be taken apart into a
        // comparison against null, which the unary path rejects. Lucene never translated this shape and is not covered.
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData("Price between $lo and $hi", SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData("Price >= $lo and Price <= $hi", SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void BetweenWithANullSideIsOpenEndedOnCorax(Options options, string where)
        {
            using var store = Seed(options,
                new Item { Id = "items/5", Tag = "t", Price = 5 },
                new Item { Id = "items/12", Tag = "t", Price = 12 },
                new Item { Id = "items/20", Tag = "t", Price = 20 });

            Assert.Equal(new[] { "items/12", "items/5" }, Query(store, $"from index 'Items/ByTag' where {where}", ("lo", null), ("hi", 12)));
            Assert.Equal(new[] { "items/12", "items/20" }, Query(store, $"from index 'Items/ByTag' where {where}", ("lo", 12), ("hi", null)));
        }

        // N6: a long and a double bound written as the two sides of one 'and' compare as doubles, as they always did on Corax;
        // the same pair apart keeps the long comparison against the truncated value
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("Price > 10 and Price <= 10.5", "items/10.5", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Price > $lo and Price <= $hi", "items/10.5", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Price >= 10.5 and Price <= 15", "items/10.5,items/12,items/15", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Price >= 10.5 and Price <= 15 and Tag = 't'", "items/10.5,items/12,items/15", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(Tag = 't' and Price > 10) and Price <= 10.5", "", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("(Tag = 't' and Price >= 10.5) and Price <= 15", "items/10.5,items/12,items/15,items/15.5", SearchEngineMode = RavenSearchEngineMode.All)]
        public void SiblingPairOfALongAndADoubleBoundComparesAsDoubles(Options options, string where, string expectedIds)
        {
            using var store = Seed(options,
                new Item { Id = "items/10", Tag = "t", Price = 10 },
                new Item { Id = "items/10.5", Tag = "t", Price = 10.5 },
                new Item { Id = "items/12", Tag = "t", Price = 12 },
                new Item { Id = "items/15", Tag = "t", Price = 15 },
                new Item { Id = "items/15.5", Tag = "t", Price = 15.5 });

            Assert.Equal(expectedIds.Split(',', StringSplitOptions.RemoveEmptyEntries), Query(store, $"from index 'Items/ByTag' where {where}", ("lo", 10), ("hi", 10.5)));
        }

        // N7: with a long and a double literal on a multi-valued field the result does not depend on the order of the clauses
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("Values > 1 and Values < 7 and Values < 2.5", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Values > 1 and Values < 2.5 and Values < 7", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ClauseOrderOfMixedLiteralsOnMultiValuedField(Options options, string where)
        {
            using var store = Seed(options,
                new Item { Id = "items/apart", Tag = "t", Values = new long[] { 0, 10 } },
                new Item { Id = "items/two", Tag = "t", Values = new long[] { 2 } },
                new Item { Id = "items/zero", Tag = "t", Values = new long[] { 0 } },
                new Item { Id = "items/ten", Tag = "t", Values = new long[] { 10 } },
                new Item { Id = "items/two-again", Tag = "t", Values = new long[] { 2 } });

            Assert.Equal(new[] { "items/two", "items/two-again" }, Query(store, $"from index 'Items/ByTag' where {where}"));
        }

        // N8: a sharded query is re-rendered for its shards from the where clause as written, so a pair keeps the shape, and
        // with it the comparison, it has on a single node
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        [RavenData("Tag = 't' and Price >= 10.5 and Price <= 15", "items/10.5,items/12,items/15,items/15.5", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
        [RavenData("Price >= 10.5 and Price <= 15", "items/10.5,items/12,items/15", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
        public void ShardsGetTheWhereClauseAsWritten(Options options, string where, string expectedIds)
        {
            using var store = Seed(options,
                new Item { Id = "items/10", Tag = "t", Price = 10 },
                new Item { Id = "items/10.5", Tag = "t", Price = 10.5 },
                new Item { Id = "items/12", Tag = "t", Price = 12 },
                new Item { Id = "items/15", Tag = "t", Price = 15 },
                new Item { Id = "items/15.5", Tag = "t", Price = 15.5 });

            var expected = expectedIds.Split(',');

            using var session = store.OpenSession();
            Assert.Equal(expected, Page(0));
            Assert.Equal(expected.Skip(1), Page(skip: 1));

            string[] Page(int skip) => session.Advanced.RawQuery<Item>($"from index 'Items/ByTag' where {where} order by Price")
                .Skip(skip)
                .ToList()
                .Select(x => x.Id)
                .ToArray();
        }

        // N2: same-direction literal bounds of mixed numeric kinds on one field; a long literal compares against the long
        // term, so 10.5 fails 'Price > 10' even though it passes 'Price > 10.1'
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void SameDirectionLongAndDoubleLiteralBoundsKeepBothComparisons(Options options)
        {
            using var store = Seed(options,
                new Item { Id = "items/10.5", Tag = "t", Price = 10.5 },
                new Item { Id = "items/11.5", Tag = "t", Price = 11.5 });

            Assert.Equal(new[] { "items/11.5" }, Query(store, "from index 'Items/ByTag' where Tag = 't' and Price > 10 and Price > 10.1"));
        }

        // N3: the same through parameters, a JSON client sends 10 as an integer and 10.1 as a double
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void SameDirectionLongAndDoubleParameterBoundsKeepBothComparisons(Options options)
        {
            using var store = Seed(options,
                new Item { Id = "items/10.5", Tag = "t", Price = 10.5 },
                new Item { Id = "items/11.5", Tag = "t", Price = 11.5 });

            Assert.Equal(new[] { "items/11.5" }, Query(store, "from index 'Items/ByTag' where Tag = 't' and Price > $a and Price > $b", ("a", 10), ("b", 10.1)));
        }

        private IDocumentStore Seed(Options options, params Item[] items) => Seed(options, new Items_ByTag(), items);

        private IDocumentStore Seed(Options options, Items_ByTag index, params Item[] items)
        {
            var store = GetDocumentStore(options);
            index.Execute(store);

            using (var session = store.OpenSession())
            {
                foreach (var item in items)
                    session.Store(item);
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            return store;
        }

        private static string[] Query(IDocumentStore store, string rql, params (string Name, object Value)[] parameters)
        {
            using var session = store.OpenSession();
            var query = session.Advanced.RawQuery<Item>(rql);
            foreach (var (name, value) in parameters)
                query = query.AddParameter(name, value);

            return query.ToList().Select(x => x.Id).OrderBy(x => x).ToArray();
        }

        private static string[] QueryDynamic<T>(IDocumentStore store, string rql)
        {
            using var session = store.OpenSession();
            var results = session.Advanced.RawQuery<T>(rql).WaitForNonStaleResults().ToList();
            return results.Select(x => session.Advanced.GetDocumentId(x)).OrderBy(x => x).ToArray();
        }

        // Review cases on auto indexes: a folded between must never change what either bound meant on its own.

        private class Money
        {
            public int Amount { get; set; }
        }

        private class Product
        {
            public string Id { get; set; }
            public string Supplier { get; set; }
            public Money Price { get; set; }
            public Money Cost { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public double Freight { get; set; }
            public DateTime OrderedAt { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        private class OrderLine
        {
            public long Quantity { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("Supplier = 'suppliers/1' and Price.Amount > 10 and Cost.Amount < 20", SearchEngineMode = RavenSearchEngineMode.All)]
        public void RangesOnDifferentNestedFieldsWithTheSameLeafNameStaySeparate(Options options, string where)
        {
            using var store = GetDocumentStore(options);
            Store(store,
                new Product { Id = "products/1", Supplier = "suppliers/1", Price = new Money { Amount = 15 }, Cost = new Money { Amount = 25 } },
                new Product { Id = "products/2", Supplier = "suppliers/1", Price = new Money { Amount = 15 }, Cost = new Money { Amount = 5 } });

            Assert.Equal(new[] { "products/2" }, QueryIds<Product>(store, $"from Products where {where}"));
        }

        // a string lower bound and a numeric upper bound, or a long and a double: each bound keeps its own translation
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Lucene)]
        [RavenData("Company = 'companies/1' and Freight >= $min and Freight < $max", 10, 20.5, "orders/2", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData("Company = 'companies/1' and Freight >= $min and Freight < $max", "*", 30, "orders/1,orders/2", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void LuceneRangeBoundsOfDifferentTypesAreTranslatedIndependently(Options options, string where, object min, object max, string expectedIds)
        {
            using var store = GetDocumentStore(options);
            Store(store,
                new Order { Id = "orders/1", Company = "companies/1", Freight = 5 },
                new Order { Id = "orders/2", Company = "companies/1", Freight = 15 },
                new Order { Id = "orders/3", Company = "companies/1", Freight = 100 });

            Assert.Equal(expectedIds.Split(','), QueryIds<Order>(store, $"from Orders where {where}", ("min", min), ("max", max)));
        }

        // a long literal compares against the long term of a double field, which truncates 10.2 to 10
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
        [RavenData("Company = 'companies/1' and Freight > 10 and Freight < 20.5", SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void CoraxLongBoundPairedWithDoubleBoundKeepsTheLongComparison(Options options, string where)
        {
            using var store = GetDocumentStore(options);
            Store(store,
                new Order { Id = "orders/1", Company = "companies/1", Freight = 10.2 },
                new Order { Id = "orders/2", Company = "companies/1", Freight = 15 });

            Assert.Equal(new[] { "orders/2" }, QueryIds<Order>(store, $"from Orders where {where}"));
        }

        // the pair folds inside exact() too, and the between it becomes queries the exact field, so the lower-case name
        // stays outside the upper-case range
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("exact(Age = 30 and Name >= 'A' and Name < 'C')", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("exact((Age = 30 and Name >= 'A') and Name < 'C')", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Age = 30 and exact(Name >= 'A' and Name < 'C')", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ExactStringRangeOnAutoIndexUsesTheExactField(Options options, string where)
        {
            using var store = GetDocumentStore(options);
            Store(store,
                new User { Id = "users/1", Name = "Alice", Age = 30 },
                new User { Id = "users/2", Name = "Bob", Age = 30 },
                new User { Id = "users/3", Name = "Carl", Age = 30 },
                new User { Id = "users/4", Name = "alice", Age = 30 });

            Assert.Equal(new[] { "users/1", "users/2" }, QueryIds<User>(store, $"from Users where {where}"));
        }

        // a hand-written between inside exact() queries the exact field of the auto index, as the comparisons do; the
        // lower-case name falls outside an upper-case range only there
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Lucene)]
        [RavenData("exact(Age = 30 and Name between 'A' and 'C')", "users/1,users/2", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("exact(Name between 'A' and 'M')", "users/1,users/2,users/3", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("Name between 'A' and 'M'", "users/1,users/2,users/3,users/4", SearchEngineMode = RavenSearchEngineMode.All)]
        public void ExactBetweenOnAutoIndexUsesTheExactField(Options options, string where, string expectedIds)
        {
            using var store = GetDocumentStore(options);
            Store(store,
                new User { Id = "users/1", Name = "Alice", Age = 30 },
                new User { Id = "users/2", Name = "Bob", Age = 30 },
                new User { Id = "users/3", Name = "Carl", Age = 30 },
                new User { Id = "users/4", Name = "alice", Age = 30 });

            Assert.Equal(expectedIds.Split(','), QueryIds<User>(store, $"from Users where {where}"));
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Lucene)]
        [RavenData("Company = 'companies/1' and OrderedAt >= $from and OrderedAt < $to", "2020-01-01T12:00:00.0000000", "2020-01-02T00:00:00.0000000", "orders/1", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData("Company = 'companies/1' and OrderedAt >= $from and OrderedAt < $to", "2020-01-03T05:00:00.0000000", "2020-01-01T03:00:00.0000000", "", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void LuceneSegmentedDateRangeMatchesTheAndOfBothBounds(Options options, string where, string from, string to, string expectedIds)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    options.ModifyDatabaseRecord?.Invoke(record);
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.QueryClauseCacheDisabled)] = "false";
                }
            });
            Store(store,
                new Order { Id = "orders/1", Company = "companies/1", OrderedAt = new DateTime(2020, 1, 1, 13, 0, 0) },
                new Order { Id = "orders/2", Company = "companies/1", OrderedAt = new DateTime(2020, 1, 2) },
                new Order { Id = "orders/3", Company = "companies/1", OrderedAt = new DateTime(2020, 1, 1, 1, 0, 0) },
                new Order { Id = "orders/4", Company = "companies/1", OrderedAt = new DateTime(2020, 1, 3, 10, 0, 0) });

            Assert.Equal(expectedIds.Split(',', StringSplitOptions.RemoveEmptyEntries), QueryIds<Order>(store, $"from Orders where {where}", ("from", from), ("to", to)));
        }

        private static void Store(IDocumentStore store, params object[] documents)
        {
            using (var session = store.OpenSession())
            {
                foreach (var document in documents)
                    session.Store(document);
                session.SaveChanges();
            }
        }

        private static string[] QueryIds<T>(IDocumentStore store, string rql, params (string Name, object Value)[] parameters)
        {
            using var session = store.OpenSession();
            Raven.Client.Documents.Session.IRawDocumentQuery<T> query = session.Advanced.RawQuery<T>(rql).WaitForNonStaleResults();
            foreach (var (name, value) in parameters)
                query = query.AddParameter(name, value);

            return query.ToList().Select(x => session.Advanced.GetDocumentId(x)).OrderBy(x => x).ToArray();
        }
    }
}
