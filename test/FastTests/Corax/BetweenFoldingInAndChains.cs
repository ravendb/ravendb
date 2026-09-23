using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Timings;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    public class BetweenFoldingInAndChains : RavenTestBase
    {
        private readonly ITestOutputHelper _output;

        public BetweenFoldingInAndChains(ITestOutputHelper output) : base(output)
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

        [RavenTheory(RavenTestCategory.Querying)]
        [InlineData(ClientShape, "AND[ AND[ CustomerId in ($p0) , CreatedAt between $p1 and $p2 ] , EntityType in ($p3) ]")]
        [InlineData(ProposedShape, "AND[ AND[ CustomerId in ($p0) , CreatedAt between $p1 and $p2 ] , EntityType in ($p3) ]")]
        [InlineData(FlatShape, "AND[ AND[ CustomerId in ($p0) , CreatedAt between $p1 and $p2 ] , EntityType in ($p3) ]")]
        [InlineData(GroupedShape, "AND[ AND[ CustomerId in ($p0) , CreatedAt between $p1 and $p2 ] , EntityType in ($p3) ]")]
        [InlineData(RangeFirstShape, "AND[ AND[ CreatedAt between $p1 and $p2 , CustomerId in ($p0) ] , EntityType in ($p3) ]")]
        [InlineData(ReversedSeparatedShape, "AND[ AND[ EntityType in ($p3) , CreatedAt between $p1 and $p2 ] , CustomerId in ($p0) ]")]
        [InlineData(SplitAcrossGroupsShape, "AND[ AND[ CreatedAt between $p1 and $p2 , CustomerId in ($p0) ] , EntityType in ($p3) ]")]
        [InlineData(DuplicateDirectionShape, "AND[ AND[ AND[ CreatedAt between $p1 and $p2 , CreatedAt >= $p1 ] , CustomerId in ($p0) ] , EntityType in ($p3) ]")]
        [InlineData(NegatedOperandShape, "AND[ AND[ CustomerId in ($p0) , CreatedAt between $p1 and $p2 ] , NOT[ EntityType = 'Withdrawal' ] ]")]
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

        private static void Collect(QueryInspectionNode node, List<QueryInspectionNode> rangeScansOnCreatedAt)
        {
            if (node == null)
                return;

            if (node.Operation.Contains("RangeProvider") &&
                node.Parameters != null &&
                node.Parameters.TryGetValue("FieldName", out var field) &&
                field.Contains("CreatedAt"))
            {
                rangeScansOnCreatedAt.Add(node);
            }

            foreach (var child in node.Children ?? new List<QueryInspectionNode>())
                Collect(child, rangeScansOnCreatedAt);
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
    }
}
