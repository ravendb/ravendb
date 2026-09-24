using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Analyzers;
using Raven.Client.Documents.Queries.Timings;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27586 : RavenTestBase
    {
        public RavenDB_27586(ITestOutputHelper output) : base(output)
        {
        }

        private const string Lookup = "CompoundKeyLookup";
        private const string Bitmap = "BitmapPipeline";

        private class Item
        {
            public string Id { get; set; }
            public double Num { get; set; }
            public long Other { get; set; }
            public string Name { get; set; }
            public string Tag { get; set; }
            public bool Flag { get; set; }
            public float[] Embedding { get; set; }
        }

        private class ItemWithNumericName
        {
            public string Id { get; set; }
            public double Num { get; set; }
            public long Name { get; set; }
            public string Tag { get; set; }
        }

        private class Items_ByCompounds : AbstractIndexCreationTask<Item>
        {
            public Items_ByCompounds()
            {
                Map = items => from i in items select new { i.Num, i.Other, i.Name, i.Tag, i.Flag, Grade = i.Num > 1.2 ? 'A' : 'B' };
                CompoundField("Num", "Other");
                CompoundField("Name", "Tag");
                CompoundField("Name", "Other");
                CompoundField("Flag", "Name");
                CompoundField("Grade", "Tag");
            }
        }

        private class Items_ByNameAndUnindexedNum : AbstractIndexCreationTask<Item>
        {
            public Items_ByNameAndUnindexedNum()
            {
                Map = items => from i in items select new { i.Name, i.Num };
                Index(i => i.Num, FieldIndexing.No);
                Store(i => i.Num, FieldStorage.Yes);
                CompoundField("Name", "Num");
            }
        }

        private const string StemmingAnalyzerCode = @"
using System.IO;
using Lucene.Net.Analysis;

namespace SlowTests.Data.RavenDB_27586
{
    public class StemmingKeywordAnalyzer : Analyzer
    {
        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            return new PorterStemFilter(new LowerCaseFilter(new KeywordTokenizer(reader)));
        }
    }
}";

        private class Items_ByNameStemmedFlag : AbstractIndexCreationTask<Item>
        {
            public Items_ByNameStemmedFlag()
            {
                Map = items => from i in items select new { i.Name, i.Flag };
                Index(i => i.Name, FieldIndexing.Exact); // the first compound field needs a keyword analyzer
                CompoundField("Name", "Flag");
                Configuration[RavenConfiguration.GetKey(x => x.Indexing.DefaultAnalyzer)] = "StemmingKeywordAnalyzer";
            }
        }

        private class Items_ByNameStemmedSearchFlag : AbstractIndexCreationTask<Item>
        {
            public Items_ByNameStemmedSearchFlag()
            {
                Map = items => from i in items select new { i.Name, i.Flag };
                Index(i => i.Flag, FieldIndexing.Search);
                Analyze(i => i.Flag, "StemmingKeywordAnalyzer");
                CompoundField("Name", "Flag");
            }
        }

        private class Items_ByNameTagVector : AbstractIndexCreationTask<Item>
        {
            public Items_ByNameTagVector()
            {
                Map = items => from i in items select new { i.Name, i.Tag, Vector = CreateVector(i.Embedding) };
                CompoundField("Name", "Tag");
                // otherwise the score ordering, not the vector check, turns the lookup down
                Configuration[RavenConfiguration.GetKey(x => x.Indexing.CoraxVectorSearchOrderByScoreAutomatically)] = "false";
            }
        }

        private class Place
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Tag { get; set; }
            public double Lat { get; set; }
            public double Lng { get; set; }
        }

        private class Places_ByNameTagAndLocation : AbstractIndexCreationTask<Place>
        {
            public Places_ByNameTagAndLocation()
            {
                Map = places => from p in places select new { p.Name, p.Tag, Location = CreateSpatialField(p.Lat, p.Lng) };
                CompoundField("Name", "Tag");
            }
        }

        private class Items_BySearchTag : AbstractIndexCreationTask<Item>
        {
            public Items_BySearchTag()
            {
                Map = items => from i in items select new { i.Name, i.Tag };
                Index(Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.Search);
                Index(i => i.Name, FieldIndexing.Exact);
                CompoundField("Name", "Tag");
            }
        }

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData("where Num == $v and Other == 7 order by Num as double", 1L, new[] { 1.0, 1.1, 1.5, 1.8 }, true, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Num == $v and Other == 7", 1L, new[] { 1.0, 1.1, 1.5, 1.8 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Num == $v and Other == 7", 1.5, new[] { 1.5 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Num == 1.5 and Other == $v", 7.0, new[] { 1.5 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Name == 'ann' and Other == $v", "7", new[] { 1.0, 1.1, 1.5, 1.8 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Flag == $v and Name == 'ann'", true, new[] { 1.1, 1.5, 1.8 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Flag == $v and Name == 'ann'", false, new[] { 1.0 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Grade == $v and Tag == 'red'", "A", new[] { 1.5, 1.8 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where exact(Grade == $v) and Tag == 'red'", "A\u0000", new double[0], false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Name == $v and Tag == 'red' order by random('x')", "Ann", new[] { 1.0, 1.1, 1.5, 1.8 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Name == $v and Tag == 'red' order by score()", "Ann", new[] { 1.0, 1.1, 1.5, 1.8 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where exact(Name == $v) and Tag == 'red'", "Ann", new double[0], false, Lookup, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where exact(Name == $v) and Tag == 'red'", "ann", new[] { 1.0, 1.1, 1.5, 1.8 }, false, Lookup, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Name == $v and Tag == 'red' order by Num as double", "Ann", new[] { 1.0, 1.1, 1.5, 1.8 }, true, Lookup, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Name == $v and Tag == 'red' order by Other as long, Num as double desc limit 3", "Ann", new[] { 1.8, 1.5, 1.1 }, true, Lookup, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CompoundKeyLookupReturnsSameRowsAsRegularEquality(Options options, string where, object value, double[] expected, bool ordered, string strategy)
        {
            using var store = GetStoreWithFourItems(options);

            var (actual, ran) = Query<Item>(store, $"from index 'Items/ByCompounds' {where}", value);
            var nums = actual.Select(x => x.Num).ToList();
            if (ordered == false)
                nums.Sort();

            Assert.Equal(expected, nums);
            AssertStrategy(options, strategy, ran);
        }

        // a number, a third clause, a lone equality: a forced lookup must fall back to the bitmap, not answer differently or throw
        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData("where Num == $v and Other == 7", 1L, new[] { 1.0, 1.1, 1.5, 1.8 }, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData("where Name == 'ann' and Tag == 'red' and Num > $v", 1.2, new[] { 1.5, 1.8 }, SearchEngineMode = RavenSearchEngineMode.Corax)]
        [RavenData("where Name == $v", "Ann", new[] { 1.0, 1.1, 1.5, 1.8 }, SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void ForcedLookupFallsBackWhereItCannotAnswer(Options options, string where, object value, double[] expected)
        {
            using var store = GetStoreWithFourItems(options);

            var (actual, ran) = Query<Item>(store, $"from index 'Items/ByCompounds' {where}", value, force: Lookup);
            Assert.Equal(expected, actual.Select(x => x.Num).OrderBy(x => x));
            Assert.Equal(Bitmap, ran);
        }

        private IDocumentStore GetStoreWithFourItems(Options options)
        {
            var store = GetDocumentStore(options);
            new Items_ByCompounds().Execute(store);

            using (var session = store.OpenSession())
            {
                foreach (var num in new[] { 1.8, 1.1, 1.5, 1.0 })
                    session.Store(new Item { Num = num, Other = 7, Name = "Ann", Tag = "red", Flag = num != 1.0 });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            return store;
        }

        // null, "" and strings share one plan; a null is looked up as the byte 0 a stored false is keyed with
        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void LookupPlanReusedForNullEmptyAndBoolValues(Options options)
        {
            using var store = GetDocumentStore(options);
            new Items_ByCompounds().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Num = 1, Name = "Ann", Tag = "red", Flag = true });
                session.Store(new Item { Num = 2, Name = "Ann", Tag = "red", Flag = false });
                session.Store(new Item { Num = 3, Name = null, Tag = "red", Flag = true });
                session.Store(new Item { Num = 4, Name = "", Tag = "red", Flag = true });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            foreach (var (value, expected, strategy) in new (object, double[], string)[]
                     {
                         ("Ann", [1, 2], Lookup), (null, [3], Bitmap), ("", [4], Bitmap), ("Ann", [1, 2], Lookup)
                     })
                AssertNums(options, store, "from index 'Items/ByCompounds' where Name == $v and Tag == 'red'", value, expected, strategy);

            foreach (var (value, expected, strategy) in new (object, double[], string)[]
                     {
                         ("xx", [], Lookup), (null, [], Bitmap), (true, [1], Bitmap), (false, [2], Bitmap)
                     })
                AssertNums(options, store, "from index 'Items/ByCompounds' where Flag == $v and Name == 'ann'", value, expected, strategy);
        }

        // a null first run must not keep the plan off the lookup
        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void LookupWithTheEqualitiesInReverseOrder(Options options)
        {
            using var store = GetDocumentStore(options);
            new Items_ByCompounds().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Num = 1, Name = "Ann", Tag = "red" });
                session.Store(new Item { Num = 3, Name = null, Tag = "red" });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            const string rql = "from index 'Items/ByCompounds' where Tag == 'red' and Name == $v";
            AssertNums(options, store, rql, null, [3], Bitmap);
            AssertNums(options, store, rql, "Ann", [1], Lookup);
        }

        // the stemming analyzer indexes a stored false as 'fals'
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void LookupTurnedDownForTheFieldsAnalyzedBoolLiterals()
        {
            var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition { Name = "StemmingKeywordAnalyzer", Code = StemmingAnalyzerCode }));
            new Items_ByNameStemmedFlag().Execute(store);
            new Items_ByNameStemmedSearchFlag().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Num = 1, Name = "ann", Flag = false });
                session.Store(new Item { Num = 2, Name = "ann", Flag = true });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            AssertNums(options, store, "from index 'Items/ByNameStemmedFlag' where Name == 'ann' and Flag == $v", "false", [1], Bitmap);
            AssertNums(options, store, "from index 'Items/ByNameStemmedSearchFlag' where Name == 'ann' and Flag == $v", "fals", [1], Bitmap);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void LookupKeepsTheVectorFilter()
        {
            var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
            using var store = GetDocumentStore(options);
            new Items_ByNameTagVector().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Num = 1, Name = "Ann", Tag = "red", Embedding = [1f, 0f] });
                session.Store(new Item { Num = 2, Name = "Ann", Tag = "red", Embedding = [0f, 1f] });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            AssertNums(options, store, "from index 'Items/ByNameTagVector' where Name == 'ann' and Tag == 'red' and vector.search(Vector, $v, 0.99)",
                new[] { 1f, 0f }, [1], Bitmap);
        }

        // the cached plan outlives indexing
        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void LookupTurnedDownOnceTheFieldHoldsNumbers(Options options)
        {
            using var store = GetDocumentStore(options);
            new Items_ByCompounds().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Num = 1, Name = "77", Tag = "red" });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            const string rql = "from index 'Items/ByCompounds' where Name == $v and Tag == 'red'";
            AssertNums(options, store, rql, "77", [1], Lookup);

            using (var session = store.OpenSession())
            {
                var numeric = new ItemWithNumericName { Num = 2, Name = 77, Tag = "red" };
                session.Store(numeric);
                session.Advanced.GetMetadataFor(numeric)[Constants.Documents.Metadata.Collection] = "Items";
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            AssertNums(options, store, rql, "77", [1, 2], Bitmap);
        }

        // "AAAAAAAA" is the key bytes of the stored double
        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void LookupOnAnUnindexedFieldFindsNothing(Options options)
        {
            using var store = GetDocumentStore(options);
            new Items_ByNameAndUnindexedNum().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Num = BitConverter.Int64BitsToDouble(0x4141414141414141), Name = "Ann" });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            AssertNums(options, store, "from index 'Items/ByNameAndUnindexedNum' where Name == 'ann' and Num == $v", "AAAAAAAA", [], Bitmap);
        }

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData("where Name == 'ann' and Tag == 'red' and spatial.within(Location, spatial.circle(10, 32.0, 34.0))", SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where spatial.within(Location, spatial.circle(10, 32.0, 34.0)) and Name == 'ann' and Tag == 'red'", SearchEngineMode = RavenSearchEngineMode.All)]
        public void LookupKeepsTheSpatialFilter(Options options, string where)
        {
            using var store = GetDocumentStore(options);
            new Places_ByNameTagAndLocation().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Place { Id = "places/1", Name = "Ann", Tag = "red", Lat = 32.0, Lng = 34.0 });
                session.Store(new Place { Id = "places/2", Name = "Ann", Tag = "red", Lat = 40.0, Lng = -70.0 });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            var (actual, ran) = Query<Place>(store, $"from index 'Places/ByNameTagAndLocation' {where}", value: null);
            Assert.Equal(new[] { "places/1" }, actual.Select(x => x.Id));
            AssertStrategy(options, Bitmap, ran);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void LookupOfAMultiTokenValueFindsNothing()
        {
            var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
            using var store = GetDocumentStore(options);
            new Items_BySearchTag().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Num = 1, Name = "Ann", Tag = "hello" });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            const string rql = "from index 'Items/BySearchTag' where Name == 'Ann' and Tag == $v";
            AssertNums(options, store, rql, "hello", [1], Lookup);
            AssertNums(options, store, rql, "hello world", [], Bitmap);
        }

        private static void AssertNums(Options options, IDocumentStore store, string rql, object value, double[] expected, string strategy)
        {
            var (actual, ran) = Query<Item>(store, rql, value);
            Assert.Equal(expected, actual.Select(x => x.Num).OrderBy(x => x));
            AssertStrategy(options, strategy, ran, $"{rql} with {value ?? "null"}");
        }

        private static void AssertStrategy(Options options, string expected, string actual, string query = null)
        {
            if (options.SearchEngineMode == RavenSearchEngineMode.Corax)
                Assert.True(expected == actual, $"expected {expected}, ran {actual ?? "(no plan)"}: {query}");
        }

        private static (List<T> Results, string Strategy) Query<T>(IDocumentStore store, string rql, object value, string force = null)
        {
            using var session = store.OpenSession();
            var query = session.Advanced.RawQuery<T>(rql + " include timings()").NoCaching().Timings(out QueryTimings timings); // a cached reply has no plan
            if (rql.Contains("$v"))
                query.AddParameter("v", value);
            if (force != null)
                query.AddParameter("rvn_corax_strategy", force);

            var results = query.ToList();
            string strategy = null;
            FindNode(timings.QueryPlan as QueryInspectionNode, "CompiledQuery")?.Parameters?.TryGetValue("OptimizationHint", out strategy);
            return (results, strategy);

            static QueryInspectionNode FindNode(QueryInspectionNode node, string operation) =>
                node == null ? null : node.Operation == operation ? node : node.Children?.Select(c => FindNode(c, operation)).FirstOrDefault(n => n != null);
        }
    }
}
