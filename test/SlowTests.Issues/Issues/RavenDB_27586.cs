using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Analyzers;
using Raven.Client.Documents.Operations.Indexes;
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

        private class ItemWithLongNum
        {
            public string Id { get; set; }
            public long Num { get; set; }
            public long Other { get; set; }
            public string Name { get; set; }
        }

        private class ItemWithTextNum
        {
            public string Id { get; set; }
            public string Num { get; set; }
            public long Other { get; set; }
        }

        private class Items_ByNumAndOther : AbstractIndexCreationTask<Item>
        {
            public Items_ByNumAndOther()
            {
                Map = items => from i in items select new { i.Num, i.Other };
                CompoundField("Num", "Other");
            }
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

        private const string SplitOnUAnalyzerCode = @"
using System.IO;
using Lucene.Net.Analysis;

namespace SlowTests.Data.RavenDB_27586
{
    public class SplitOnUAnalyzer : Analyzer
    {
        public override TokenStream TokenStream(string fieldName, TextReader reader) => new SplitOnU(reader);

        private class SplitOnU : CharTokenizer
        {
            public SplitOnU(TextReader input) : base(input) { }
            protected override bool IsTokenChar(char c) => c != 'u';
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

        private class Items_ByNameNGramFlag : AbstractIndexCreationTask<Item>
        {
            public Items_ByNameNGramFlag()
            {
                Map = items => from i in items select new { i.Name, i.Flag };
                Index(i => i.Flag, FieldIndexing.Search);
                Analyze(i => i.Flag, "NGramAnalyzer"); // [NotForQuerying]: the reader swaps it for the standard analyzer
                CompoundField("Name", "Flag");
            }
        }

        private class Items_ByNameSplitDefaultFlag : AbstractIndexCreationTask<Item>
        {
            public Items_ByNameSplitDefaultFlag()
            {
                Map = items => from i in items select new { i.Name, i.Flag };
                Index(i => i.Flag, FieldIndexing.Search);
                CompoundField("Name", "Flag");
                Configuration[RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer)] = "SplitOnUAnalyzer"; // "true" -> "tr", "e"
            }
        }

        private class Items_ByNameTagAndCreatedName : AbstractIndexCreationTask<Item>
        {
            public Items_ByNameTagAndCreatedName()
            {
                Map = items => from i in items select new { i.Name, i.Tag, _ = CreateField("Name", i.Tag) };
                CompoundField("Name", "Tag");
            }
        }

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData("where Num == $v and Other == 7 order by Num as double", 1L, new[] { 1.0, 1.1, 1.5, 1.8 }, true, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Num == $v and Other == 7", 1L, new[] { 1.0, 1.1, 1.5, 1.8 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Num == $v and Other == 7", 1.5, new[] { 1.5 }, false, Lookup, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Num == 1.5 and Other == $v", 7.0, new[] { 1.5 }, false, Lookup, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Name == 'ann' and Other == $v", 7L, new[] { 1.0, 1.1, 1.5, 1.8 }, false, Lookup, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Name == 'ann' and Other == $v", 7.0, new[] { 1.0, 1.1, 1.5, 1.8 }, false, Lookup, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Name == 'ann' and Other == $v", "7", new[] { 1.0, 1.1, 1.5, 1.8 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Flag == $v and Name == 'ann'", true, new[] { 1.1, 1.5, 1.8 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Flag == $v and Name == 'ann'", false, new[] { 1.0 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Grade == $v and Tag == 'red'", "A", new[] { 1.5, 1.8 }, false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where exact(Grade == $v) and Tag == 'red'", "A\u0000", new double[0], false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where exact(Name == $v) and Tag == 'red'", "Ann\u0000", new double[0], false, Bitmap, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData("where Name == $v and Tag == 'red' order by random('x')", "Ann", new[] { 1.0, 1.1, 1.5, 1.8 }, false, Lookup, SearchEngineMode = RavenSearchEngineMode.All)]
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
                         ("xxx", [], Lookup), (null, [], Bitmap), (true, [1], Bitmap), (false, [2], Bitmap)
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
                session.Store(new Item { Num = 1, Name = "777", Tag = "red" });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            const string rql = "from index 'Items/ByCompounds' where Name == $v and Tag == 'red'";
            AssertNums(options, store, rql, "777", [1], Lookup);

            using (var session = store.OpenSession())
            {
                var numeric = new ItemWithNumericName { Num = 2, Name = 777, Tag = "red" };
                session.Store(numeric);
                session.Advanced.GetMetadataFor(numeric)[Constants.Documents.Metadata.Collection] = "Items";
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            AssertNums(options, store, rql, "777", [1, 2], Bitmap);
        }

        // a long matches by the integer part, a double exactly
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void NumericLookupReturnsTheEqualitysRows()
        {
            using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
            new Items_ByCompounds().Execute(store);
            StoreNums(store, ("a", 1L), ("b", 1.0), ("c", 1.8), ("d", -1L), ("e", -1.5), ("f", -0.5), ("g", 0.5), ("h", 0L), ("i", -0.0), ("j", 2L),
                ("k", 9007199254740993L), ("z", 0.0));

            const string rql = "from index 'Items/ByCompounds' where Num == $v and Other == 7";
            foreach (var (value, expected, strategy) in new (object, string, string)[]
                     {
                         (1L, "abc", Bitmap), (1.0, "ab", Lookup), (1.8, "c", Lookup), (2L, "j", Lookup), (2.0, "j", Lookup),
                         (-1L, "de", Bitmap), (-1.0, "d", Lookup), (-1.5, "e", Lookup),
                         (0L, "fghiz", Bitmap), (0.0, "hiz", Bitmap), (-0.0, "hiz", Bitmap), (-0.5, "f", Lookup), (0.5, "g", Lookup),
                         (9007199254740993L, "k", Bitmap), (9007199254740992.0, "k", Bitmap), (double.NaN, "", Bitmap)
                     })
                AssertIds(store, rql, value, expected, strategy);

            AssertIds(store, "from index 'Items/ByCompounds' where Name == 'ann' and Other == $v", 7L, "abcdefghijkz", Lookup);
            Assert.Equal([1, 1], Sorted(store, rql.Replace("$v", "1.0") + " order by Num as double", Lookup, null, out _));
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void NumericLookupTurnedDownForAKeyStartingWithAZeroByte()
        {
            using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
            new Items_ByNumAndOther().Execute(store);
            StoreNums(store, ("e", double.Epsilon), ("t", "ann"));

            AssertIds(store, "from index 'Items/ByNumAndOther' where Num == $v and Other == 7", double.Epsilon, "e", Bitmap);
        }

        // "@@@@@@@@" keys as the double 32.50196.., -4616189618054758400 as 1.0, -double.MaxValue as the long 2^52
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void NumericLookupTurnedDownWhereAnotherTypeKeysTheSameBytes()
        {
            using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
            new Items_ByNumAndOther().Execute(store);
            var at = BitConverter.Int64BitsToDouble(0x4040404040404040);
            StoreNums(store, ("t", "@@@@@@@@"), ("u", at), ("v", -4616189618054758400L), ("b", 1.0), ("w", -double.MaxValue), ("x", 4503599627370496L));

            const string rql = "from index 'Items/ByNumAndOther' where Num == $v and Other == 7";
            AssertIds(store, rql, at, "u", Bitmap);
            AssertIds(store, rql, 1.0, "b", Bitmap);
            AssertIds(store, rql, 4503599627370496L, "x", Bitmap);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void NumericLookupTurnedDownOnATimeField()
        {
            using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
            new Items_ByNumAndOther().Execute(store);
            StoreNums(store, ("o", "0010-01-01T02:00:00.0000000+02:00"));

            AssertIds(store, "from index 'Items/ByNumAndOther' where Num == $v and Other == 7", new DateTime(10, 1, 1).Ticks, "o", Bitmap);
        }

        // the client stores a NaN as text, an index can compute one: (long)NaN is 0
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void NumericLookupTurnedDownWhereANaNTruncatesToTheLong()
        {
            using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = "Items/ByNaNNumAndOther",
                Maps = { "from i in docs.Items select new { Num = i.Num > 4 ? (object)double.NaN : i.Num, i.Other }" },
                CompoundFields = [["Num", "Other"]]
            }));
            StoreNums(store, ("f", -0.5), ("h", 0L), ("n", 5.0));

            const string rql = "from index 'Items/ByNaNNumAndOther' where Num == $v and Other == 7";
            AssertIds(store, rql, 0L, "fhn", Bitmap);
            AssertIds(store, rql, -0.5, "f", Lookup);
        }

        // a char is indexed as its raw bytes analyzed (U+BAC8 as "ⱥ"), keyed by the other count of them
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void LookupTurnedDownForTextACharIsIndexedAs()
        {
            var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Name = "Items/ByCharGradeAndTag",
                Maps = { "from i in docs.Items select new { Grade = i.Num == 1 ? (object)'中' : i.Num == 3 ? (object)'뫈' : i.Name, i.Tag }" },
                CompoundFields = [["Grade", "Tag"]]
            }));

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Num = 1, Name = "x", Tag = "red" });
                session.Store(new Item { Num = 2, Name = "-n", Tag = "red" });
                session.Store(new Item { Num = 3, Name = "x", Tag = "red" });
                session.Store(new Item { Num = 4, Name = "ⱥ", Tag = "red" });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            const string rql = "from index 'Items/ByCharGradeAndTag' where Grade == $v and Tag == 'red'";
            AssertNums(options, store, rql, "-n", [1, 2], Bitmap);
            AssertNums(options, store, rql, "Ⱥ", [3, 4], Bitmap);
        }

        private void StoreNums(IDocumentStore store, params (string Id, object Num)[] items)
        {
            using (var session = store.OpenSession())
            {
                foreach (var (id, num) in items)
                {
                    object item = num switch
                    {
                        long l => new ItemWithLongNum { Num = l, Other = 7, Name = "Ann" },
                        string s => new ItemWithTextNum { Num = s, Other = 7 },
                        _ => new Item { Num = (double)num, Other = 7, Name = "Ann" }
                    };
                    session.Store(item, "items/" + id);
                    session.Advanced.GetMetadataFor(item)[Constants.Documents.Metadata.Collection] = "Items";
                }

                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
        }

        private static void AssertIds(IDocumentStore store, string rql, object value, string expected, string strategy)
        {
            var (baseline, _) = Query<ItemWithTextNum>(store, rql, value, force: Bitmap);
            var (actual, ran) = Query<ItemWithTextNum>(store, rql, value);
            string Letters(List<ItemWithTextNum> rows) => string.Concat(rows.Select(x => x.Id[^1]).OrderBy(x => x));
            Assert.Equal(expected, Letters(baseline));
            Assert.Equal(expected, Letters(actual));
            Assert.True(strategy == ran, $"expected {strategy}, ran {ran ?? "(no plan)"}: {rql} with {value}");
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

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void LookupTurnedDownWhereTheReaderSwapsTheBoolAnalyzer(Options options) =>
            AssertBoolLiteralLookup(options, new Items_ByNameNGramFlag(), "ru");

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void LookupTurnedDownWhereTheBoolLiteralSplitsIntoTokens(Options options) =>
            AssertBoolLiteralLookup(options, new Items_ByNameSplitDefaultFlag(), "tr");

        private void AssertBoolLiteralLookup(Options options, AbstractIndexCreationTask<Item> index, object value)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition { Name = "SplitOnUAnalyzer", Code = SplitOnUAnalyzerCode }));
            index.Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Num = 1, Name = "Ann", Flag = true });
                session.Store(new Item { Num = 2, Name = "Ann", Flag = false });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            AssertNums(options, store, $"from index '{index.IndexName}' where Name == 'ann' and Flag == $v", value, [1], Bitmap);
        }

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void LookupTurnedDownWhenACreatedFieldSharesTheName(Options options)
        {
            using var store = GetDocumentStore(options);
            new Items_ByNameTagAndCreatedName().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Item { Num = 1, Name = "Ann", Tag = "red" });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            AssertNums(options, store, "from index 'Items/ByNameTagAndCreatedName' where Name == 'red' and Tag == 'red'", null, [1], Bitmap);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void LookupSortStreamsInIndexOrderAndLearnsWithThePlan()
        {
            using var store = GetStoreWithFourItems(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
            const string rql = "from index 'Items/ByCompounds' where Name == 'ann' and Tag == 'red' order by Num as double";

            Assert.Equal([1.0, 1.1], Sorted(store, rql + " limit 2", Bitmap, null, out var sort));
            Assert.Equal("IndexOrderStreaming", sort?.Parameters.GetValueOrDefault("Strategy"));
            Assert.Equal([1.0, 1.1], Sorted(store, rql + " limit 2", Lookup, null, out sort));
            Assert.True(sort?.Parameters.ContainsKey("StreamScanInflation"), "the lookup corrects its estimate by the plan's bitmap run");
            Assert.Equal("IndexOrderStreaming", sort?.Parameters.GetValueOrDefault("Strategy"));
            Assert.Equal([1.0, 1.1, 1.5, 1.8], Sorted(store, rql, Lookup, "IndexOrderStreaming", out sort)); // no limit: only the pin streams
            Assert.Equal("IndexOrderStreaming", sort?.Parameters.GetValueOrDefault("Strategy"));
        }

        private static List<double> Sorted(IDocumentStore store, string rql, string strategy, string sortPin, out QueryInspectionNode sort)
        {
            using var session = store.OpenSession();
            var query = session.Advanced.RawQuery<Item>(rql + " include timings()").NoCaching().Timings(out QueryTimings timings);
            if (strategy == Bitmap)
                query.AddParameter("rvn_corax_strategy", Bitmap);
            if (sortPin != null)
                query.AddParameter("rvn_corax_sort", sortPin);

            var nums = query.ToList().Select(x => x.Num).ToList();
            var plan = timings.QueryPlan as QueryInspectionNode;
            Assert.Equal(strategy, FindNode(plan, "CompiledQuery")?.Parameters.GetValueOrDefault("OptimizationHint"));
            sort = FindNode(plan, "SortingMatch");
            return nums;
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
        }

        private static QueryInspectionNode FindNode(QueryInspectionNode node, string operation) =>
            node == null ? null : node.Operation == operation ? node : node.Children?.Select(c => FindNode(c, operation)).FirstOrDefault(n => n != null);
    }
}
