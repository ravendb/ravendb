using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;
using Raven.Server.Documents.Queries;
using Sparrow;
using Sparrow.Server;
using Voron;
using Xunit;
using Sparrow.Threading;
using Tests.Infrastructure;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;


namespace FastTests.Corax
{
    public class CoraxQueries : StorageTest
    {
        private List<Entry> _entries;
        private const int IndexId = 0, LongValueId = 1, DoubleValueId = 2, TextualValueId = 3;
        private IndexFieldsMapping _knownFields;
        private FieldMetadata _longItemFieldMetadata, _doubleItemFieldMetadata, _textualItemFieldMetadata;
        public CoraxQueries(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void GreaterThanQuery()
        {
            PrepareData();
            IndexEntries();
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var match1 = searcher.GreaterThanQuery<long>(_longItemFieldMetadata, 3);
            var expectedList = GetExpectedResult(3);
            expectedList.Sort();
            var outputList = FetchFromCorax(ref match1);
            outputList.Sort();
            Assert.Equal(expectedList.Count, outputList.Count);
            for (int i = 0; i < expectedList.Count; ++i)
                Assert.Equal(expectedList[i], outputList[i]);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void UnaryMatchWithSequential()
        {
            PrepareData();
            IndexEntries();
            using var searcher = new IndexSearcher(Env, _knownFields);
            // String greater-than comparison: entries where the stored LongItem term > "3" lexicographically.
            // GreaterThanQuery(fieldMeta, "3") performs a CompactTree range scan.
            var match1 = searcher.GreaterThanQuery(_longItemFieldMetadata, "3");
            var expectedList = GetExpectedResult("3");
            expectedList.Sort();
            var outputList = FetchFromCorax(ref match1);
            outputList.Sort();
            Assert.Equal(expectedList.Count, outputList.Count);
            for (int i = 0; i < expectedList.Count; ++i)
                Assert.Equal(expectedList[i], outputList[i]);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void LexicographicalLessThan()
        {
            PrepareData(1);
            IndexEntries();
            using var searcher = new IndexSearcher(Env, _knownFields);
            // LessThanQuery on "Id" field: entries where Id < 'entries/0' lexicographically.
            // Only one entry exists ("entries/0"), so nothing is less than it.
            var match1 = searcher.LessThanQuery(searcher.FieldMetadataBuilder("Id", IndexId), "entries/0");
            var ids = new long[16];
            int read = match1.Fill(ids);
            Assert.Equal(0, read);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void LexicographicalLessThanQuery()
        {
            PrepareData(1);
            IndexEntries();
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            var match1 = searcher.LessThanQuery<long>(_longItemFieldMetadata, 0);
            var ids = new long[16];
            int read = match1.Fill(ids);
            Assert.Equal(0, read);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void BetweenQuery()
        {
            PrepareData();
            IndexEntries();
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            Slice.From(ctx, "991", out var low);
            Slice.From(ctx, "995", out var high);

            var match1 = searcher.BetweenQuery(_longItemFieldMetadata, low.ToString(), high.ToString());
            var expectedList = _entries.Where(x => x.LongValue is >= 991 and <= 995).Select(x => x.Id).ToList();
            expectedList.Sort();
            var outputList = FetchFromCorax(ref match1);
            outputList.Sort();
            Assert.Equal(expectedList.Count, outputList.Count);
            for (int i = 0; i < expectedList.Count; ++i)
                Assert.Equal(expectedList[i], outputList[i]);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void BetweenQueryNumeric()
        {
            PrepareData();
            IndexEntries();
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            var match1 = searcher.BetweenQuery(_longItemFieldMetadata, 95L, 212L);
            var expectedList = _entries.Where(x => x.LongValue is >= 95 and <= 212).Select(x => x.Id).ToList();
            expectedList.Sort();
            var outputList = FetchFromCorax(ref match1);
            outputList.Sort();
            Assert.Equal(expectedList.Count, outputList.Count);
            for (int i = 0; i < expectedList.Count; ++i)
                Assert.Equal(expectedList[i], outputList[i]);
        }


        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void BetweenQueryNumericDouble()
        {
            PrepareData();
            IndexEntries();
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            var match1 = searcher.BetweenQuery(_longItemFieldMetadata, 95.2, 213.2);
            var expectedList = _entries.Where(x => (double)x.LongValue is >= 95.2 and <= 213.2).Select(x => x.Id).ToList();
            expectedList.Sort();
            var outputList = FetchFromCorax(ref match1);
            outputList.Sort();
            Assert.Equal(expectedList.Count, outputList.Count);
            for (int i = 0; i < expectedList.Count; ++i)
                Assert.Equal(expectedList[i], outputList[i]);
        }


        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void UnaryMatchWithNumerical()
        {
            PrepareData();
            IndexEntries();
            using var searcher = new IndexSearcher(Env, _knownFields);
            var match1 = searcher.GreaterThanQuery<long>(_longItemFieldMetadata, 3);
            var expectedList = _entries.Where(x => x.LongValue > 3).Select(x => x.Id).ToList();
            expectedList.Sort();
            var outputList = FetchFromCorax(ref match1);
            outputList.Sort();
            Assert.Equal(expectedList.Count, outputList.Count);
            for (int i = 0; i < expectedList.Count; ++i)
                Assert.Equal(expectedList[i], outputList[i]);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void MultiUnaryMatchWithNumerical()
        {
            PrepareData();
            IndexEntries();
            var expectedList = _entries.Where(x => x.LongValue > 3 && x.DoubleValue < 20.5).Select(x => x.Id).ToList();
            expectedList.Sort();
            var outputList = ExecuteRQLQuery("FROM TestIndex WHERE LongItem > 3 AND DoubleItem < 20.5");
            outputList.Sort();
            Assert.Equal(expectedList.Count, outputList.Count);
            for (int i = 0; i < expectedList.Count; ++i)
                Assert.Equal(expectedList[i], outputList[i]);
        }
        
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void MultiUnaryMatchWithMultipleInnerFillCalls()
        {
            _entries = new List<Entry>();
            for (var idX = 0; idX < 32; ++idX)
                _entries.Add(new Entry() {Id = $"entries/0", LongValue = idX + 1, DoubleValue = 0.0, TextualValue = "abc" });
            
            IndexEntries();

            var expectedList = _entries.Where(x => x.LongValue > 18).Select(x => x.Id).ToList();
            expectedList.Sort();

            var outputList = ExecuteRQLQuery("FROM TestIndex WHERE TextualItem = 'abc' AND LongItem > 18");
            outputList.Sort();
            Assert.Equal(expectedList.Count, outputList.Count);
            for (int i = 0; i < expectedList.Count; ++i)
                Assert.Equal(expectedList[i], outputList[i]);
        }
        
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void MultiUnaryMatchAndWithMultipleCalls()
        {
            _entries = new List<Entry>();
            for (var idX = 0; idX < 32; ++idX)
                _entries.Add(new Entry() {Id = $"entries/0", LongValue = idX + 1, DoubleValue = 0.0, TextualValue = $"abc{idX}" });
            
            IndexEntries();

            var totalResults = ExecuteRQLQuery("FROM TestIndex WHERE LongItem > 18").Count;
            Assert.Equal(_entries.Count(x => x.LongValue > 18), totalResults);
        }
        
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void CanDoNumericalTermMatch()
        {
            _entries = new List<Entry>();
            _entries.Add(new Entry() {Id = $"entries/0", LongValue = 0, DoubleValue = 0.0, TextualValue = "abc" });
            IndexEntries();
            
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var match0 = searcher.TermQuery(_doubleItemFieldMetadata, 0.0D);
            var ids = new long[16];
            Assert.Equal(1, match0.Fill(ids)); //match one doc

            var match1 = searcher.TermQuery(_doubleItemFieldMetadata, 0L);
            Assert.Equal(1, match1.Fill(ids)); //match one doc
            
            //Lets assert also longs:
            var match2 = searcher.TermQuery(_longItemFieldMetadata, 0.0D);
            Assert.Equal(1, match2.Fill(ids)); //match one doc

            var match3 = searcher.TermQuery(_longItemFieldMetadata, 0L);
            Assert.Equal(1, match3.Fill(ids)); //match one doc
        }
        
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void MultiTermMatchWithTermMatch()
        {
            PrepareData();
            IndexEntries();

            // Both orderings of AND must return the same single result.
            // entry 1 has LongValue=1 (odd → "cde"), so both conditions match only entries/1.
            var first = ExecuteRQLQuery("FROM TestIndex WHERE LongItem = 1 AND TextualItem = 'cde'");
            Assert.Equal(1, first.Count);

            var second = ExecuteRQLQuery("FROM TestIndex WHERE TextualItem = 'cde' AND LongItem = 1");
            Assert.Equal(1, second.Count);

            Assert.True(first.SequenceEqual(second));
        }

        /// <summary>
        /// RavenDB-22603: Test that optimized AndNot works correctly with TermMatch as base.
        /// This test has an Equals anchor (TermQuery) which allows the MultiUnaryMatch optimization.
        /// </summary>
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void AndNotWithTermMatchAsBase()
        {
            PrepareData();
            IndexEntries();

            // Entries where TextualValue == "cde" AND LongValue != 1
            var results = ExecuteRQLQuery("FROM TestIndex WHERE TextualItem = 'cde' AND NOT LongItem = 1");
            var expected = _entries.Where(e => e.TextualValue == "cde" && e.LongValue != 1).Select(e => e.Id).ToList();

            results.Sort();
            expected.Sort();

            Assert.Equal(expected.Count, results.Count);
            Assert.True(expected.SequenceEqual(results));
        }

        private List<string> ExecuteRQLQuery(string rqlQuery)
        {
            using var searcher = new IndexSearcher(Env, _knownFields);
            var queryMetadata = new QueryMetadata(rqlQuery, null, 0);
            var planParams = new PlanParameters
            {
                IndexSearcher = searcher,
                Metadata = queryMetadata,
                QueryParameters = null,
                Allocator = Allocator
            };
            var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, null, _knownFields), null, false, default);

            var list = new List<string>();
            Span<long> ids = stackalloc long[256];
            int count;
            while ((count = match.Fill(ids)) > 0)
            {
                for (int i = 0; i < count; i++)
                    list.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(ids[i]));
            }
            return list;
        }

        private List<string> FetchFromCorax<TMatch>(ref TMatch match, int batchSize = 256)
            where TMatch : IQueryMatch
        {
            using var indexSearcher = new IndexSearcher(Env, _knownFields);

            List<string> list = new();
            Span<long> ids = stackalloc long[batchSize];
            HashSet<long> test = new();
            int read = match.Fill(ids);
            var it = 1;
            while (read != 0)
            {
                for (int i = 0; i < read; ++i)
                {
                    long id = ids[i];
                    list.Add(indexSearcher.TermsReaderFor(indexSearcher.GetFirstIndexedFiledName()).GetTermFor(id));
                    if (test.Add(id) == false)
                        Debugger.Break();
                }

                read = match.Fill(ids);
                it++;
            }

            return list;
        }

        private void IndexEntries()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            _knownFields = CreateKnownFields(bsc);
            
            _knownFields.TryGetByFieldId(LongValueId, out var binding);
            _longItemFieldMetadata = binding.Metadata;
            
            _knownFields.TryGetByFieldId(DoubleValueId, out binding);
            _doubleItemFieldMetadata = binding.Metadata;
            
            _knownFields.TryGetByFieldId(TextualValueId, out binding);
            _textualItemFieldMetadata = binding.Metadata;
            
            using var indexWriter = new IndexWriter(Env, _knownFields, SupportedFeatures.All);

            foreach (var entry in _entries)
            {
                using var entryBuilder = indexWriter.Index(entry.Id);
                entryBuilder.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
                entryBuilder.Write(LongValueId, Encoding.UTF8.GetBytes(entry.LongValue.ToString()), entry.LongValue, entry.LongValue);
                entryBuilder.Write(DoubleValueId, Encoding.UTF8.GetBytes(entry.DoubleValue.ToString()), (long)entry.DoubleValue, entry.DoubleValue);
                entryBuilder.Write(TextualValueId, Encodings.Utf8.GetBytes(entry.TextualValue));
                entryBuilder.EndWriting();
            }

            indexWriter.Commit();
        }

        private IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
        {
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "LongItem", ByteStringType.Immutable, out Slice longSlice);
            Slice.From(ctx, "DoubleItem", ByteStringType.Immutable, out Slice doubleSlice);
            Slice.From(ctx, "TextualItem", ByteStringType.Immutable, out Slice textualSlice);

            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IndexId, idSlice)
                .AddBinding(LongValueId, longSlice)
                .AddBinding(DoubleValueId, doubleSlice)
                .AddBinding(TextualValueId, textualSlice);
            return builder.Build();
        }

        private const int seed = 1000;

        private void PrepareData(int size = 1000)
        {
            var random = new Random(seed);
            _entries ??= new();
            for (int i = 0; i < size; ++i)
            {
                _entries.Add(new Entry() {Id = $"entries/{i}", LongValue = i, DoubleValue = i * random.NextDouble(), TextualValue = i % 2 == 0 ? "abc" : "cde"});
            }
        }
        
        private List<string> GetExpectedResult(string input)
        {
            return _entries.Where(entry => entry.LongValue.ToString().CompareTo(input) == 1).Select(x => x.Id).ToList();
        }

        private List<string> GetExpectedResult(long input)
        {
            return _entries.Where(entry => entry.LongValue > input).Select(x => x.Id).ToList();
        }

        private class Entry
        {
            public string Id { get; set; }

            public long LongValue { get; set; }

            public double DoubleValue { get; set; }

            public string TextualValue { get; set; }
        }

        public override async ValueTask DisposeAsync()
        {
            await base.DisposeAsync();
            _knownFields?.Dispose();
        }
    }
}
