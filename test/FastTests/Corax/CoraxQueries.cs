using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Voron;
using Xunit.Abstractions;
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
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var match0 = searcher.AllEntries();
            var match1 = searcher.CreateMultiUnaryMatch(match0, [(new MultiUnaryItem(searcher, _longItemFieldMetadata, "3", UnaryMatchOperation.GreaterThan))]);
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
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            Slice.From(ctx, "entries/0", out var id);
            var match0 = searcher.AllEntries();
            var match1 = searcher.CreateMultiUnaryMatch(match0, [(new MultiUnaryItem(searcher, searcher.FieldMetadataBuilder("Id", IndexId), "entries/0", UnaryMatchOperation.LessThan))]);
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
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            var match0 = searcher.AllEntries();
            var match1 = searcher.CreateMultiUnaryMatch(match0, new[] { new MultiUnaryItem(_longItemFieldMetadata, 3L, UnaryMatchOperation.GreaterThan) });
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
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            
            var match0 = searcher.AllEntries();
            var comparers = new MultiUnaryItem[] {new(_longItemFieldMetadata, 3L, UnaryMatchOperation.GreaterThan), new(_doubleItemFieldMetadata, 20.5, UnaryMatchOperation.LessThan)};
            var match1 = searcher.CreateMultiUnaryMatch(match0, comparers);
            var expectedList = _entries.Where(x => x.LongValue > 3 && x.DoubleValue < 20.5).Select(x => x.Id).ToList();
            expectedList.Sort();
            var outputList = FetchFromCorax(ref match1);
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
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            
            var match0 = searcher.TermQuery(_textualItemFieldMetadata, "abc"); //should return list [1, n]
            
            var comparers = new MultiUnaryItem[] {new(_longItemFieldMetadata, 18, UnaryMatchOperation.GreaterThan)};
            var match1 = searcher.CreateMultiUnaryMatch(match0, comparers);
            
            var expectedList = _entries.Where(x => x.LongValue > 18).Select(x => x.Id).ToList();
            expectedList.Sort();
            
            //Batch size must be small, since we expect Fill to return at least 1 element in the first call, otherwise it may affect the correctness of the result.
            var outputList = FetchFromCorax(ref match1, batchSize: 8);
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
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            var match0 = searcher.ExistsQuery(_textualItemFieldMetadata); //should return list [1, n]
            var comparers = new MultiUnaryItem[] {new(_longItemFieldMetadata, 18, UnaryMatchOperation.GreaterThan)};
            var match1 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), comparers);

            Span<long> ids = stackalloc long[16];
            var totalResults = 0;
            while (match0.Fill(ids) is var read and > 0)
            {
                totalResults += match1.AndWith(ids, read);
            }
            
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
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            var match0 = searcher.TermQuery(_longItemFieldMetadata, "1");
            var match1 = searcher.StartWithQuery("Id", "ent");
            var multiTermTerm = searcher.And(match1, match0);
            var first = FetchFromCorax(ref multiTermTerm);
            Assert.Equal(1, first.Count);

            match0 = searcher.TermQuery(_longItemFieldMetadata, "1");
            match1 = searcher.StartWithQuery("Id", "ent");
            var termMultiTerm = searcher.And(match0, match1);
            var second = FetchFromCorax(ref termMultiTerm);
            Assert.Equal(1, second.Count);


            Assert.True(first.SequenceEqual(second));
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

        public override void Dispose()
        {
            base.Dispose();
            _knownFields?.Dispose();
        }
    }
}
