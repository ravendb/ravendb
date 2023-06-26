using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax.Queries;
using Corax;
using Corax.Mappings;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Voron;
using Xunit.Abstractions;
using Xunit;
using Sparrow.Threading;


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

        [Fact]
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

        [Fact]
        public void UnaryMatchWithSequential()
        {
            PrepareData();
            IndexEntries();
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            Slice.From(ctx, "3", out var three);
            var match0 = searcher.AllEntries();
            var match1 = searcher.UnaryQuery(match0, _longItemFieldMetadata, three, UnaryMatchOperation.GreaterThan);
            var expectedList = GetExpectedResult("3");
            expectedList.Sort();
            var outputList = FetchFromCorax(ref match1);
            outputList.Sort();
            Assert.Equal(expectedList.Count, outputList.Count);
            for (int i = 0; i < expectedList.Count; ++i)
                Assert.Equal(expectedList[i], outputList[i]);
        }

        [Fact]
        public void LexicographicalLessThan()
        {
            PrepareData(1);
            IndexEntries();
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            Slice.From(ctx, "entries/0", out var id);
            var match0 = searcher.AllEntries();
            var match1 = searcher.UnaryQuery(match0, searcher.FieldMetadataBuilder("Id", IndexId), id, UnaryMatchOperation.LessThan);
            var ids = new long[16];
            int read = match1.Fill(ids);
            Assert.Equal(0, read);
        }

        [Fact]
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

        [Fact]
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

        [Fact]
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


        [Fact]
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


        [Fact]
        public void UnaryMatchWithNumerical()
        {
            PrepareData();
            IndexEntries();
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            var match0 = searcher.AllEntries();
            var match1 = searcher.UnaryQuery<AllEntriesMatch, long>(match0, _longItemFieldMetadata, 3, UnaryMatchOperation.GreaterThan);
            var expectedList = _entries.Where(x => x.LongValue > 3).Select(x => x.Id).ToList();
            expectedList.Sort();
            var outputList = FetchFromCorax(ref match1);
            outputList.Sort();
            Assert.Equal(expectedList.Count, outputList.Count);
            for (int i = 0; i < expectedList.Count; ++i)
                Assert.Equal(expectedList[i], outputList[i]);
        }

        [Fact]
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

        [Fact]
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
        
        [Fact]
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

        private List<string> FetchFromCorax<TMatch>(ref TMatch match)
            where TMatch : IQueryMatch
        {
            using var indexSearcher = new IndexSearcher(Env, _knownFields);

            List<string> list = new();
            Span<long> ids = stackalloc long[256];
            int read = match.Fill(ids);
            while (read != 0)
            {
                for (int i = 0; i < read; ++i)
                {
                    long id = ids[i];
                    list.Add(indexSearcher.TermsReaderFor(indexSearcher.GetFirstIndexedFiledName()).GetTermFor(id));
                }

                read = match.Fill(ids);
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
            
            using var indexWriter = new IndexWriter(Env, _knownFields);
            var entryWriter = new IndexEntryWriter(bsc, _knownFields);

            foreach (var entry in _entries)
            {
                using var __ = CreateIndexEntry(ref entryWriter, entry, out var data);
                indexWriter.Index(entry.Id,data.ToSpan());
            }

            indexWriter.PrepareAndCommit();
        }

        private ByteStringContext<ByteStringMemoryCache>.InternalScope CreateIndexEntry(
            ref IndexEntryWriter entryWriter, Entry entry, out ByteString output)
        {
            entryWriter.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
            entryWriter.Write(LongValueId, Encoding.UTF8.GetBytes(entry.LongValue.ToString()), entry.LongValue, entry.LongValue);
            entryWriter.Write(DoubleValueId, Encoding.UTF8.GetBytes(entry.DoubleValue.ToString()), (long)entry.DoubleValue, entry.DoubleValue);
            entryWriter.Write(TextualValueId, Encodings.Utf8.GetBytes(entry.TextualValue));
            return entryWriter.Finish(out output);
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

        private List<string> FetchFromCorax(ref UnaryMatch match)
        {
            using var indexSearcher = new IndexSearcher(Env, _knownFields);
            List<string> list = new();
            Span<long> ids = stackalloc long[256];
            int read = match.Fill(ids);
            while (read != 0)
            {
                for (int i = 0; i < read; ++i)
                {
                    long id = ids[i];
                    list.Add(indexSearcher.TermsReaderFor(indexSearcher.GetFirstIndexedFiledName()).GetTermFor(id));
                }

                read = match.Fill(ids);
            }

            return list;
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
