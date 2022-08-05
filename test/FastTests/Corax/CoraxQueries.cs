using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax.Queries;
using Corax;
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
        private const int IndexId = 0, LongValue = 1, DoubleValue = 2, TextualValue = 3;
        private IndexFieldsMapping _knownFields;
        public CoraxQueries(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GreaterThanQuery()
        {
            PrepareData();
            IndexEntries();
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env);
            var match1 = searcher.GreaterThanQuery<long, NullScoreFunction>("Content", 3, default);
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
            using var searcher = new IndexSearcher(Env);
            Slice.From(ctx, "3", out var three);
            var match0 = searcher.AllEntries();
            var match1 = searcher.UnaryQuery(match0, LongValue, three, UnaryMatchOperation.GreaterThan);
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
            using var searcher = new IndexSearcher(Env);
            Slice.From(ctx, "entries/0", out var id);
            var match0 = searcher.AllEntries();
            var match1 = searcher.UnaryQuery(match0, IndexId, id, UnaryMatchOperation.LessThan);
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
            using var searcher = new IndexSearcher(Env);
            var match1 = searcher.LessThanQuery<long, NullScoreFunction>("Content", 0, default);
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
            using var searcher = new IndexSearcher(Env);
            Slice.From(ctx, "991", out var low);
            Slice.From(ctx, "995", out var high);
            Slice.From(ctx, "Content", out var field);

            var match1 = searcher.BetweenQuery(field, low, high);
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
            using var searcher = new IndexSearcher(Env);
            Slice.From(ctx, "Content", out var field);
            Slice.From(ctx, "Content-L", out var fieldLong);


            var match1 = searcher.BetweenQuery(field, fieldLong, 95, 212);
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
            using var searcher = new IndexSearcher(Env);
            Slice.From(ctx, "Content", out var field);
            Slice.From(ctx, "Content-D", out var fieldLong);


            var match1 = searcher.BetweenQuery(field, fieldLong, 95.2, 213.2);
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
            using var searcher = new IndexSearcher(Env);
      
            var match0 = searcher.AllEntries();
            var match1 = searcher.UnaryQuery<AllEntriesMatch, long>(match0, LongValue, 3, UnaryMatchOperation.GreaterThan);
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
            using var searcher = new IndexSearcher(Env);
      
            var match0 = searcher.AllEntries();
            var comparers = new MultiUnaryItem[] {new(LongValue, 3L, UnaryMatchOperation.GreaterThan), new(DoubleValue, 20.5, UnaryMatchOperation.LessThan)};
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
        public void MultiTermMatchWithTermMatch()
        {
            PrepareData();
            IndexEntries();
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env);
            
            var match0 = searcher.TermQuery("Content", "1");
            var match1 = searcher.StartWithQuery("Id", "ent");
            var multiTermTerm = searcher.And(match1, match0);
            var first = FetchFromCorax(ref multiTermTerm);
            Assert.Equal(1, first.Count);

            match0 = searcher.TermQuery("Content", "1");
            match1 = searcher.StartWithQuery("Id", "ent");
            var termMultiTerm = searcher.And(match0, match1);
            var second = FetchFromCorax(ref termMultiTerm);
            Assert.Equal(1, second.Count);


            Assert.True(first.SequenceEqual(second));
        }

        private List<string> FetchFromCorax<TMatch>(ref TMatch match)
            where TMatch : IQueryMatch
        {
            using var indexSearcher = new IndexSearcher(Env);
            List<string> list = new();
            Span<long> ids = stackalloc long[256];
            int read = match.Fill(ids);
            while (read != 0)
            {
                for (int i = 0; i < read; ++i)
                    list.Add(indexSearcher.GetIdentityFor(ids[i]));
                read = match.Fill(ids);
            }

            return list;
        }

        private void IndexEntries()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            _knownFields = CreateKnownFields(bsc);

            using var indexWriter = new IndexWriter(Env, _knownFields);
            var entryWriter = new IndexEntryWriter(bsc, _knownFields);

            foreach (var entry in _entries)
            {
                using var __ = CreateIndexEntry(ref entryWriter, entry, out var data);
                indexWriter.Index(entry.Id, data.ToSpan());
                entryWriter.Reset();
            }

            indexWriter.Commit();
        }

        private ByteStringContext<ByteStringMemoryCache>.InternalScope CreateIndexEntry(
            ref IndexEntryWriter entryWriter, Entry entry, out ByteString output)
        {
            entryWriter.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
            entryWriter.Write(LongValue, Encoding.UTF8.GetBytes(entry.LongValue.ToString()), entry.LongValue, entry.LongValue);
            entryWriter.Write(DoubleValue, Encoding.UTF8.GetBytes(entry.DoubleValue.ToString()), (long)entry.DoubleValue, entry.DoubleValue);
            entryWriter.Write(TextualValue, Encodings.Utf8.GetBytes(entry.TextualValue));
            return entryWriter.Finish(out output);
        }

        private IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
        {
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice longSlice);
            Slice.From(ctx, "DoubleItem", ByteStringType.Immutable, out Slice doubleSlice);
            Slice.From(ctx, "TextualItem", ByteStringType.Immutable, out Slice textualSlice);

            return new IndexFieldsMapping(ctx)
                        .AddBinding(IndexId, idSlice)
                        .AddBinding(LongValue, longSlice)
                        .AddBinding(DoubleValue, doubleSlice)
                        .AddBinding(TextualValue, textualSlice);
        }

        private const int seed = 1000;
        private void PrepareData(int size = 1000)
        {
            var random = new Random(seed);
            _entries ??= new();
            for (int i = 0; i < size; ++i)
            {
                _entries.Add(new Entry()
                {
                    Id = $"entries/{i}",
                    LongValue = i,
                    DoubleValue = i * random.NextDouble(),
                    TextualValue = i % 2 == 0 ? "abc" : "cde" 
                });
            }
        }

        private List<string> FetchFromCorax(ref UnaryMatch match)
        {
            using var indexSearcher = new IndexSearcher(Env);
            List<string> list = new();
            Span<long> ids = stackalloc long[256];
            int read = match.Fill(ids);
            while (read != 0)
            {
                for(int i = 0; i < read; ++i)
                    list.Add(indexSearcher.GetIdentityFor(ids[i]));
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
    }
}
