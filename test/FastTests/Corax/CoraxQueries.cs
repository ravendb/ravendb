using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax.Queries;
using Corax;
using Corax.IndexEntry;
using FastTests.Voron;
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
        private const int IndexId = 0, LongValue = 1;
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
            Slice.From(ctx, "Content", out var fieldName);
            Slice.From(ctx, "3", out var three);
            var match1 = searcher.GreaterThanQuery(fieldName, three);
            var expectedList = GetExpectedResult("3");
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
            Slice.From(ctx, "0", out var id);
            Slice.From(ctx, "Content", out var field);
            var match1 = searcher.LessThanQuery(field, id);
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
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);

            _knownFields = CreateKnownFields(ctx);

            const int bufferSize = 4096;
            using var _ = ctx.Allocate(bufferSize, out ByteString buffer);

            {
                using var indexWriter = new IndexWriter(Env, _knownFields);
                foreach (var entry in _entries)
                {
                    var entryWriter = new IndexEntryWriter(buffer.ToSpan(), _knownFields);
                    var data = CreateIndexEntry(ref entryWriter, entry);
                    indexWriter.Index(entry.Id, data);
                }

                indexWriter.Commit();
            }
        }

        private Span<byte> CreateIndexEntry(ref IndexEntryWriter entryWriter, Entry entry)
        {
            entryWriter.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
            entryWriter.Write(LongValue, Encoding.UTF8.GetBytes(entry.LongValue.ToString()), entry.LongValue, entry.LongValue);
            entryWriter.Finish(out var output);
            return output;
        }

        private IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
        {
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice longSlice);

            return new IndexFieldsMapping(ctx)
                        .AddBinding(IndexId, idSlice)
                        .AddBinding(LongValue, longSlice);
        }
        
        private void PrepareData(int size = 1000)
        {
            _entries ??= new();
            for (int i = 0; i < size; ++i)
            {
                _entries.Add(new Entry()
                {
                    Id = $"entries/{i}",
                    LongValue = i
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
        
        private class Entry
        {
            public string Id { get; set; }
            
            public long LongValue { get; set; }
        }
    }
}
