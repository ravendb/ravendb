using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Queries;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;
using static Corax.Queries.SortingMatch;

namespace FastTests.Corax
{

    public class OrderByMultiSortingTests : StorageTest
    {
        private readonly List<IndexSingleNumericalEntry<long, long>> longList = new();
        private const int IndexId = 0, Content1 = 1, Content2 = 2;
        private readonly long[] _buffer = new long[200_005];
        public OrderByMultiSortingTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void OrderByNoRepetitions()
        {
            PrepareData();

            IndexEntries();
            longList.Sort(CompareDescending);
            using var searcher = new IndexSearcher(Env);
            {
                var match1 = searcher.AllEntries();

                var comparer1 = new DescendingMatchComparer(searcher, Content1, MatchCompareFieldType.Integer);
                var comparer2 = new AscendingMatchComparer(searcher, Content2, MatchCompareFieldType.Integer);
                var match = SortingMultiMatch.Create(searcher, match1, comparer1, comparer2);

                List<string> sortedByCorax = new();
                Span<long> ids = _buffer;
                int read = 0;
                do
                {
                    read = match.Fill(ids);
                    for (int i = 0; i < read; ++i)
                        sortedByCorax.Add(searcher.GetIdentityFor(ids[i]));
                }
                while (read != 0);

                for (int i = 0; i < longList.Count; ++i)
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);

                Assert.Equal(100_000, sortedByCorax.Count);
            }
        }

        [Fact]
        public void OrderByWithRepetitions()
        {
            PrepareData();
            PrepareData(inverse:true);

            IndexEntries();
            longList.Sort(CompareAscendingThenDescending);
            using var searcher = new IndexSearcher(Env);
            {
                var match1 = searcher.AllEntries();

                var comparer1 = new AscendingMatchComparer(searcher, Content1, MatchCompareFieldType.Integer);
                var comparer2 = new DescendingMatchComparer(searcher, Content2, MatchCompareFieldType.Integer);
               
                var match = SortingMultiMatch.Create(searcher, match1, comparer1, comparer2);

                List<string> sortedByCorax = new();
                Span<long> ids = _buffer;
                int read = 0;
                do
                {
                    read = match.Fill(ids);
                    for (int i = 0; i < read; ++i)
                        sortedByCorax.Add(searcher.GetIdentityFor(ids[i]));
                }
                while (read != 0);

                for (int i = 0; i < longList.Count; ++i)
                {                    
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);
                }
                    

                Assert.Equal(100_000 * 2, sortedByCorax.Count);
            }
        }

        [Fact]
        public void WithBoosting()
        {
            for (int i = 0; i < 100_000; ++i)            
            {
                longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/{i}", Content1 = i % 2137, });
            }

            IndexEntries();
            longList.Sort(CompareDescending);
            using var searcher = new IndexSearcher(Env);
            {
                //var match = searcher.Or(searcher.Boost(searcher.GreaterThan(searcher.AllEntries(), Content1, 2137), 1000),
                //    searcher.LessThan(searcher.AllEntries(), Content1, 99L));
                var match = searcher.Boost(searcher.UnaryQuery(searcher.AllEntries(), Content1, 2137, UnaryMatchOperation.GreaterThanOrEqual), 1000);
                var sorted = SortingMultiMatch.Create(searcher, match, default(BoostingComparer),
                    new AscendingMatchComparer(searcher, IndexId, MatchCompareFieldType.Sequence));
                var read = sorted.Fill(_buffer);

                var localResult = longList.Where(x => x.Content1 >= 2137).OrderBy(o => o.Content1).ThenBy(o => o.Id).Select(ll => ll.Id).ToList();
                Assert.Equal(localResult.Count, read);

                var realIds = new List<string>();
                for (var i = 0; i < localResult.Count; ++i)
                    realIds.Add(searcher.GetIdentityFor(_buffer[i]));

                Assert.True(localResult.SequenceEqual(realIds));
            }
        }

        private static int CompareAscending(IndexSingleNumericalEntry<long, long> value1, IndexSingleNumericalEntry<long, long> value2)
        {
            return value1.Content1.CompareTo(value2.Content1);
        }

        private static int CompareAscendingThenDescending(IndexSingleNumericalEntry<long, long> value1, IndexSingleNumericalEntry<long, long> value2)
        {
            var result = value1.Content1.CompareTo(value2.Content1);
            if (result == 0)
                return value2.Content2.CompareTo(value1.Content2);
            return result;
        }

        private static int CompareDescending(IndexSingleNumericalEntry<long, long> value1, IndexSingleNumericalEntry<long, long> value2)
        {
            return value2.Content1.CompareTo(value1.Content1);
        }

        private void PrepareData(bool inverse = false)
        {
            for (int i = 0; i < 100_000; ++i)
            {
                longList.Add(new IndexSingleNumericalEntry<long, long>
                {
                    Id = inverse ? $"list/1000-{i}" : $"list/{i}",
                    Content1 = i,
                    Content2 = inverse ? 100_000 - i : i
                });
            }
        }

        private void IndexEntries()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var knownFields = CreateKnownFields(bsc);

            const int bufferSize = 4096;
            using var _ = bsc.Allocate(bufferSize, out ByteString buffer);

            {
                using var indexWriter = new IndexWriter(Env, knownFields);
                foreach (var entry in longList)
                {
                    var entryWriter = new IndexEntryWriter(buffer.ToSpan(), knownFields);
                    var data = CreateIndexEntry(ref entryWriter, entry);
                    indexWriter.Index(entry.Id, data);
                }
                indexWriter.Commit();
            }
        }

        private Span<byte> CreateIndexEntry(ref IndexEntryWriter entryWriter, IndexSingleNumericalEntry<long, long> entry)
        {
            entryWriter.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
            entryWriter.Write(Content1, Encoding.UTF8.GetBytes(entry.Content1.ToString()), entry.Content1, Convert.ToDouble(entry.Content1));
            entryWriter.Write(Content2, Encoding.UTF8.GetBytes(entry.Content2.ToString()), entry.Content2, Convert.ToDouble(entry.Content2));
            entryWriter.Finish(out var output);
            return output;
        }

        private IndexFieldsMapping CreateKnownFields(ByteStringContext bsc)
        {
            Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(bsc, "Content1", ByteStringType.Immutable, out Slice content1Slice);
            Slice.From(bsc, "Content2", ByteStringType.Immutable, out Slice content2Slice);

            return new IndexFieldsMapping(bsc)
                        .AddBinding(IndexId, idSlice)
                        .AddBinding(Content1, content1Slice)
                        .AddBinding(Content2, content2Slice);
        }

        private class IndexSingleNumericalEntry<T1, T2>
        {
            public string Id { get; set; }
            public T1 Content1 { get; set; }
            public T2 Content2 { get; set; }
        }
    }
}
