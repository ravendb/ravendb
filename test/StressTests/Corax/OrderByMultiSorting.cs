using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Querying;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace StressTests.Corax
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

            // Since there are no repetition, Ascending must not trigger, if it does it is showing an error in the implementation
            // of the sorter logic.
            longList.Sort(CompareDescendingThenAscending);

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                var match1 = searcher.AllEntries();
                var orderMetadata = new OrderMetadata[2]
                {
                    new(searcher.FieldMetadataBuilder("Content1", Content1), false, MatchCompareFieldType.Integer),
                    new(searcher.FieldMetadataBuilder("Content2", Content2), true, MatchCompareFieldType.Integer)
                };
                
                var match = searcher.OrderBy(match1, orderMetadata);

                List<string> sortedByCorax = new();
                Span<long> ids = _buffer;
                int read = 0;
                do
                {
                    read = match.Fill(ids);
                    for (int i = 0; i < read; ++i)
                    {
                        long id = ids[i];
                        sortedByCorax.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                    }
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
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                var match1 = searcher.AllEntries();
                var orderMetadata = new OrderMetadata[2]
                {
                    new(searcher.FieldMetadataBuilder("Content1", Content1), true, MatchCompareFieldType.Integer),
                    new(searcher.FieldMetadataBuilder("Content2", Content2), false, MatchCompareFieldType.Integer)
                };

                var match = searcher.OrderBy(match1, orderMetadata);

                List<string> sortedByCorax = new();
                Span<long> ids = _buffer;
                int read = 0;
                do
                {
                    read = match.Fill(ids);
                    for (int i = 0; i < read; ++i)
                    {
                        long id = ids[i];
                        sortedByCorax.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                    }
                }
                while (read != 0);

                for (int i = 0; i < longList.Count; ++i)
                {                    
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);
                }
                    

                Assert.Equal(100_000 * 2, sortedByCorax.Count);
            }
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

        private static int CompareDescendingThenAscending(IndexSingleNumericalEntry<long, long> value1, IndexSingleNumericalEntry<long, long> value2)
        {
            var result = value2.Content1.CompareTo(value1.Content1);
            if (result == 0)
                return value1.Content2.CompareTo(value2.Content2);
            return result;
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
            using var knownFields = CreateKnownFields(bsc);

            {
                using var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All);

                foreach (var entry in longList)
                {
                    CreateEntry(indexWriter, entry);
                }
                indexWriter.Commit();
            }
        }

        private static void CreateEntry(IndexWriter indexWriter, IndexSingleNumericalEntry<long, long> entry)
        {
            using var builder = indexWriter.Index(entry.Id);
            builder.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
            builder.Write(Content1, Encoding.UTF8.GetBytes(entry.Content1.ToString()), entry.Content1, Convert.ToDouble(entry.Content1));
            builder.Write(Content2, Encoding.UTF8.GetBytes(entry.Content2.ToString()), entry.Content2, Convert.ToDouble(entry.Content2));
        }


        private IndexFieldsMapping CreateKnownFields(ByteStringContext bsc)
        {
            Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(bsc, "Content1", ByteStringType.Immutable, out Slice content1Slice);
            Slice.From(bsc, "Content2", ByteStringType.Immutable, out Slice content2Slice);

            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                        .AddBinding(IndexId, idSlice)
                        .AddBinding(Content1, content1Slice)
                        .AddBinding(Content2, content2Slice);
            return builder.Build();
        }

        private class IndexSingleNumericalEntry<T1, T2>
        {
            public string Id { get; set; }
            public T1 Content1 { get; set; }
            public T2 Content2 { get; set; }
        }
    }
}
