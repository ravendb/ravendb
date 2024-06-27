using System;
using System.Collections.Generic;
using System.Text;
using Corax;
using Corax.Querying;
using Corax.Mappings;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Sparrow.Server;
using Voron;
using Xunit.Abstractions;
using Xunit;
using Sparrow.Threading;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax
{
    public class OrderBySortingTests : StorageTest
    {
        private readonly List<IndexSingleNumericalEntry<long>> longList = new();
        private const int IndexId = 0, ContentId = 1;

        public OrderBySortingTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void OrderByNumber()
        {
            PrepareData();
            IndexEntries();
            longList.Sort(CompareDescending);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                var allEntries = searcher.AllEntries();
                var match1 = searcher.StartWithQuery("Id", "l");
                var concat = searcher.And(allEntries, match1);

                var match = searcher.OrderBy(concat,
                    new OrderMetadata(searcher.FieldMetadataBuilder("Content", ContentId), false, MatchCompareFieldType.Integer));

                List<string> sortedByCorax = new();
                Span<long> ids = stackalloc long[2048];
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
                
                Assert.Equal(1000, sortedByCorax.Count);
            }
        }

        private static int CompareAscending(IndexSingleNumericalEntry<long> value1, IndexSingleNumericalEntry<long> value2)
        {
            return value1.Content.CompareTo(value2.Content);
        }

        private static int CompareDescending(IndexSingleNumericalEntry<long> value1, IndexSingleNumericalEntry<long> value2)
        {
            return value2.Content.CompareTo(value1.Content);
        }
        private void PrepareData()
        {
            for (int i = 0; i < 1000; ++i)
            {
                longList.Add(new IndexSingleNumericalEntry<long>
                {
                    Id = $"list/{i}",
                    Content = i
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
                    using var builder = indexWriter.Index(entry.Id);
                    builder.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
                    builder.Write(ContentId, Encoding.UTF8.GetBytes(entry.Content.ToString()), entry.Content, Convert.ToDouble(entry.Content));
                }
                indexWriter.Commit();
            }
        }


        private IndexFieldsMapping CreateKnownFields(ByteStringContext bsc)
        {
            Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(bsc, "Content", ByteStringType.Immutable, out Slice contentSlice);

            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IndexId, idSlice)
                .AddBinding(ContentId, contentSlice);
            return builder.Build();
        }

        private class IndexSingleNumericalEntry<T>
        {
            public string Id { get; set; }
            public T Content { get; set; }
        }
    }
}
