using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Querying;
using Corax.Mappings;
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
    public class DeleteTest : StorageTest
    {
        private List<IndexSingleNumericalEntry<long>> _longList = new();
        private const int IndexId = 0, ContentId = 1;
        private readonly IndexFieldsMapping _analyzers;
        private readonly ByteStringContext _bsc;

        public DeleteTest(ITestOutputHelper output) : base(output)
        {
            _bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            _analyzers = CreateKnownFields(_bsc);
        }

        [Fact]
        public void CanDelete()
        {
            PrepareData(batchSize: 1000);
            IndexEntries(CreateKnownFields(_bsc));

            Span<long> ids = stackalloc long[1024];
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Content", "0");
                Assert.Equal(_longList.Count, match.Fill(ids));
            }

            using (var indexWriter = new IndexWriter(Env, _analyzers, SupportedFeatures.All))
            {
                indexWriter.TryDeleteEntry("list/0");
                indexWriter.Commit();
            }

            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Content", "0");
                Assert.Equal(_longList.Count - 1, match.Fill(ids));
            }

            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Id", "list/0");
                Assert.Equal(0, match.Fill(ids));
            }

            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match1 = indexSearcher.AllEntries();
                Assert.Equal(_longList.Count - 1, match1.Fill(ids));
            }
        }
        
        [Fact]
        public void CanDeleteNumericalData()
        {
            PrepareData();
            IndexEntries(CreateKnownFields(_bsc));

            Span<long> ids = stackalloc long[1024];
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
                var match = indexSearcher.GreatThanOrEqualsQuery(indexSearcher.FieldMetadataBuilder("Content"), 0L);
                Assert.Equal(_longList.Count, match.Fill(ids));
            }

            using (var indexWriter = new IndexWriter(Env, _analyzers, SupportedFeatures.All))
            {
                indexWriter.TryDeleteEntry("list/0");
                indexWriter.Commit();
            }

            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
                var match = indexSearcher.GreatThanOrEqualsQuery(indexSearcher.FieldMetadataBuilder("Content"), 0L);
                Assert.Equal(_longList.Count -1, match.Fill(ids));
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(4)]
        [InlineData(10000)]
        public void CanDeleteSingleItemInList(int batchSize)
        {
            PrepareData(DataType.Modulo, batchSize, 2);
            IndexEntries(CreateKnownFields(_bsc));
            
            using var y = Slice.From(_bsc, "Content", out var contentSlice);
            using var x = Slice.From(_bsc, "Content-L", out var fieldLong);

            
            Span<long> ids = new long[batchSize + 10];
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Content", "0");
                Assert.Equal((int)Math.Ceiling(batchSize/2.0), match.Fill(ids));
                Assert.Equal(batchSize, indexSearcher.NumberOfEntries);
            }
            
            using (var indexWriter = new IndexWriter(Env, _analyzers, SupportedFeatures.All))
            {
                indexWriter.TryDeleteEntry("list/0");
                indexWriter.Commit();
            }
            
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Content", "0");
                Assert.Equal(((int)Math.Ceiling(batchSize/2.0))-1, match.Fill(ids));
                Assert.Equal(batchSize-1, indexSearcher.NumberOfEntries);
            }
        }
        
        [Fact]
        public void CanDeleteOneElement()
        {
            PrepareData(DataType.Modulo);
            IndexEntries(CreateKnownFields(_bsc));
            var count = _longList.Count(p => p.Content == 9);
            
            Span<long> ids = stackalloc long[1024];
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Content", "9");
                Assert.Equal(count, match.Fill(ids));
            }

            using (var indexWriter = new IndexWriter(Env, _analyzers, SupportedFeatures.All))
            {
                indexWriter.TryDeleteEntry("list/9");
                indexWriter.Commit();
            }

            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Content", "9");
                Assert.Equal(count - 1, match.Fill(ids));
            }

            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Id", "list/9");
                Assert.Equal(0, match.Fill(ids));
            }

            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match1 = indexSearcher.AllEntries();
                Assert.Equal(_longList.Count - 1, match1.Fill(ids));
            }
        }

        [Fact]
        public void CanDeleteAndPushUnderSameId()
        {
            PrepareData(DataType.Modulo, 1);
            IndexEntries(CreateKnownFields(_bsc));
            Span<long> ids = stackalloc long[1024];
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Content", "0");
                Assert.Equal(1, match.Fill(ids));
            }
            using (var indexWriter = new IndexWriter(Env, _analyzers, SupportedFeatures.All))
            {
                indexWriter.TryDeleteEntry("list/0");
                indexWriter.Commit();
            }

            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                Assert.Equal(0, indexSearcher.NumberOfEntries);
            }

            IndexEntries(CreateKnownFields(_bsc));
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Content", "0");
                Assert.Equal(1, match.Fill(ids));
                var termsReader = indexSearcher.TermsReaderFor("Id");
                Assert.True(termsReader.TryGetTermFor(ids[0], out var term));
                Assert.Equal("list/0", term);

            }
        }

        private void PrepareData(DataType type = DataType.Default, int batchSize = 1000, uint modulo = 33)
        {
            for (int i = 0; i < batchSize; ++i)
                switch (type)
                {
                    case DataType.Modulo:
                        _longList.Add(new IndexSingleNumericalEntry<long>{Id = $"list/{i}", Content = i % modulo});
                        break;
                    case DataType.Linear:
                        _longList.Add(new IndexSingleNumericalEntry<long>{Id = $"list/{i}", Content = i });
                        break;
                    default:
                        _longList.Add(new IndexSingleNumericalEntry<long> {Id = $"list/{i}", Content = 0});
                        break;
                }
        }

        private enum DataType
        {
            Default,
            Linear,
            Modulo
        }

        private void IndexEntries(IndexFieldsMapping knownFields)
        {
            using var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All);

            foreach (var entry in _longList)
            {
                using var builder = indexWriter.Index(Encoding.UTF8.GetBytes(entry.Id));
                
                builder.Write(IndexId, null, Encoding.UTF8.GetBytes(entry.Id));
                builder.Write(ContentId, null, Encoding.UTF8.GetBytes(entry.Content.ToString()), entry.Content, entry.Content);
            }

            indexWriter.Commit();
        }


        private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
        {
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            using (var builder = IndexFieldsMappingBuilder.CreateForWriter(false).AddBinding(IndexId, idSlice).AddBinding(ContentId, contentSlice))
                return builder.Build();
        }

        private class IndexSingleNumericalEntry<T>
        {
            public string Id { get; set; }
            public T Content { get; set; }
        }

        public override void Dispose()
        {
            _bsc.Dispose();
            _analyzers.Dispose();
            base.Dispose();
        }
    }
}
