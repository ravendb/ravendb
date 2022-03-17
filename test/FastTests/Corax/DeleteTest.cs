using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using FastTests.Voron;
using Sparrow.Server;
using Voron;
using Xunit.Abstractions;
using Xunit;
using Sparrow.Threading;

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
            PrepareData();
            IndexEntries();

            Span<long> ids = stackalloc long[1024];
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Content", "0");
                Assert.Equal(_longList.Count, match.Fill(ids));
            }

            using (var indexWriter = new IndexWriter(Env, _analyzers))
            {
                indexWriter.TryDeleteEntry("Id", "list/0");
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
        public void CanDeleteOneElement()
        {
            PrepareData(DataType.Modulo);
            IndexEntries();
            var count = _longList.Count(p => p.Content == 9);
            
            Span<long> ids = stackalloc long[1024];
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Content", "9");
                Assert.Equal(count, match.Fill(ids));
            }

            using (var indexWriter = new IndexWriter(Env, _analyzers))
            {
                indexWriter.TryDeleteEntry("Id", "list/9");
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

        private void PrepareData(DataType type = DataType.Default)
        {
            for (int i = 0; i < 1000; ++i)
                switch (type)
                {
                    case DataType.Modulo:
                        _longList.Add(new IndexSingleNumericalEntry<long>{Id = $"list/{i}", Content = i % 33});
                        break;
                    default:
                        _longList.Add(new IndexSingleNumericalEntry<long> {Id = $"list/{i}", Content = 0});
                        break;
                }
        }

        private enum DataType
        {
            Default,
            Modulo
        }

        private void IndexEntries()
        {
            const int bufferSize = 4096;
            using var _ = _bsc.Allocate(bufferSize, out ByteString buffer);

            {
                using var indexWriter = new IndexWriter(Env);
                foreach (var entry in _longList)
                {
                    var entryWriter = new IndexEntryWriter(buffer.ToSpan(), _analyzers);
                    var data = CreateIndexEntry(ref entryWriter, entry);
                    indexWriter.Index(entry.Id, data, _analyzers);
                }

                indexWriter.Commit();
            }
        }

        private Span<byte> CreateIndexEntry(ref IndexEntryWriter entryWriter, IndexSingleNumericalEntry<long> entry)
        {
            entryWriter.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
            entryWriter.Write(ContentId, Encoding.UTF8.GetBytes(entry.Content.ToString()));
            entryWriter.Finish(out var output);
            return output;
        }

        private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
        {
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            return new IndexFieldsMapping(ctx)
                .AddBinding(IndexId, idSlice)
                .AddBinding(ContentId, contentSlice);
        }

        private class IndexSingleNumericalEntry<T>
        {
            public string Id { get; set; }
            public T Content { get; set; }
        }

        public override void Dispose()
        {
            _bsc.Dispose();
            base.Dispose();
        }
    }
}
