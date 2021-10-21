using System;
using System.Collections.Generic;
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
        private readonly Dictionary<Slice, int> _knownFields;
        private readonly ByteStringContext _bsc;

        public DeleteTest(ITestOutputHelper output) : base(output)
        {
            _bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            _knownFields = CreateKnownFields(_bsc);
        }

        [Fact]
        public void CanDelete()
        {
            PrepareData();
            IndexEntries();

            Span<long> ids = stackalloc long[1024];
            {
                using var indexSearcher = new IndexSearcher(Env);
                var match = indexSearcher.TermQuery("Content", "0");
                Assert.Equal(_longList.Count, match.Fill(ids));
            }

            using (var indexWriter = new IndexWriter(Env))
            {
                indexWriter.TryDeleteEntry("Id", "list/0");
                indexWriter.Commit(_knownFields);
            }

            {
                using var indexSearcher = new IndexSearcher(Env);
                var match = indexSearcher.TermQuery("Content", "0");
                Assert.Equal(_longList.Count - 1, match.Fill(ids));
            }

            {
                using var indexSearcher = new IndexSearcher(Env);
                var match = indexSearcher.TermQuery("Id", "list/0");
                Assert.Equal(0, match.Fill(ids));
            }

            {
                using var indexSearcher = new IndexSearcher(Env);
                var match1 = indexSearcher.AllEntries();
                Assert.Equal(_longList.Count - 1, match1.Fill(ids));
            }
        }

        private void PrepareData()
        {
            for (int i = 0; i < 1000; ++i)
            {
                _longList.Add(new IndexSingleNumericalEntry<long> { Id = $"list/{i}", Content = 0 });
            }
        }


        private void IndexEntries()
        {


            const int bufferSize = 4096;
            using var _ = _bsc.Allocate(bufferSize, out ByteString buffer);

            {
                using var indexWriter = new IndexWriter(Env);
                foreach (var entry in _longList)
                {
                    var entryWriter = new IndexEntryWriter(buffer.ToSpan(), _knownFields);
                    var data = CreateIndexEntry(ref entryWriter, entry);
                    indexWriter.Index(entry.Id, data, _knownFields);
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

        private Dictionary<Slice, int> CreateKnownFields(ByteStringContext bsc)
        {
            Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(bsc, "Content", ByteStringType.Immutable, out Slice contentSlice);

            return new Dictionary<Slice, int> { [idSlice] = IndexId, [contentSlice] = ContentId, };
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
