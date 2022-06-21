using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Corax;
using Corax.Queries;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Voron;
using Xunit.Abstractions;
using Xunit;
using Sparrow.Threading;
using Voron.Data.Containers;
using Voron.Data.Sets;

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
            IndexEntries(CreateKnownFields(_bsc));

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
        public void MultipleEntriesUnderSameId()
        {
            for (int i = 0; i < 1000; ++i) 
                PrepareData(DataType.Modulo);
            IndexEntries(CreateKnownFields(_bsc));
            var count = _longList.Count(p => p.Content == 9);

            Span<long> ids = new long[(int)Math.Ceiling(1.5 * _longList.Count)];
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Content", "9");
                Assert.Equal(count, match.Fill(ids));
            }

            var previousIds = new List<long>();
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Id", "list/9");
                var read = match.Fill(ids);
                previousIds.AddRange(ids.Slice(0, read).ToArray());
            }
            
            using (var indexWriter = new IndexWriter(Env, _analyzers))
            {
                indexWriter.TryDeleteEntry("Id", "list/9");
                indexWriter.Commit();
            }
            
            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);
                var match = indexSearcher.TermQuery("Id", "list/9");
                Assert.Equal(0, match.Fill(ids));
            }

            {
                using var indexSearcher = new IndexSearcher(Env, _analyzers);

                Span<byte> idAsBytes = Encodings.Utf8.GetBytes("list/9");
                Span<byte> nine = Encodings.Utf8.GetBytes("9");
                
                var match = indexSearcher.TermQuery("Content", "9");
                
                
                
                // Count in the TermMatch is 100% accurate, isnt it?      
                Assert.Equal(count - previousIds.Count, match.Count);
                var read = match.Fill(ids);
                Assert.NotEqual(0, read);
                
                //Lets check low level storage
                using var writeTransaction = Env.WriteTransaction();
                var llt = writeTransaction.LowLevelTransaction;
                
                var fields = indexSearcher.Transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
                var terms = fields?.CompactTreeFor("Content");
                Assert.True(terms!.TryGetValue("9", out var containerId));
                Assert.NotEqual(0, containerId & (long)TermIdMask.Set);
                var setId = containerId & ~0b11;
                var setStateSpan = Container.GetMutable(llt, setId);
                ref var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
                using var _ = Slice.From(llt.Allocator,"Content", ByteStringType.Immutable, out var fieldName);
                var set = new Set(llt, fieldName, in setState);
                setState = set.State;
                Assert.Equal(count - previousIds.Count, setState.NumberOfEntries);

                using var iterator = set.Iterate();
                previousIds.Sort();
                
                // Look for "list/9" in the set
                while (iterator.MoveNext())
                {
                    Assert.False(previousIds.BinarySearch(iterator.Current) >= 0);
                }

                Assert.Equal(read, match.Count);

                for (int i = 0; i < read; i++)
                {
                    var entry = indexSearcher.GetReaderFor(ids[i]);
                    Assert.True(entry.Read(0, out Span<byte> id));
                    if (idAsBytes.SequenceEqual(id))
                    {
                        entry.Read(ContentId, out Span<byte> content);
                       // Assert.False(nine.SequenceEqual(content));
                    }
                }
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
            const int bufferSize = 4096;
            using var _ = _bsc.Allocate(bufferSize, out ByteString buffer);

            {
                using var indexWriter = new IndexWriter(Env, knownFields);
                foreach (var entry in _longList)
                {
                    var entryWriter = new IndexEntryWriter(buffer.ToSpan(), _analyzers);
                    var data = CreateIndexEntry(ref entryWriter, entry);
                    indexWriter.Index(entry.Id, data);
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
