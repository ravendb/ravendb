using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Utils;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

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
            indexWriter.TryDeleteEntry("list/9");
            indexWriter.PrepareAndCommit();
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
            Assert.NotEqual(0, containerId & (long)TermIdMask.PostingList);
            var setId = EntryIdEncodings.DecodeAndDiscardFrequency(containerId);
            var setStateSpan = Container.GetMutable(llt, setId);
            ref var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
            using var _ = Slice.From(llt.Allocator, "Content", ByteStringType.Immutable, out var fieldName);
            var set = new PostingList(llt, fieldName, in setState);
            setState = set.State;
            Assert.Equal(count - previousIds.Count, setState.NumberOfEntries);

            var iterator = set.Iterate();
            previousIds.Sort();

            // Look for "list/9" in the set
            {
                Span<long> buffer = stackalloc long[256];
                while (iterator.Fill(buffer, out var total))
                {
                    for (int i = 0; i < total; i++)
                    {
                        var current = buffer[i];
                        Assert.False(previousIds.BinarySearch(EntryIdEncodings.DecodeAndDiscardFrequency(current)) >= 0);
                    }
                }
            }

            Assert.Equal(read, match.Count);

            for (int i = 0; i < read; i++)
            {
                var entry = indexSearcher.GetEntryReaderFor(ids[i]);
                Assert.True(entry.GetFieldReaderFor(0).Read(out Span<byte> id));
                if (idAsBytes.SequenceEqual(id))
                {
                    entry.GetFieldReaderFor(ContentId).Read(out Span<byte> content);
                    // Assert.False(nine.SequenceEqual(content));
                }
            }
        }
    }

    private void PrepareData(DataType type = DataType.Default, int batchSize = 1000, uint modulo = 33)
    {
        for (int i = 0; i < batchSize; ++i)
            switch (type)
            {
                case DataType.Modulo:
                    _longList.Add(new IndexSingleNumericalEntry<long> {Id = $"list/{i}", Content = i % modulo});
                    break;
                case DataType.Linear:
                    _longList.Add(new IndexSingleNumericalEntry<long> {Id = $"list/{i}", Content = i});
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
        using var indexWriter = new IndexWriter(Env, knownFields);
        var entryWriter = new IndexEntryWriter(_bsc, knownFields);

        foreach (var entry in _longList)
        {
            using var __ = CreateIndexEntry(ref entryWriter, entry, out var data);
            indexWriter.Index(entry.Id,data.ToSpan());
        }

        indexWriter.PrepareAndCommit();
        knownFields.Dispose();
    }

    private ByteStringContext<ByteStringMemoryCache>.InternalScope CreateIndexEntry(
        ref IndexEntryWriter entryWriter, IndexSingleNumericalEntry<long> entry, out ByteString output)
    {
        entryWriter.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
        entryWriter.Write(ContentId, Encoding.UTF8.GetBytes(entry.Content.ToString()), entry.Content, entry.Content);
        return entryWriter.Finish(out output);
    }

    private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

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

    public override void Dispose()
    {
        _bsc.Dispose();
        _analyzers?.Dispose();
        base.Dispose();
    }
}
