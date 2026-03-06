using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax;
using Corax.Indexing;
using Corax.Utils;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;
using Xunit;

namespace SlowTests.Voron.Issues;

public unsafe class RavenDB_25916(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void CanMergeLeavesSimple()
    {
        ContainerId postingListContainerId;
        long setId;
        var currentId = 1;
        List<long> currentElements = new();
        using (var wTx = Env.WriteTransaction())
        {
            using var pForEncoder = new FastPForEncoder(wTx.Allocator);
            using var pForDecoderDisposal = new FastPForDecoder(wTx.Allocator);
            postingListContainerId = wTx.OpenContainer(Constants.IndexWriter.PostingListsSlice);

            setId = (long)Container.Allocate(wTx.LowLevelTransaction, postingListContainerId, sizeof(PostingListState), out var setSpace);
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);


            var currentMax = currentId + 600_000;
            var toInsert = new ContextBoundNativeList<long>(wTx.Allocator, 1024);
            for (; currentId < currentMax; currentId++)
            {
                var id = EntryIdEncodings.Encode(currentId, 1, TermIdMask.Single);
                toInsert.Add(id);
                currentElements.Add(id);
            }

            toInsert.Sort();
            pForEncoder.Encode(toInsert.RawItems, toInsert.Count);
            PostingList.Create(wTx.LowLevelTransaction, ref postingListState, pForEncoder);

            wTx.Commit();
        }
        ValidateTree();

        Remove(1, (long)EntryIdEncodings.DecodeAndDiscardFrequency(62_391_300));
        ValidateTree();

        Remove((long)EntryIdEncodings.DecodeAndDiscardFrequency(124_781_572), (long)EntryIdEncodings.DecodeAndDiscardFrequency(187_171_844));
        ValidateTree();

        Remove((long)EntryIdEncodings.DecodeAndDiscardFrequency(62_391_300), (long)EntryIdEncodings.DecodeAndDiscardFrequency(124_781_572));
        ValidateTree();

        using (var rTx = Env.ReadTransaction())
        {
            using var pForEncoder = new FastPForEncoder(rTx.Allocator);
            using var pForDecoderDisposal = new FastPForDecoder(rTx.Allocator);
            Container.Get(rTx.LowLevelTransaction, new ContainerEntryId(setId), out var setSpace);
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace.ToSpan());
            var ps = new PostingList(rTx.LowLevelTransaction, Slices.Empty, postingListState);

            var it = ps.Iterate();
            it.Seek();
            Span<long> buffer = new long[1024];
            while (it.Fill(buffer, out var read) && read != 0)
            {
                foreach (var id in buffer.Slice(0, read))
                    currentElements.Remove(id);
            }
            
            Assert.Empty(currentElements);
        }


        void ValidateTree()
        {
            using (var rTx = Env.ReadTransaction())
            {
                using var pForEncoder = new FastPForEncoder(rTx.Allocator);
                using var pForDecoderDisposal = new FastPForDecoder(rTx.Allocator);
                Container.Get(rTx.LowLevelTransaction, new ContainerEntryId(setId), out var setSpace);
                ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace.ToSpan());
                var ps = new PostingList(rTx.LowLevelTransaction, Slices.Empty, postingListState);
                var startPage = rTx.LowLevelTransaction.GetPage(ps.State.RootPage);
                AssertRecursively(rTx.LowLevelTransaction, startPage);


                void AssertRecursively(LowLevelTransaction llt, Page page)
                {
                    Assert.True(RuntimeHelpers.TryEnsureSufficientExecutionStack());
                    var header = new PostingListCursorState { Page = page };
                    var leaf = new PostingListLeafPage(page);
                    var branch = new PostingListBranchPage(page);

                    if (header.IsLeaf)
                    {
                        Assert.NotEqual(0, leaf.Header->NumberOfEntries);
                        Assert.NotEqual(0, leaf.Header->SizeUsed);
                    }
                    else
                    {
                        Assert.NotEqual(0, branch.Header->NumberOfEntries);
                        for (int branchElementIDx = 0; branchElementIDx < branch.Header->NumberOfEntries; branchElementIDx++)
                        {
                            (long key, long pageNum) = branch.GetByIndex(branchElementIDx);
                            AssertRecursively(llt, llt.GetPage(pageNum));
                        }
                    }
                }
            }
        }

        void Remove(long fromInclusive, long toExclusive)
        {
            using (var wTx = Env.WriteTransaction())
            {
                using var pForEncoder = new FastPForEncoder(wTx.Allocator);
                using var pForDecoderDisposal = new FastPForDecoder(wTx.Allocator);
                Container.GetMutable(wTx.LowLevelTransaction, new ContainerEntryId(setId), out var setSpace);
                ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace.ToSpan());

                var removalList = new ContextBoundNativeList<long>(wTx.Allocator, 1024);
                for (long i = fromInclusive; i < toExclusive; i++)
                {
                    var id = EntryIdEncodings.Encode(i, 1, TermIdMask.Single);
                    removalList.Add(id | 1);
                    currentElements.Remove(id);
                }

                removalList.Sort();
                var tmpBuffer = new ContextBoundNativeList<long>(wTx.Allocator, 1024);

                var pForDecoder = pForDecoderDisposal;
                PostingList.Update(wTx.LowLevelTransaction, ref postingListState, null, 0, removalList.RawItems, removalList.Count, pForEncoder, ref tmpBuffer, ref pForDecoder);
                wTx.Commit();
            }
        }
    }
}
