using System.Collections.Generic;
using System.Runtime.InteropServices;
using Corax;
using Corax.Indexing;
using Corax.Utils;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Util;
using Voron.Util.PFor;
using Xunit;
namespace SlowTests.Voron.Issues;

public unsafe class RavenDB_25839(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Corax)]
    public void CanProperlyUpdateNumberOfEntriesWhenMergingLeaves()
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


            var currentMax = currentId + 2 * 80_000;
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

        var toRemoveLimitFirst = EntryIdEncodings.DecodeAndDiscardFrequency(62391300);
        var toRemoveSecond = EntryIdEncodings.DecodeAndDiscardFrequency(124781572);

        using (var wTx = Env.WriteTransaction())
        {
            using var pForEncoder = new FastPForEncoder(wTx.Allocator);
            using var pForDecoderDisposal = new FastPForDecoder(wTx.Allocator);
            Container.GetMutable(wTx.LowLevelTransaction, new ContainerEntryId(setId), out var setSpace);
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace.ToSpan());

            var pForDecoder = pForDecoderDisposal;
            var tmpBuffer = new ContextBoundNativeList<long>(wTx.Allocator, 1024);
            var removalList = new ContextBoundNativeList<long>(wTx.Allocator, 1024);
            for (var i = 1; i < (long)toRemoveLimitFirst / 2; i++)
            {
                var id = EntryIdEncodings.Encode(i, 1, TermIdMask.Single);
                currentElements.Remove(id);
                removalList.Add(id | 1); //removal mask
            }

            var skipIt = 0;
            for (var i = (long)toRemoveLimitFirst; i < (long)toRemoveSecond; i++)
            {
                if (skipIt++ < 100) continue;

                var id = EntryIdEncodings.Encode(i, 1, TermIdMask.Single);
                currentElements.Remove(id);
                removalList.Add(id | 1); //removal mask
            }

            removalList.Sort();

            PostingList.Update(wTx.LowLevelTransaction, ref postingListState, null, 0, removalList.RawItems, removalList.Count, pForEncoder, ref tmpBuffer, ref pForDecoder);

            wTx.Commit();
        }


        using (var wTx = Env.WriteTransaction())
        {
            using var pForEncoder = new FastPForEncoder(wTx.Allocator);
            using var pForDecoderDisposal = new FastPForDecoder(wTx.Allocator);
            var pForDecoder = pForDecoderDisposal;
            Container.GetMutable(wTx.LowLevelTransaction, new ContainerEntryId(setId), out var setSpace);
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace.ToSpan());
            var postingList = new PostingList(wTx.LowLevelTransaction, Slices.Empty, postingListState);
            postingList.Render();
            var iterate = postingList.Iterate();
            iterate.Seek(0);
            var buff = new long[1024];
            var dataFromIterator = new List<long>();
            while (iterate.Fill(buff, out var read))
            {
                dataFromIterator.AddRange(buff[0..read]);
            }

            Assert.Equal(currentElements, dataFromIterator);

            var tmpBuffer = new ContextBoundNativeList<long>(wTx.Allocator, 1024);
            var removalList = new ContextBoundNativeList<long>(wTx.Allocator, 1024);
            removalList.Add(currentElements[0] | 1);
            currentElements.RemoveAt(0);
            PostingList.Update(wTx.LowLevelTransaction, ref postingListState, null, 0, removalList.RawItems, removalList.Count, pForEncoder, ref tmpBuffer, ref pForDecoder);
            wTx.Commit();
        }
    }
}
