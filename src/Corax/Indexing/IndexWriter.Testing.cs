using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.Containers;
using Voron.Data.PostingLists;

namespace Corax.Indexing;

public partial class IndexWriter
{
    internal TestingStuff ForTestingPurposes() => new(this);
    
    internal class TestingStuff(IndexWriter writer)
    {
        public unsafe bool ValidateIdTreeToEntries(out long numberOfEntriesLocation, out long numberOfEntriesCompactTreeId)
        {
            numberOfEntriesLocation = writer._entryIdToLocation.NumberOfEntries;
            numberOfEntriesCompactTreeId = 0;
            
            var idCompactTree = writer._fieldsTree.CompactTreeFor(
                writer._fieldsMapping.GetByFieldId(Constants.IndexWriter.PrimaryKeyFieldId).FieldName);


            var keys = new long[1024];
            var keysPtr = new long[1024];
            using var _ = writer._transaction.Allocator.Allocate(1024 * sizeof(UnmanagedSpan), out ByteString containersPtrBs);
            var containersPtr = (UnmanagedSpan*)(containersPtrBs.Ptr);
            
            var iterator = idCompactTree.Iterate();
            iterator.Reset();
            while (iterator.Fill(keys) is var read and > 0)
            {
                for (int i = 0; i < read; ++i)
                {
                    keysPtr[i] = keys[i];
                    if ((keys[i] & (long)TermIdMask.EnsureIsSingleMask) != 0)
                    {
                        keysPtr[i] = EntryIdEncodings.GetContainerId(keys[i]);
                        continue;
                    }

                    keysPtr[i] = -1;
                }


                Container.GetAll(writer._transaction.LowLevelTransaction, keysPtr[..read], containersPtr, -1, writer._transaction.LowLevelTransaction.PageLocator);
                for (int i = 0; i < read; i++)
                {
                    var currentKey = keys[i];
                    switch (currentKey & (long)TermIdMask.EnsureIsSingleMask)
                    {
                        case (long)TermIdMask.SmallPostingList:
                            numberOfEntriesCompactTreeId += VariableSizeEncoding.Read<long>(containersPtr[i].Address, out var __);
                            break;
                        case (long)TermIdMask.PostingList:
                            numberOfEntriesCompactTreeId += ((PostingListState*)containersPtr[i].Address)->NumberOfEntries;
                            break;
                        default:
                            numberOfEntriesCompactTreeId += 1;
                            break;
                    }
                }
            }

            return numberOfEntriesLocation == numberOfEntriesCompactTreeId;
        }
    }
}
