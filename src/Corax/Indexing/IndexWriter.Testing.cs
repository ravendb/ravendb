using System;
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
            const int bufferLength = 1024;
            
            numberOfEntriesLocation = writer._entryIdToLocation.NumberOfEntries;
            numberOfEntriesCompactTreeId = 0;
            
            var idCompactTree = writer._fieldsTree.CompactTreeFor(
                writer._fieldsMapping.GetByFieldId(Constants.IndexWriter.PrimaryKeyFieldId).FieldName);


            var keys = new long[bufferLength];
            var keysPtr = new long[bufferLength];
            using var _ = writer._transaction.Allocator.Allocate(bufferLength * sizeof(UnmanagedSpan), out ByteString containersPtrBs);
            var containers = new Span<UnmanagedSpan>(containersPtrBs.Ptr, bufferLength);
            
            var iterator = idCompactTree.Iterate();
            iterator.Reset();
            while (iterator.Fill(keys) is var read and > 0)
            {
                for (int i = 0; i < read; ++i)
                {
                    keysPtr[i] = keys[i];
                    if ((keys[i] & (long)TermIdMask.EnsureIsSingleMask) != 0)
                    {
                        keysPtr[i] = (long)EntryIdEncodings.GetContainerId(keys[i]);
                        continue;
                    }

                    keysPtr[i] = -1;
                }


                Container.GetAll(writer._transaction.LowLevelTransaction, keysPtr[..read], containers, -1, writer._transaction.LowLevelTransaction.PageLocator);
                for (int i = 0; i < read; i++)
                {
                    var currentKey = keys[i];
                    switch (currentKey & (long)TermIdMask.EnsureIsSingleMask)
                    {
                        case (long)TermIdMask.SmallPostingList:
                            numberOfEntriesCompactTreeId += VariableSizeEncoding.Read<long>(containers[i].Address, out var __);
                            break;
                        case (long)TermIdMask.PostingList:
                            numberOfEntriesCompactTreeId += ((PostingListState*)containers[i].Address)->NumberOfEntries;
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
