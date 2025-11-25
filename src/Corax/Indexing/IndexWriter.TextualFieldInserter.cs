using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax.Utils;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Util;

namespace Corax.Indexing;

public unsafe partial class IndexWriter
{
    private ref struct TextualFieldInserter
    {
        private readonly IndexWriter _writer;
        private readonly IndexedField _indexedField;
        private readonly Span<byte> _tmpBuf;
        private readonly CompactTree _fieldTree;
        private readonly FieldBuffers<Slice, CompactTree.CompactKeyLookup> _buffers;

        private IndexTermDumper _dumper;
        private NativeList<TermInEntryModification> _entriesForTerm;
        private ContextBoundNativeList<long> _postingListPagesProcessingBuffer;
        private ContextBoundNativeList<LookupTreeOperationJob> _jobs;

        /// <summary>
        /// Terms are lazily initialized on disk, and we obtain the real address after processing EntriesModification.
        /// Creates a mapping (Index: StorageIndex, Value: physical term container). If the term doesn't exist: Constants.IndexedField.Invalid.
        /// </summary>
        private NativeList<long> _virtualTermIdToTermContainerId;

        private int _offsetAdjustment;
        private long _curPage;

        public TextualFieldInserter(IndexWriter writer, IndexedField indexedField, Span<byte> tmpBuf)
        {
            _writer = writer;
            _indexedField = indexedField;
            _tmpBuf = tmpBuf;
            _fieldTree = writer._fieldsTree.CompactTreeFor(_indexedField.Name);
            _dumper = new IndexTermDumper(writer._fieldsTree, _indexedField.Name);
            _writer._entriesToTermsTracker.ClearEntriesForTerm();
            _fieldTree.InitializeStateForTryGetNextValue();
            _entriesForTerm = new NativeList<TermInEntryModification>();
            _entriesForTerm.Initialize(_writer._entriesAllocator);
            _postingListPagesProcessingBuffer = new (_writer._entriesAllocator);
            _jobs = new(writer._entriesAllocator);
            _buffers = _writer._textualFieldBuffers ??= new FieldBuffers<Slice, CompactTree.CompactKeyLookup>(_writer);

            if (indexedField.FieldSupportsPhraseQuery)
            {
                // For most cases, _indexField.Storage.Count is equal to _indexedField.Textual.Count().
                // However, in cases where the field has mixed values (string/numerics), it differs. Therefore, we need to ensure that we have enough space to create the mapping.
                _virtualTermIdToTermContainerId = new NativeList<long>();
                _virtualTermIdToTermContainerId.InitializeWithValue(_writer._entriesAllocator, Constants.IndexedField.Invalid, _indexedField.Storage.Count);
            }
        }

        public void Dispose()
        {
            _dumper.Dispose();
            _entriesForTerm.Dispose(_writer._entriesAllocator);
            _postingListPagesProcessingBuffer.Dispose();
        }

        public void InsertTextualField(in CancellationToken token)
        {
            long totalLengthOfTerm = 0;
            _buffers.PrepareTerms(_indexedField, out var sortedTerms, out var termsOffsets);
            Debug.Assert(sortedTerms.Length > 0, "sortedTerms.Length > 0 (checked by the caller)");

            // Because of sorting first we have null, then not existing value (if any document has such), then the rest of values
            var termsToIgnore = 0;

            if (sortedTerms[termsToIgnore].AsReadOnlySpan().SequenceEqual(Constants.NullValueSlice.AsReadOnlySpan()))
            {
                HandleSpecialTerm(termsOffsets, sortedTerms, termsToIgnore, _writer._nullEntriesPostingListsTree, ref totalLengthOfTerm);
                termsToIgnore++;
            }

            if (sortedTerms.Length > termsToIgnore && sortedTerms[termsToIgnore].AsReadOnlySpan().SequenceEqual(Constants.NonExistingValueSlice.AsReadOnlySpan()))
            {
                HandleSpecialTerm(termsOffsets, sortedTerms, termsToIgnore, _writer._nonExistingEntriesPostingListsTree, ref totalLengthOfTerm);
                termsToIgnore++;
            }

            sortedTerms = sortedTerms[termsToIgnore..];
            termsOffsets = termsOffsets[termsToIgnore..];

            while (true)
            {
                token.ThrowIfCancellationRequested();

                if (sortedTerms.IsEmpty)
                    break;

                PrepareTextualFieldBatch(_buffers,
                    _indexedField,
                    _fieldTree,
                    sortedTerms,
                    termsOffsets,
                    out var keys,
                    out var postingListIds,
                    out var pageOffsets);

                var entriesOffsets = termsOffsets; // a copy that we trim internally in the loop belows
                while (keys.IsEmpty == false)
                {
                    var treeChanged = _fieldTree.CheckTreeStructureChanges();

                    _offsetAdjustment = 0;
                    int read = _fieldTree.BulkUpdateStart(keys, postingListIds, pageOffsets, out _curPage);
                    _jobs.ResetAndEnsureCapacity(read);
                    _jobs.Count = read;
                    _postingListPagesProcessingBuffer.ResetAndEnsureCapacity(read);
                    FieldInserterHelper.PrefetchPostingListsPagesAndPrepareOrderedPostingListProcessingList(_writer, ref _postingListPagesProcessingBuffer, postingListIds[..read]);

                    int idx = 0;
                    for (; idx < read; idx++)
                    {
                        //We will process the containers by page ID, keeping in mind that a page can contain multiple entries.
                        //The posting lists will be processed in page order; however, the lookup tree will be updated in term order.
                        //Singles / new items are processed at the end. 
                        var offset = (int)(_postingListPagesProcessingBuffer[idx] & FieldInserterHelper.OffsetMask);
                        
                        ref var entries = ref _indexedField.Storage.GetAsRef(entriesOffsets[offset]);
                        var job = ProcessSingleEntry(ref entries, isNullTerm: false,
                            sortedTerms[offset], postingListIds[offset],
                            keys[offset].ContainerId, entriesOffsets[offset], out var totalLengthOfTermDelta);
                        totalLengthOfTerm += totalLengthOfTermDelta;
                        entries.Dispose(_writer._entriesAllocator);
                        _jobs[offset] = job;
                    }

                    for (idx = 0; idx < read; idx++)
                    {
                        ProcessCompactTreeOperation(_jobs[idx], ref keys[idx], treeChanged, pageOffsets[idx], sortedTerms[idx]);
                    }
                    

                    keys = keys[idx..];
                    postingListIds = postingListIds[idx..];
                    pageOffsets = pageOffsets[idx..];
                    entriesOffsets = entriesOffsets[idx..];
                    sortedTerms = sortedTerms[idx..];
                    termsOffsets = termsOffsets[idx..];
                }
            }

            _writer._entriesToTermsTracker.CommitCurrentDataFor(_indexedField.Name);

            _writer._indexMetadata.Increment(_indexedField.NameTotalLengthOfTerms, totalLengthOfTerm);

            ProcessTermsVector();
        }

        private void HandleSpecialTerm(Span<int> termsOffsets, Span<Slice> sortedTerms, int termIndex, Tree tree, ref long totalLengthOfTerm)
        {
            (var postingListId, var termContainerId) = GetOrCreateSpecialPostingList(tree);
            ref var entries = ref _indexedField.Storage.GetAsRef(termsOffsets[termIndex]);
            var nullLookup = new CompactTree.CompactKeyLookup(CompactKey.NullInstance);
            var job = ProcessSingleEntry(ref entries, isNullTerm: true,
                sortedTerms[termIndex], (long)postingListId, termContainerId, termsOffsets[termIndex], out var totalLengthOfTermDelta);
            totalLengthOfTerm += totalLengthOfTermDelta;
            ProcessCompactTreeOperation(job, ref nullLookup, _fieldTree.CheckTreeStructureChanges(), pageOffset: -1, sortedTerms[termIndex]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessCompactTreeOperation(in LookupTreeOperationJob job, ref CompactTree.CompactKeyLookup key, in Lookup<CompactTree.CompactKeyLookup>.TreeStructureChanged treeChanged, int pageOffset, Slice term)
        {
            switch (job.Operation, TreeChanged: treeChanged.Changed)
            {
                case (AddEntriesToTermResult.UpdateTermId, TreeChanged: false):
                    _fieldTree.BulkUpdateSet(ref key, job.TermId, _curPage, pageOffset, ref _offsetAdjustment);
                    _dumper.WriteAddition(term, job.TermId);
                    break;
                case (AddEntriesToTermResult.UpdateTermId, TreeChanged: true):
                    _fieldTree.Add(key, job.TermId);
                    _dumper.WriteAddition(term, job.TermId);
                    break;
                case (AddEntriesToTermResult.RemoveTermId, TreeChanged: false):
                {
                    if (_fieldTree.BulkUpdateRemove(ref key, _curPage, pageOffset, ref _offsetAdjustment, out long oldValue) == false)
                    {
                        _dumper.WriteRemoval(term, job.TermId);
                        ThrowTriedToDeleteTermThatDoesNotExists(term, _indexedField.Name);
                    }
                    _dumper.WriteRemoval(term, oldValue);
                    break;
                }
                case (AddEntriesToTermResult.RemoveTermId, TreeChanged: true):
                {
                    if (_fieldTree.TryRemove(key, out long oldValue) == false)
                    {
                        _dumper.WriteRemoval(term, job.TermId);
                        ThrowTriedToDeleteTermThatDoesNotExists(term, _indexedField.Name);
                    }
                    _dumper.WriteRemoval(term, oldValue);
                    break;
                }
                case (AddEntriesToTermResult.NothingToDo, TreeChanged: _):
                    break;
                default: throw new ArgumentOutOfRangeException($"Unknown operation: {job.Operation}");
            }
        }
        
        private LookupTreeOperationJob ProcessSingleEntry(ref EntriesModifications entries, bool isNullTerm, Slice term, long postListId, ContainerEntryId termContainerId, int storageLocation, out int totalLengthOfTermDelta)
        {
            UpdateEntriesForTerm(ref _entriesForTerm, in entries);
            if (_indexedField.Spatial == null) // For spatial, we handle this in InsertSpatialField, so we skip it here
                _writer._entriesToTermsTracker.InsertEntries(entries, termContainerId);

            bool found = postListId != Constants.IndexSearcher.InvalidId;
            Debug.Assert(found || entries.Removals.Count == 0, "Cannot remove entries from term that isn't already there");
            LookupTreeOperationJob lookupTreeOperationJob;
            totalLengthOfTermDelta = 0;
            if (entries.HasChanges)
            {
                long termId;
                if (entries.Additions.Count > 0 && found == false)
                {
                    if (entries.Removals.Count != 0)
                        throw new InvalidOperationException($"Attempt to remove entries from new term: '{term}' for field {_indexedField.Name}! This is a bug.");

                    _writer.CreatePostingListForNewTerm(ref entries, _tmpBuf, out termId);
                    totalLengthOfTermDelta = entries.TermSize;
                    lookupTreeOperationJob = new LookupTreeOperationJob(AddEntriesToTermResult.UpdateTermId, termId);
                }
                else
                {
                    var entriesToTermResult = _writer.AddEntriesToTerm(_tmpBuf, postListId, isNullTerm, ref entries, out termId);
                    switch (entriesToTermResult)
                    {
                        case AddEntriesToTermResult.UpdateTermId:
                            if (termId != postListId)
                            {
                                _dumper.WriteRemoval(term, postListId);
                            }

                            Debug.Assert(isNullTerm == false, "isNullTerm == false - we pre-generate the ids, after all");

                            lookupTreeOperationJob = new LookupTreeOperationJob(AddEntriesToTermResult.UpdateTermId, termId);
                            break;
                        case AddEntriesToTermResult.RemoveTermId:
                            Debug.Assert(isNullTerm == false, "isNullTerm == false, checked inside AddEntriesToTerm");
                            totalLengthOfTermDelta = -entries.TermSize;
                            _writer._numberOfTermModifications--;
                            lookupTreeOperationJob = new LookupTreeOperationJob(AddEntriesToTermResult.RemoveTermId, termId);
                            break;
                        case AddEntriesToTermResult.NothingToDo:
                            lookupTreeOperationJob = new LookupTreeOperationJob(AddEntriesToTermResult.NothingToDo, -1L);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(entriesToTermResult.ToString());
                    }
                }
            }
            else
            {
                lookupTreeOperationJob = new LookupTreeOperationJob(AddEntriesToTermResult.NothingToDo, -1L);
            }

            RecordTermsForEntries(entries, termContainerId);

            //Update mapping virtual<=> storage location location. Final writing will be done after inserting ALL terms for specific field.
            if (_indexedField.FieldSupportsPhraseQuery)
            {
                Debug.Assert(_virtualTermIdToTermContainerId[storageLocation] == Constants.IndexedField.Invalid,
                    "virtualMapping[entries.StorageLocation] == Constants.IndexedField.Invalid, Term was already set! Persisted: {_virtualTermIdToTermContainerId[storageLocation]}, new: {termContainerId}");
                _virtualTermIdToTermContainerId[storageLocation] = (long)termContainerId;
            }

            return lookupTreeOperationJob;
        }

        private void ProcessTermsVector()
        {
            if (_indexedField.FieldSupportsPhraseQuery == false)
                return;

            const StoredFieldType storedFieldType = (StoredFieldType.List | StoredFieldType.Term);
            _writer.InitializeFieldRootPageForTermsVector(_indexedField);
            var termsPerEntrySpan = _writer._termsPerEntryId.ToSpan();

            IDisposable memoryHandler = null;
            var processingBufferPosition = 0;
            var virtualMapping = _virtualTermIdToTermContainerId.ToSpan();

            Span<long> termsBuffer = stackalloc long[32];
            Span<int> indexesBuffer = stackalloc int[32];
            Span<byte> processingBuffer = stackalloc byte[32 * ZigZagEncoding.MaxEncodedSize];

            for (var documentIndex = 0; documentIndex < _indexedField.EntryToTerms.Count; ++documentIndex)
            {
                ref var fieldTerms = ref _indexedField.EntryToTerms[documentIndex];
                ref var entryTerms = ref termsPerEntrySpan[documentIndex];

                //When document has no terms we proceed
                if (fieldTerms.Count == 0)
                    continue;

                if (fieldTerms.Count > termsBuffer.Length)
                    UnlikelyGrowBuffer(_writer._entriesAllocator, fieldTerms.Count, ref termsBuffer, ref indexesBuffer, ref processingBuffer);

                var terms = termsBuffer.Slice(0, fieldTerms.Count);
                var indexes = indexesBuffer.Slice(0, fieldTerms.Count);

                for (var termIndex = 0; termIndex < fieldTerms.Count; ++termIndex)
                {
                    ref var virtualTermId = ref fieldTerms[termIndex];
                    Debug.Assert(virtualMapping.Length > virtualTermId, "_indexedField.NativeVirtualTermIdToTermContainerId.Count > term");

                    terms[termIndex] = virtualMapping[virtualTermId];
                    indexes[termIndex] = termIndex << 1; // Gives bit for duplicate marker.
                }

// In the EntryTermsWriter, we are storing terms sorted. Since we also store frequency inside TermID, it has an impact on the order because we're moving
// each container ID by `Constants.IndexWriter.TermFrequencyShift` to store encoded frequency. We want to reconstruct exactly the same process that happens inside indexing
// to have terms in the exact same order as they will be on the disk. To do so, we have to sort terms by IDs first.
// Secondly, we have to shift all repetitions by `Constants.IndexWriter.TermFrequencyShift` and sort them again. This will give us the order from the disk.
                terms.Sort(indexes);
                var lastTermIndex = 0;
                var lastTerm = terms[lastTermIndex];
                var count = 1;
                for (int currentTermIdx = 1; currentTermIdx < fieldTerms.Count; ++currentTermIdx)
                {
                    if (lastTerm != terms[currentTermIdx])
                    {
                        for (; lastTermIndex < currentTermIdx && count > 1; ++lastTermIndex)
                            terms[lastTermIndex] <<= Constants.IndexWriter.TermFrequencyShift;

                        lastTerm = terms[currentTermIdx];
                        lastTermIndex = currentTermIdx;
                        count = 1;
                    }
                    else
                    {
                        count++;
                    }
                }

                //last duplicate batch e.g. [...., N, N, N, N]
                for (; count > 1 && lastTermIndex < fieldTerms.Count; ++lastTermIndex)
                    terms[lastTermIndex] <<= Constants.IndexWriter.TermFrequencyShift;


// Terms stored in the EntryTerms struct are sorted and unique. This means that in the case of duplicates, our offsets list may have a different size than the term array.
// Since we know that adjacent offsets may be duplicates (although not adjacent elements cannot be duplicates of each other),
// let's use the lowest bit to mark the duplication of a term from the previous elements.
// Example:
// indexes [0, 2 | 1, 4 | 1, 6 | 1, 10]
// terms   [23, 50]
// the lowest bit indicates whether to move to the next term on the list or to reuse the current one.
                terms.Sort(indexes);
                for (int currentTermIdx = terms.Length - 1; currentTermIdx >= 1; --currentTermIdx)
                {
                    // We've sorted terms, so when we're moving from right to left and find the first one without |TermFrequencyShift| bits set, that means all repetitions have been processed, and we can finish.
                    if ((terms[currentTermIdx] & Constants.IndexWriter.FrequencyTermFreeSpace) != 0)
                        break;

                    if (terms[currentTermIdx - 1] == terms[currentTermIdx])
                        indexes[currentTermIdx] |= 0b1;
                }


                for (var termIndex = 0; termIndex < fieldTerms.Count; ++termIndex)
                {
                    processingBufferPosition += ZigZagEncoding.Encode(processingBuffer, indexes[termIndex], processingBufferPosition);
                }

                var listContainerId = Container.Allocate(
                    _writer._transaction.LowLevelTransaction,
                    _writer._storedFieldsContainerId,
                    size: processingBufferPosition, //compression
                    pageLevelMetadata: _indexedField.TermsVectorFieldRootPage, // identifies list
                    out var listSpace);

                processingBuffer.Slice(0, processingBufferPosition).CopyTo(listSpace);
                var recordedTerm = RecordedTerm.CreateForStored(fieldTerms, storedFieldType, listContainerId);

                fieldTerms.Dispose(_writer._entriesAllocator);
                if (entryTerms.TryAdd(recordedTerm) == false)
                {
                    entryTerms.Grow(_writer._entriesAllocator, 1);
                    entryTerms.AddUnsafe(recordedTerm);
                }

                processingBufferPosition = 0;
            }

            _virtualTermIdToTermContainerId.Dispose(_writer._entriesAllocator);
            _virtualTermIdToTermContainerId = default;
            memoryHandler?.Dispose();

            void UnlikelyGrowBuffer(ByteStringContext allocator, int count, ref Span<long> termsBuffer, ref Span<int> indexesBuffer, ref Span<byte> processingBuffer)
            {
                var length = Bits.NextAllocationSize(count + 1);
                memoryHandler?.Dispose();
                memoryHandler = allocator.Allocate(length * (sizeof(int) + sizeof(long) + ZigZagEncoding.MaxEncodedSize), out var memory);
                termsBuffer = MemoryMarshal.Cast<byte, long>(memory.ToSpan().Slice(0, length * sizeof(long)));
                indexesBuffer = MemoryMarshal.Cast<byte, int>(memory.ToSpan().Slice(length * sizeof(long), length * sizeof(int)));
                processingBuffer = memory.ToSpan().Slice(length * (sizeof(int) + sizeof(long)));
            }
        }

        private void RecordTermsForEntries(in EntriesModifications entries, ContainerEntryId termContainerId)
        {
            foreach (var entry in _entriesForTerm)
            {
                ref var recordedTermList = ref _writer.GetEntryTerms(entry.TermsPerEntryIndex);

                if (recordedTermList.HasCapacityFor(1) == false)
                    recordedTermList.Grow(_writer._entriesAllocator, 1);

                ref var recordedTerm = ref recordedTermList.AddByRefUnsafe();

                Debug.Assert(((long)termContainerId & 0b111) == 0); // ensure that the three bottom bits are cleared

                long recordedTermContainerId = entry.Frequency switch
                {
                    > 1 => (long)termContainerId << Constants.IndexWriter.TermFrequencyShift | // note, bottom 3 are cleared, so we have 11 bits to play with
                           EntryIdEncodings.FrequencyQuantization(entry.Frequency) << 3 |
                           0b100, // marker indicating that we have a term frequency here
                    _ => (long)termContainerId
                };

                Debug.Assert(entry.InserterMode is not InserterMode.Ignore);
                if (entries.Long != null && entry.InserterMode is InserterMode.Numerical)
                {
                    recordedTermContainerId |= 1; // marker!
                    recordedTerm.Long = entries.Long.Value;

                    // only if the double value can not be computed by casting from long, we store it
                    // Since we store double values internally as longs, converted via BitConverter, it is good to check whether equal elements have exactly the same value in this form.
                    if (entries.Double != null && BitConverter.DoubleToInt64Bits(entries.Double.Value) != BitConverter.DoubleToInt64Bits(recordedTerm.Long))
                    {
                        recordedTermContainerId |= 2; // marker!
                        recordedTerm.Double = entries.Double.Value;
                    }
                }

                recordedTerm.TermContainerId = recordedTermContainerId;
            }
        }


        private void UpdateEntriesForTerm(ref NativeList<TermInEntryModification> entriesForTerm, in EntriesModifications entries)
        {
            entriesForTerm.ResetAndEnsureCapacity(_writer._entriesAllocator, entries.Additions.Count + entries.Updates.Count);
            entriesForTerm.AddRangeUnsafe(entries.Additions.ToSpan());
            entriesForTerm.AddRangeUnsafe(entries.Updates.ToSpan());
        }

        private void PrepareTextualFieldBatch(FieldBuffers<Slice, CompactTree.CompactKeyLookup> buffers,
            IndexedField indexedField,
            CompactTree fieldTree,
            Span<Slice> sortedTerms,
            Span<int> termsIndexes,
            out Span<CompactTree.CompactKeyLookup> keys,
            out Span<long> postListIds,
            out Span<int> pageOffsets)
        {
            var max = Math.Min(FieldBuffers<Slice, CompactTree.CompactKeyLookup>.BatchSize, sortedTerms.Length);
            var llt = _writer._transaction.LowLevelTransaction;
            for (int i = 0; i < max; i++)
            {
                var term = sortedTerms[i];

                var key = buffers.Keys[i].Key ??= llt.AcquireCompactKey();
                buffers.Keys[i].ContainerId = ContainerEntryId.Invalid;
                key.Set(term.AsSpan());
                key.ChangeDictionary(fieldTree.DictionaryId);
                key.EncodedWithCurrent(out _);

                ref var entries = ref indexedField.Storage.GetAsRef(termsIndexes[i]);
                entries.Prepare(_writer._entriesAllocator);
            }

            keys = new Span<CompactTree.CompactKeyLookup>(buffers.Keys, 0, max);
            postListIds = new Span<long>(buffers.PostListIds, 0, max);
            pageOffsets = new Span<int>(buffers.PageOffsets, 0, max);
        }

        private (ContainerEntryId NonExistingTermListId, ContainerEntryId NonExistingTermId) GetOrCreateSpecialPostingList(Tree tree)
        {
            // In the case where the field does not have any null values, we will create a *large* posting list (an empty one)
            // then we'll insert data to it as if it was any other term
            var entry = tree.Read(_indexedField.Name);

            if (entry != null)
            {
                Debug.Assert(sizeof(long) * 2 == sizeof((long, long)));
                Debug.Assert(entry.Reader.Length == sizeof((long, long)));
                var tuple = *((long, long)*)entry.Reader.Base;
                return (new ContainerEntryId(tuple.Item1), new ContainerEntryId(tuple.Item2));
            }

            var setId = Container.Allocate(_writer._transaction.LowLevelTransaction, _writer._postingListContainerId, sizeof(PostingListState), out var setSpace);

            _writer.InitializeFieldRootPage(_indexedField);

            var nullMarkerId = Container.Allocate(_writer._transaction.LowLevelTransaction, _writer._entriesTermsContainerId,
                1, _indexedField.FieldRootPage, out var nullBuffer);
            nullBuffer.Clear();

            // we need to account for the size of the posting lists, once a term has been switch to a posting list
            // it will always be in this model, so we don't need to do any cleanup
            _writer._largePostingListSet ??= _writer._transaction.OpenPostingList(Constants.IndexWriter.LargePostingListsSetSlice);
            _writer._largePostingListSet.Add((long)setId);

            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);
            PostingList.Create(_writer._transaction.LowLevelTransaction, ref postingListState);
            var encodedPostingListId = new ContainerEntryId(EntryIdEncodings.Encode((long)setId, 0, TermIdMask.PostingList));

            using (tree.DirectAdd(_indexedField.Name, sizeof((long, long)), out var p))
            {
                *((long, long)*)p = ((long)encodedPostingListId, (long)nullMarkerId);
            }

            return (encodedPostingListId, nullMarkerId);
        }

        
    }

    private static class FieldInserterHelper
    {
        public const int OffsetMask = 0x3FF;
        
        public static void PrefetchPostingListsPagesAndPrepareOrderedPostingListProcessingList(IndexWriter writer, ref ContextBoundNativeList<long> postingListPagesProcessingBuffer, Span<long> postListIds)
        {
            postingListPagesProcessingBuffer.Clear();
            postingListPagesProcessingBuffer.EnsureCapacityFor(postListIds.Length);

            foreach (var cur in postListIds)
            {
                if (cur == Constants.IndexSearcher.InvalidId)
                    continue;
                if ((cur & (long)TermIdMask.EnsureIsSingleMask) == 0)
                    continue;

                // Decoding the posting list id yields the container root that stores the posting list. We keep it as
                // ContainerId so later steps talk to the correct root instead of a payload slot.
                long containerId = (long)EntryIdEncodings.GetContainerId(cur);
                postingListPagesProcessingBuffer.Add(containerId / Voron.Global.Constants.Storage.PageSize);
            }

            postingListPagesProcessingBuffer.Count = Sorting.SortAndRemoveDuplicates(postingListPagesProcessingBuffer.RawItems, postingListPagesProcessingBuffer.Count);

            writer._transaction.LowLevelTransaction.DataPager.MaybePrefetchMemory(postingListPagesProcessingBuffer.GetEnumerator());
            postingListPagesProcessingBuffer.Clear();

            Debug.Assert(postListIds.Length <= 1024);

            //ContainerId has 10 lowest bits in use to encode the metadata. We can use them to store the offset of the item in the original order.
            for (int i = 0; i < postListIds.Length; i++)
            {
                if (postListIds[i] == Constants.IndexSearcher.InvalidId || (postListIds[i] & (long)TermIdMask.EnsureIsSingleMask) == 0)
                {
                    // new and single term will be allocating new containers or will be deleted. So let's put them at the end of the list.
                    postingListPagesProcessingBuffer.AddByRefUnsafe() = (long.MaxValue & ~OffsetMask) | (long)i;
                    continue;
                }
                
                postingListPagesProcessingBuffer.AddByRefUnsafe() = (postListIds[i] & ~OffsetMask) | (long)i;
            }
            
            postingListPagesProcessingBuffer.Sort();
        }
    }
}
