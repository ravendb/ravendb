using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Sparrow.Server.Utils.VxSort;
using Voron;
using Voron.Data.Lookups;
using Voron.Util;

namespace Corax.Indexing;

public partial class IndexWriter
{
    /// <summary>
    /// The purpose of this class is to encapsulate entries-to-terms structures and processing. This is used to create mapping from Field -> EntryId -> TermId (used by sorting primitives)
    /// </summary>
    private class EntriesToTermsTracker : IDisposable
    {
        private readonly IndexWriter _writer;
        private ContextBoundNativeList<long> _entriesForTermsRemovalsBuffer;
        private NativeList<long> _entriesForTermsAdditionsBufferEntryId;
        private NativeList<long> _entriesForTermsAdditionsBufferTermId;
        private readonly List<long> _additionsForTerm, _removalsForTerm;
        private readonly HashSet<long> _entriesAlreadyAdded;
        
        public EntriesToTermsTracker(IndexWriter writer)
        {
            _writer = writer;
            _entriesForTermsRemovalsBuffer = new(writer._entriesAllocator);
            _entriesForTermsAdditionsBufferEntryId.Initialize(_writer._entriesAllocator);
            _entriesForTermsAdditionsBufferTermId.Initialize(_writer._entriesAllocator);
            _additionsForTerm = new();
            _removalsForTerm = new();
            _entriesAlreadyAdded = new();
        }

        /// <summary>
        /// Gathers all entry IDs to be processed with the term.
        /// </summary>
        public void InsertEntries(in EntriesModifications entries)
        {
            SetRange(_additionsForTerm, entries.Additions);
            SetRange(_removalsForTerm, entries.Removals);
         
            void SetRange(List<long> list, in NativeList<TermInEntryModification> span)
            {
                list.Clear();
                list.EnsureCapacity(span.Count);
                
                for (int i = 0; i < span.Count; i++)
                    list.Add(span[i].EntryId);
            }
        }
        
        /// <summary>
        /// Clear all data structures used for preparing data for _entriesToTerms tree. 
        /// </summary>
        public void ClearEntriesForTerm()
        {
            _entriesAlreadyAdded.Clear();
            _entriesForTermsRemovalsBuffer.Clear();
            _entriesForTermsAdditionsBufferEntryId.Clear();
            _entriesForTermsAdditionsBufferTermId.Clear();
        }

        /// <summary>
        /// Prepares unique dataset for entriesToTerms tree.
        /// </summary>
        /// <param name="term">The term associated with entry IDs currently stored in _additionsForTerm and _removalsForTerm</param>
        public void ProcessCurrentEntriesForTerm(long term)
        {
            _entriesForTermsRemovalsBuffer.EnsureCapacityFor(_removalsForTerm.Count + _entriesForTermsRemovalsBuffer.Count);
            foreach (long removal in CollectionsMarshal.AsSpan(_removalsForTerm))
            {
                // if already added, we don't need to remove it in this batch
                if (_entriesAlreadyAdded.Contains(removal))
                    continue;
                _entriesForTermsRemovalsBuffer.AddUnsafe(removal);
            }

            if (_entriesForTermsAdditionsBufferTermId.HasCapacityFor(_additionsForTerm.Count) == false)
                _entriesForTermsAdditionsBufferTermId.Grow(_writer._entriesAllocator, _additionsForTerm.Count);
            
            if (_entriesForTermsAdditionsBufferEntryId.HasCapacityFor(_additionsForTerm.Count) == false)
                _entriesForTermsAdditionsBufferEntryId.Grow(_writer._entriesAllocator, _additionsForTerm.Count);
            
            foreach (long addition in CollectionsMarshal.AsSpan(_additionsForTerm))
            {
                if (_entriesAlreadyAdded.Add(addition) == false)
                    continue;
                
                _entriesForTermsAdditionsBufferEntryId.AddUnsafe(addition);
                _entriesForTermsAdditionsBufferTermId.AddUnsafe(term);
            }
        }
        
        /// <summary>
        /// Perform insertion of data required to create mapping Field -> EntryId -> TermId;
        /// </summary>
        /// <param name="fieldName">Field name</param>
        public void CommitCurrentDataFor(Slice fieldName)
        {
            var entriesToTermsTree = _writer._entriesToTermsTree.LookupFor<Int64LookupKey>(fieldName);
            if (_entriesForTermsRemovalsBuffer.Count > 0)
            {
                Sort.Run(_entriesForTermsRemovalsBuffer.ToSpan());

                entriesToTermsTree.InitializeCursorState();

                foreach (var entryId in _entriesForTermsRemovalsBuffer)
                {
                    Int64LookupKey key = entryId;
                    if (entriesToTermsTree.TryGetNextValue(ref key, out _))
                        entriesToTermsTree.TryRemoveExistingValue(ref key, out _);
                }
            }

            if (_entriesForTermsAdditionsBufferEntryId.Count > 0)
            {
                var entriesIds = _entriesForTermsAdditionsBufferEntryId.ToSpan();
                var entriesTerms = _entriesForTermsAdditionsBufferTermId.ToSpan();
                entriesIds.Sort(entriesTerms);
                entriesToTermsTree.InitializeCursorState();
                for (int idX = 0; idX < _entriesForTermsAdditionsBufferEntryId.Count; ++idX)
                {
                    Int64LookupKey key = entriesIds[idX];
                    entriesToTermsTree.TryGetNextValue(ref key, out _);
                    entriesToTermsTree.AddOrSetAfterGetNext(ref key, entriesTerms[idX]);
                }
            }
        }
        

        public void Dispose()
        {
            _entriesForTermsRemovalsBuffer.Dispose();
            _entriesForTermsAdditionsBufferTermId.Dispose(_writer._entriesAllocator);
            _entriesForTermsAdditionsBufferEntryId.Dispose(_writer._entriesAllocator);
            _entriesForTermsAdditionsBufferEntryId = default;
            _entriesForTermsAdditionsBufferTermId = default;
            _entriesForTermsRemovalsBuffer = default;
        }

        public void Reset()
        {
            _entriesForTermsRemovalsBuffer = new(_writer._entriesAllocator);
            _entriesForTermsAdditionsBufferEntryId = new();
            _entriesForTermsAdditionsBufferTermId = new();
            _removalsForTerm.Clear();
            _additionsForTerm.Clear();
            _entriesAlreadyAdded.Clear();
        }
    }
}
