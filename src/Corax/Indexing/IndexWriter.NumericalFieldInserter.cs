using System;
using System.Diagnostics;
using System.Threading;
using Voron;
using Voron.Data.Lookups;
using Voron.Util;

namespace Corax.Indexing;

public partial class IndexWriter
{
    private ref struct NumericalFieldInserter<TKey, TLookupKey>
        where TKey : unmanaged
        where TLookupKey : struct, ILookupKey
    {
        private readonly IndexWriter _writer;
        private readonly IndexedField _indexedField;
        private readonly Slice _fieldName;
        private readonly FieldBuffers<TKey, TLookupKey> _buffers;
        private readonly Lookup<TLookupKey> _fieldTree;
        private readonly Span<byte> _tmpBuf;

        private IndexTermDumper _dumper;

        private ContextBoundNativeList<long> _pagesToPrefetch;

        private int _offsetAdjustment;
        private long _curPage;
        private long _numberOfTermsToProcess;

        public NumericalFieldInserter(IndexWriter writer, IndexedField indexedField, Span<byte> tmpBuf)
        {
            _writer = writer;
            _indexedField = indexedField;

            _buffers = GetBuffers();
            _fieldName = typeof(Int64LookupKey) == typeof(TLookupKey) ? _indexedField.NameLong : _indexedField.NameDouble;

            _fieldTree = _writer._fieldsTree.LookupFor<TLookupKey>(_fieldName);
            _fieldTree.InitializeCursorState();

            _dumper = new IndexTermDumper(_writer._fieldsTree, _fieldName);
            _pagesToPrefetch = new(_writer._entriesAllocator);

            _writer._entriesToTermsTracker.ClearEntriesForTerm();
            _tmpBuf = tmpBuf;
            _numberOfTermsToProcess = typeof(Int64LookupKey) == typeof(TLookupKey) ? _indexedField.Longs.Count : indexedField.Doubles.Count;
        }

        public void Dispose()
        {
            _dumper.Dispose();
            _pagesToPrefetch.Dispose();
        }

        public void InsertNumericalField(CancellationToken token)
        {
            if (_numberOfTermsToProcess == 0)
                goto Finish;
            
            _buffers.PrepareTerms(_indexedField, out var sortedTerms, out var termsOffsets);
            Debug.Assert(sortedTerms.Length > 0);

            while (true)
            {
                token.ThrowIfCancellationRequested();

                if (sortedTerms.IsEmpty)
                    break;

                PrepareNumericalFieldBatch(sortedTerms, termsOffsets, out var keys, out var postingListIds, out var pageOffsets);
                var entriesOffsets = termsOffsets; // a copy that we trim internally in the loop belows
                while (keys.IsEmpty == false)
                {
                    var treeChanged = _fieldTree.CheckTreeStructureChanges();
                    _offsetAdjustment = 0;
                    int read = _fieldTree.BulkUpdateStart(keys, postingListIds, pageOffsets, out _curPage);
                    FieldInserterHelper.PrefetchContainerPages(_writer, ref _pagesToPrefetch, postingListIds[..read]);

                    int idX = 0;
                    for (; idX < read; idX++)
                    {
                        ref var entries = ref _indexedField.Storage.GetAsRef(entriesOffsets[idX]);
                        ProcessSingleEntry(ref entries, ref keys[idX], sortedTerms[idX], postingListIds[idX], pageOffsets[idX]);
                        entries.Dispose(_writer._entriesAllocator);

                        if (treeChanged.Changed)
                        {
                            idX++; // we need to skip the currently processed
                            break;
                        }
                    }

                    keys = keys[idX..];
                    postingListIds = postingListIds[idX..];
                    pageOffsets = pageOffsets[idX..];
                    entriesOffsets = entriesOffsets[idX..];
                    sortedTerms = sortedTerms[idX..];
                    termsOffsets = termsOffsets[idX..];
                }
            }

            Finish:
            _writer._entriesToTermsTracker.CommitCurrentDataFor(_fieldName);
        }

        private void ProcessSingleEntry(ref EntriesModifications entries, ref TLookupKey key, TKey term, long postingListId, int pageOffset)
        {
            bool termFound = postingListId != Constants.IndexSearcher.InvalidId;
            Debug.Assert(termFound || entries.Removals.Count == 0, "Cannot remove entries from term that isn't already there");
            _writer._entriesToTermsTracker.InsertEntries(entries);

            if (entries.HasChanges)
            {
                long termId;
                if (entries.Additions.Count > 0 && termFound == false)
                {
                    if (entries.Removals.Count != 0)
                        throw new InvalidOperationException($"Attempt to remove entries from new term: '{term}' for field {_indexedField.Name}! This is a bug.");

                    _writer.CreatePostingListForNewTerm(ref entries, _tmpBuf, out termId);
                    _dumper.WriteAddition(term, termId);
                    _fieldTree.BulkUpdateSet(ref key, termId, _curPage, pageOffset, ref _offsetAdjustment);
                }
                else
                {
                    var entriesToTermResult = _writer.AddEntriesToTerm(_tmpBuf, postingListId, isNullTerm: false, ref entries, out termId);
                    switch (entriesToTermResult)
                    {
                        case AddEntriesToTermResult.UpdateTermId:
                            if (termId != postingListId)
                                _dumper.WriteRemoval(term, postingListId);
                            _dumper.WriteAddition(term, termId);
                            _fieldTree.BulkUpdateSet(ref key, termId, _curPage, pageOffset, ref _offsetAdjustment);
                            break;
                        case AddEntriesToTermResult.RemoveTermId:
                            if (_fieldTree.BulkUpdateRemove(ref key, _curPage, pageOffset, ref _offsetAdjustment, out long oldValue) == false)
                            {
                                _dumper.WriteRemoval(term, termId);
                                ThrowTriedToDeleteTermThatDoesNotExists(term, _fieldName);
                            }

                            _dumper.WriteRemoval(term, oldValue);
                            _writer._numberOfTermModifications--;
                            break;
                        case AddEntriesToTermResult.NothingToDo:
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(entriesToTermResult.ToString());
                    }
                }
                
                _writer._entriesToTermsTracker.ProcessCurrentEntriesForTerm(key.ToLong());
            }
        }
        
        private void PrepareNumericalFieldBatch(
            Span<TKey> sortedTerms,
            Span<int> termsIndexes,
            out Span<TLookupKey> keys,
            out Span<long> postingListIds,
            out Span<int> pageOffsets)
        {
            var max = Math.Min(FieldBuffers<TKey, TLookupKey>.BatchSize, sortedTerms.Length);

            for (int idX = 0; idX < max; idX++)
            {
                var term = sortedTerms[idX];
                TLookupKey ilk;
                if (typeof(TLookupKey) == typeof(Int64LookupKey))
                    ilk = (TLookupKey)(object)new Int64LookupKey(((long)(object)term));
                else if (typeof(TLookupKey) == typeof(DoubleLookupKey))
                    ilk = (TLookupKey)(object)new DoubleLookupKey(((double)(object)term));
                else
                    throw new InvalidOperationException($"Type {typeof(TLookupKey).FullName} is not supported");

                _buffers.Keys[idX] = ilk;

                ref var entries = ref _indexedField.Storage.GetAsRef(termsIndexes[idX]);
                entries.Prepare(_writer._entriesAllocator);
            }

            keys = _buffers.Keys.AsSpan(start: 0, length: max);
            postingListIds = _buffers.PostListIds.AsSpan(start: 0, length: max);
            pageOffsets = _buffers.PageOffsets.AsSpan(start: 0, length: max);
        }

        private FieldBuffers<TKey, TLookupKey> GetBuffers()
        {
            if (typeof(TLookupKey) == typeof(Int64LookupKey))
            {
                _writer._longFieldBuffers ??= new(_writer);
                return (FieldBuffers<TKey, TLookupKey>)(object)_writer._longFieldBuffers;
            }

            if (typeof(TLookupKey) == typeof(DoubleLookupKey))
            {
                _writer._doubleFieldBuffers ??= new(_writer);
                return (FieldBuffers<TKey, TLookupKey>)(object)_writer._doubleFieldBuffers;
            }

            throw new InvalidOperationException(
                $"{nameof(NumericalFieldInserter<TKey, TLookupKey>)} does not support {typeof(TLookupKey).FullName} ( {typeof(TLookupKey).Name}>)");
        }
    }
}
