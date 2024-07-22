using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;

namespace Corax.Querying.Matches.TermProviders
{
    public struct ExistsTermProvider<TLookupIterator> : ITermProvider, IAggregationProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly long _numberOfTerms;
        private readonly CompactTree _tree;
        private readonly Querying.IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        
        
        private readonly bool _nullExists;
        private readonly PostingList _nullPostingList;
        private PostingList.Iterator _nullIterator;
        private bool _fetchNulls;
        private long _postingListId;
        
        private CompactTree.Iterator<TLookupIterator> _iterator;
        private readonly CompactKey _compactKey;

        public ExistsTermProvider(Querying.IndexSearcher searcher, CompactTree tree, in FieldMetadata field, bool forAggregation = false)
        {
            _tree = tree;
            _field = field;
            _searcher = searcher;
            _nullIterator = default;
            _nullExists = false;
            _fetchNulls = false;
            if (_searcher.TryGetPostingListForNull(field, out  _postingListId))
            {
                _nullPostingList = searcher.GetPostingList(_postingListId);
                _nullExists = _nullPostingList.State.NumberOfEntries > 0;
                if (_nullExists)
                {
                    _nullIterator = _nullPostingList.Iterate();
                    _fetchNulls = true;
                }
            }

            if (forAggregation)
            {
                _compactKey = _searcher._transaction.LowLevelTransaction.AcquireCompactKey();
                _compactKey.Initialize(_searcher._transaction.LowLevelTransaction);
            }
            
            _iterator = tree.Iterate<TLookupIterator>();
            _numberOfTerms = tree.NumberOfEntries;
            _iterator.Reset();
        }

        public bool IsFillSupported => true;

        public int Fill(Span<long> containers)
        {
            if (_fetchNulls)
            {
                if (_nullIterator.Fill(containers, out var total))
                {
                    return total;
                }
                
                _fetchNulls = false;
            }
            
            return _iterator.Fill(containers);
        }

        public void Reset()
        {
            _fetchNulls = _nullExists;
            if (_fetchNulls)
                _nullIterator = _nullPostingList.Iterate();

            _iterator.Reset();
        }
        
        public bool Next(out TermMatch term)
        {
            if (_fetchNulls)
            {
                _fetchNulls = false;
                term = _searcher.TermQuery(_field, containerId: _postingListId, 1D);
                return true;
            }
            
            while (_iterator.MoveNext(out var key, out _, out _))
            {
                term = _searcher.TermQuery(_field, key, _tree);
                return true;
            }

            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public bool GetNextTerm(out ReadOnlySpan<byte> term)
        {
            if (_fetchNulls)
            {
                term = Constants.ProjectionNullValueSlice;
                _fetchNulls = false;
                return true;
            }
            
            while (_iterator.MoveNext(out var compactKey, out long _, out _))
            {
                var key = compactKey.Decoded();
                int termSize = key.Length;
                if (key.Length > 1)
                {
                    if (key[^1] == 0)
                        termSize--;
                }

                term = key.Slice(0, termSize);
                return true;
            }

            term = Span<byte>.Empty;
            return false;
        }
        
        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(ExistsTermProvider<TLookupIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { Constants.QueryInspectionNode.FieldName, _field.ToString() }
                            });
        }
        
        /// <summary>
        /// Created for simple facet(FieldName) purposes. This is faster than normal since we're gathering all statistics in bulks.
        /// </summary>
        /// <param name="terms"></param>
        /// <param name="counts"></param>
        /// <returns></returns>
        public unsafe IDisposable AggregateByTerms(out List<string> terms, out Span<long> counts)
        {
            terms = new List<string>(NumberOfTerms);
            var scope = _searcher.Allocator.Allocate((sizeof(long) + sizeof(UnmanagedSpan)) * NumberOfTerms, out ByteString termsBuffer);
            Span<long> termCount = termsBuffer.ToSpan<long>().Slice(0, NumberOfTerms);
            var termIdx = 0;
            
            if (_fetchNulls)
            {
                terms.Add(Constants.ProjectionNullValue);
                termCount[termIdx++] = _nullPostingList.State.NumberOfEntries;
                _fetchNulls = false;
            }

            while (_iterator.MoveNext(_compactKey, out long postingListId, out _))
            {
                var key = _compactKey.Decoded();
                int termSize = key.Length;
                if (key.Length > 1)
                {
                    if (key[^1] == 0)
                        termSize--;
                }
                
                var term = Encodings.Utf8.GetString(key.Slice(0, termSize));
                terms.Add(term);
                
                termCount[termIdx++] = postingListId;
            }


            var containersPtr = (UnmanagedSpan*)(termsBuffer.Ptr + (sizeof(long) * NumberOfTerms));
            using var __ = _searcher.Allocator.Allocate(NumberOfTerms, out Span<long> containersIds);

            if (_nullExists)
                containersIds[0] = -1L;

            for (int i = _nullExists ? 1 : 0; i < NumberOfTerms; ++i)
            {
                if ((termCount[i] & (long)TermIdMask.EnsureIsSingleMask) != 0)
                {
                    Debug.Assert((termCount[i] & (long)TermIdMask.PostingList) != 0 || (termCount[i] & (long)TermIdMask.SmallPostingList) != 0);
                    containersIds[i] = EntryIdEncodings.GetContainerId(termCount[i]);
                    continue;
                }
                
                containersIds[i] = -1;
            }
            
            
            Voron.Data.Containers.Container.GetAll(_searcher._transaction.LowLevelTransaction, containersIds, containersPtr, -1, _searcher.Transaction.LowLevelTransaction.PageLocator);
            
            for (int i = _nullExists ? 1 : 0; i < NumberOfTerms; ++i)
            {
                var containerId = termCount[i];
                
                if ((containerId & (long)TermIdMask.PostingList) != 0)
                    termCount[i] = ((PostingListState*)containersPtr[i].Address)->NumberOfEntries;
                else if ((containerId & (long)TermIdMask.SmallPostingList) != 0)
                    termCount[i] = VariableSizeEncoding.Read<long>(containersPtr[i].Address, out _);
                else
                    termCount[i] = 1;
            }


            counts = termCount;
            return scope;
        }

        public long AggregateByRange()
        {
            throw new NotSupportedException($"{nameof(ExistsTermProvider<TLookupIterator>)} supports only terms aggregation.");
        }
        
        private int NumberOfTerms => (int)_numberOfTerms + (_nullExists ? 1 : 0);
    }
}
