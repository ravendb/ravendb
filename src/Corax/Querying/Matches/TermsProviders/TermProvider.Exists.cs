using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.CompactTrees;
using Voron.Data.Graphs;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Util;

namespace Corax.Querying.Matches.TermsProviders
{
    public struct ExistsTermsProvider<TLookupIterator> : ITermsProvider, IAggregationProvider, IIndexedTermsRetriever
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly long _numberOfTerms;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        
        private readonly bool _nullExists;
        private bool _fetchNulls;
        private readonly long _nullPostingListId;
        
        private CompactTree.Iterator<TLookupIterator> _iterator;

        public ExistsTermsProvider(IndexSearcher searcher, CompactTree tree, in FieldMetadata field, bool skipNulls = false)
        {
            _field = field;
            _searcher = searcher;
            // A sorted index-only scan (SortedDrivingMatch / SortedDrivingWithTieBreakMatch) owns null, so we shouldn't emit it as well.
            _nullExists = false;
            if (skipNulls == false && _searcher.TryGetPostingListForNull(field, out _nullPostingListId))
            {
                using var nullPostingList = _searcher.GetPostingList(_nullPostingListId);
                // The null posting-list container can linger with zero entries after every null-valued document is deleted
                _nullExists = nullPostingList.State.NumberOfEntries > 0;
            }
            _fetchNulls = _nullExists;

            _iterator = tree.Iterate<TLookupIterator>();
            _numberOfTerms = tree.NumberOfEntries;
            _iterator.Reset();
        }

        public int FillPostingListIds(Span<long> postingListIds)
        {
            if (_fetchNulls)
            {
                postingListIds[0] = _nullPostingListId;
                _fetchNulls = false;
                return 1;
            }

            return _iterator.Fill(postingListIds);
        }

        public void Reset()
        {
            _fetchNulls = _nullExists;
            _iterator.Reset();
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

        public ConvertTo Type => ConvertTo.String;

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(ExistsTermsProvider<TLookupIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { Constants.QueryInspectionNode.FieldName, _field.ToString() }
                            });
        }
        
        /// <summary>
        /// For simple facet(FieldName): faster than normal since statistics are gathered in bulk.
        /// </summary>
        public unsafe IDisposable AggregateByTerms(out List<string> terms, out Span<long> counts)
        {
            terms = new List<string>(NumberOfTerms);
            var scope = _searcher.Allocator.Allocate((sizeof(long) + sizeof(UnmanagedSpan)) * NumberOfTerms, out ByteString termsBuffer);
            Span<long> termCount = termsBuffer.ToSpan<long>().Slice(0, NumberOfTerms);
            var termIdx = 0;
            
            if (_fetchNulls)
            {
                terms.Add(Constants.ProjectionNullValue);
                using var nullPostingList = _searcher.GetPostingList(_nullPostingListId);
                termCount[termIdx++] = nullPostingList.State.NumberOfEntries;
                _fetchNulls = false;
            }

            var compactKey = _searcher._transaction.LowLevelTransaction.AcquireCompactKey();
            try
            {
                while (_iterator.MoveNext(compactKey, out long postingListId))
                {
                    var key = compactKey.Decoded();

                    int termSize = key.Length;
                    if (key.Length > 1)
                    {
                        if (key[^1] == 0)
                            termSize--;
                    }

                    var term = key.SequenceEqual(Constants.EmptyStringByteSpan)
                        ? Constants.ProjectionEmptyString
                        : Encodings.Utf8.GetString(key.Slice(0, termSize));

                    terms.Add(term);
                    termCount[termIdx++] = postingListId;
                }
            }
            finally
            {
                _searcher._transaction.LowLevelTransaction.ReleaseCompactKey(ref compactKey);
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
                    containersIds[i] = (long)EntryIdEncodings.GetContainerId(termCount[i]);
                    continue;
                }
                
                containersIds[i] = -1;
            }
            
            
            Voron.Data.Containers.Container.GetAll(_searcher._transaction.LowLevelTransaction, containersIds, new Span<UnmanagedSpan>(containersPtr, containersIds.Length), _searcher.Transaction.LowLevelTransaction.PageLocator);
            
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
            throw new NotSupportedException($"{nameof(ExistsTermsProvider<TLookupIterator>)} supports only terms aggregation.");
        }

        public RangePostingStats CountPostingsInRange(int maxTerms)
        {
            // We count the posting lists size for each one of the terms + null. This overestimate if we have duplicate terms for a single document
            // Callers should either accept that or guard against this - see HasMultipleTermsPerField. 
            var stats = new RangePostingStats();
            var allocator = _searcher.Allocator;
            var llt = _searcher._transaction.LowLevelTransaction;

            Span<NativeList<long>> buckets = stackalloc NativeList<long>[RangePostingBuckets.Count];
            RangePostingBuckets.Initialize(buckets, allocator);

            try
            {
                buckets[(int)(TermIdMask.PostingList)].Add(allocator, _nullPostingListId);
                while (_iterator.MoveNext(out _, out long termId, out _))
                {
                    buckets[(int)(termId & (long)TermIdMask.EnsureIsSingleMask)].Add(allocator, termId);
                    stats.Terms++;

                    if (maxTerms > 0 && stats.Terms >= maxTerms)
                        break;
                }

                RangePostingBuckets.Summarize(buckets, allocator, llt, ref stats);

                return stats;
            }
            finally
            {
                RangePostingBuckets.Release(buckets, allocator);
            }
        }

        public long EstimateTermCountInRange()
        {
            throw new NotSupportedException($"{nameof(ExistsTermsProvider<>)} supports only terms aggregation.");
        }

        public long TotalTermCount()
        {
            throw new NotSupportedException($"{nameof(ExistsTermsProvider<>)} supports only terms aggregation.");
        }

        private int NumberOfTerms => (int)_numberOfTerms + (_nullExists ? 1 : 0);
    }
}
