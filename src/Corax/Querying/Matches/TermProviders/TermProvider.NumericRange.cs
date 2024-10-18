using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Extensions;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Util;
using Range = Corax.Querying.Matches.Meta.Range;


namespace Corax.Querying.Matches.TermProviders
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct TermNumericRangeProvider<TLookupIterator, TLow, THigh, TVal> : ITermProvider, IAggregationProvider
        where TLookupIterator : struct, ILookupIterator
        where TLow : struct, Range.Marker
        where THigh  : struct, Range.Marker
        where TVal : struct, ILookupKey
    {
        private readonly Querying.IndexSearcher _searcher;
        private readonly Lookup<TVal> _set;
        private readonly FieldMetadata _field;
        private TVal _low, _high;
        private TLookupIterator _iterator;
        private bool _skipRangeCheck;
        private long _lastTermId = -1;
        private bool _includeLastTerm = true;
        private bool _isEmpty;

        public TermNumericRangeProvider(Querying.IndexSearcher searcher, Lookup<TVal> set, in FieldMetadata field, TVal low, TVal high)
        {
            _searcher = searcher;
            _set = set;
            _field = field;
            _iterator = set.Iterate<TLookupIterator>();
            _low = low;
            _high = high;

            //Unbounded query can skip checking range after first element (since we're taking ALL possible results from starting point)
            _skipRangeCheck = _iterator.IsForward switch
            {
                true when typeof(THigh) == typeof(Range.Inclusive) && typeof(TVal) == typeof(Int64LookupKey) && ((Int64LookupKey)(object)high).Value == long.MaxValue => true,
                true when typeof(THigh) == typeof(Range.Inclusive) && typeof(TVal) == typeof(DoubleLookupKey) && ((DoubleLookupKey)(object)high).Value.AlmostEquals(double.MaxValue) => true,
                false when typeof(TLow) == typeof(Range.Inclusive) && typeof(TVal) == typeof(Int64LookupKey) && ((Int64LookupKey)(object)low).Value == long.MinValue => true,
                false when typeof(TLow) == typeof(Range.Inclusive) && typeof(TVal) == typeof(DoubleLookupKey) && ((DoubleLookupKey)(object)low).Value.AlmostEquals(double.MinValue) => true,
                _ => false
            };

            PrepareKeys();
            Reset();
        }
        
        private void PrepareKeys()
        {
            // We want to rewrite the range, so it starts and ends with elements that are presented in our data source
            TVal startKey = _iterator.IsForward ? _low : _high;
            TVal finalKey = _iterator.IsForward ? _high : _low;

            _iterator.Seek(startKey);
            
            // The iterator may be positioned at:
            // - Element equal to startKey
            // - Element succeeding the startKey
            // - End of the collection if no elements match
            if (_iterator.MoveNext(out TVal currentKey, out _, out _) == false)
            {
                // When MoveNext returns false it means we jumped over all values stored inside a lookup tree
                _isEmpty = true;
                return;
            }

            // The first element may be equal to the startKey. For exclusive range start we want to skip it
            var skipFirst = (_iterator.IsForward ? typeof(TLow) : typeof(THigh)) == typeof(Range.Exclusive) 
                            && startKey.IsEqual(currentKey);
            
            // If we're supposed to skip first element, we rewrite the range to start from element succeeding startKey
            if (skipFirst)
            {
                if (_iterator.MoveNext(out currentKey, out _, out _) == false)
                {
                    // We moved past queried range, nothing matches the query
                    _isEmpty = true;
                    return;
                }
            }
            
            // Update the range accordingly to the iterator option
            if (_iterator.IsForward)
                _low = currentKey;
            else
                _high = currentKey;
            
            if (_skipRangeCheck)
                return;
            
            _iterator.Seek(finalKey);
            
            // The iterator may be positioned at:
            // - Element equal to finalKey
            // - Element succeeding the finalKey
            // - End of the collection if no elements match
            if (_iterator.MoveNext(out currentKey, out _lastTermId, out _) == false)
            {
                // We jumped over all data stored in the tree. Other side of the range is unbound
                _skipRangeCheck = true;
                return;
            }
            
            _includeLastTerm = true;
            
            // Compare the range boundary (finalKey) with the current element (currentKey)
            // Result:
            //   1  - Range boundary is greater than the current element
            //   0  - Range boundary is equal to the current element
            //  -1  - Range boundary is less than the current element (not expected)
            var cmp = finalKey.CompareTo(currentKey);
            
            if (_iterator.IsForward)
            {
                if (typeof(THigh) == typeof(Range.Exclusive) && cmp <= 0 ||
                    typeof(THigh) == typeof(Range.Inclusive) && cmp < 0)
                {
                    _includeLastTerm = false;
                }
            }
            else
            {
                if (typeof(TLow) == typeof(Range.Exclusive) && cmp >= 0 ||
                    typeof(TLow) == typeof(Range.Inclusive) && cmp > 0)
                    _includeLastTerm = false;
            }
        }

        public bool IsFillSupported => true;

        public int Fill(Span<long> containers)
        {
            if (_isEmpty) return 0;
            return _iterator.Fill(containers, _lastTermId, _includeLastTerm);
        }

        public void Reset()
        {
            if (_isEmpty)
                return;
            
            _iterator.Reset();
            _iterator.Seek(_iterator.IsForward ? _low : _high);
        }
        
        public bool Next(out TermMatch term)
        {
            if (_isEmpty)
            {
                term = default;
                return false;
            }
            bool hasNext = _iterator.MoveNext(out var termId);
            if (hasNext == false)
                goto Empty;


            if (termId == _lastTermId)
            {
                _isEmpty = true;
                if (_includeLastTerm == false)
                    goto Empty;
            }
            
            // Ratio will be always 1 (sizeof(T)/sizeof(T))
            term = _searcher.TermQuery(_field, termId, 1);
            return true;

            Empty:
            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            string lowValue;
            if ((typeof(TVal) == typeof(Int64LookupKey) && ((Int64LookupKey)(object)_low).Value == long.MinValue) ||
                (typeof(TVal) == typeof(DoubleLookupKey) && ((DoubleLookupKey)(object)_low).Value.AlmostEquals(double.MinValue)))
                lowValue = null;
            else
                lowValue = _low.ToString();
            
            string highValue;
            if ((typeof(TVal) == typeof(Int64LookupKey) && ((Int64LookupKey)(object)_high).Value == long.MaxValue) ||
                (typeof(TVal) == typeof(DoubleLookupKey) && ((DoubleLookupKey)(object)_high).Value.AlmostEquals(double.MaxValue)))
                highValue = null;
            else
                highValue = _high.ToString();
            
            return new QueryInspectionNode(nameof(TermNumericRangeProvider<TLookupIterator, TLow, THigh, TVal>),
                            parameters: new Dictionary<string, string>()
                            {
                                { Constants.QueryInspectionNode.FieldName, _field.ToString() },
                                { Constants.QueryInspectionNode.LowValue, lowValue},
                                { Constants.QueryInspectionNode.HighValue, highValue},
                                { Constants.QueryInspectionNode.LowOption, typeof(TLow).Name},
                                { Constants.QueryInspectionNode.HighOption, typeof(THigh).Name},
                                { Constants.QueryInspectionNode.IteratorDirection, Constants.QueryInspectionNode.IterationDirectionName(_iterator)}
                            });
        }

        public string DebugView => Inspect().ToString();
        
        public unsafe IDisposable AggregateByTerms(out List<string> terms, out Span<long> counts)
        {
            throw new NotSupportedException($"Primitive {nameof(TermNumericRangeProvider<TLookupIterator, TLow, THigh, TVal>)} doesnt support aggregation by terms.");
        }
        
        public unsafe long AggregateByRange()
        {
            //we do not support Long ranges since we want to perform aggregation on doubles 
            Debug.Assert(typeof(TVal) == typeof(DoubleLookupKey), "typeof(TVal) == typeof(DoubleLookupKey)");
            
            const long singleMarker = -1L;
            if (_isEmpty)
            {
                return 0;
            }

            var allocator = _searcher.Allocator;
            
            NativeList<long> postingLists = new();
            postingLists.Initialize(allocator);
            NativeList<TermIdMask> postingListsType = new();
            postingListsType.Initialize(allocator);
            
            while (_isEmpty == false && _iterator.MoveNext(out var termId))
            {
                if (termId == _lastTermId)
                {
                    _isEmpty = true;
                    if (_includeLastTerm == false)
                        break;
                }
                
                if ((termId & (long)TermIdMask.PostingList) != 0)
                {
                    postingLists.Add(allocator, EntryIdEncodings.GetContainerId(termId));
                    postingListsType.Add(allocator, TermIdMask.PostingList);
                }
                else if ((termId & (long)TermIdMask.SmallPostingList) != 0)
                {
                    postingLists.Add(allocator, EntryIdEncodings.GetContainerId(termId));
                    postingListsType.Add(allocator, TermIdMask.SmallPostingList);
                }
                else
                {
                    postingLists.Add(allocator, singleMarker);
                    postingListsType.Add(allocator, TermIdMask.Single);
                }
            }
            
            using var _ = allocator.Allocate((sizeof(UnmanagedSpan)) * postingLists.Count, out ByteString containers);
            var containersPtr = (UnmanagedSpan*)containers.Ptr;
      
            Container.GetAll(_searcher._transaction.LowLevelTransaction, postingLists.ToSpan(), containersPtr, singleMarker, _searcher._transaction.LowLevelTransaction.PageLocator);

            long totalCount = 0;
            for (int i = 0; i < postingLists.Count; ++i)
            {
                var localCount = (postingListsType[i]) switch
                {
                    TermIdMask.PostingList => ((PostingListState*)(containersPtr[i].Address))->NumberOfEntries,
                    TermIdMask.SmallPostingList => VariableSizeEncoding.Read<long>(containersPtr[i].Address, out var _),
                    TermIdMask.Single => 1,
                    _ => throw new InvalidDataException($"Supported posting lists types are: 'PostingList', 'SmallPostingList', 'Single' but got {postingListsType[i]}")
                };

                totalCount += localCount;
            }
            
            postingLists.Dispose(allocator);
            postingListsType.Dispose(allocator);
            return totalCount;
        }

        public int NumberOfTerms => throw new NotSupportedException($"{nameof(NumberOfTerms)} is not supported in {nameof(TermNumericRangeProvider<TLookupIterator, TLow, THigh, TVal>)}."); // unknown
    }
}
