using System;
using System.Collections.Generic;
using System.Reflection;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Voron;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Global;
using Voron.Impl;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct AllEntriesMatch : IQueryMatch
    {
        private readonly Transaction _tx;
        private readonly long _count;
        private Container.AllPagesIterator _entriesPagesIt;
        private int _offset;
        private int _itemsLeftOnCurrentPage;
        private Page _currentPage;
        private long _entriesContainerId;

        public unsafe AllEntriesMatch(IndexSearcher searcher, Transaction tx)
        {
            _tx = tx;
            _count = searcher.NumberOfEntries;
            if (_count == 0)
            {
                Unsafe.SkipInit(out _currentPage);
                Unsafe.SkipInit(out _offset);
                Unsafe.SkipInit(out _entriesContainerId);
                Unsafe.SkipInit(out _entriesPagesIt);
                Unsafe.SkipInit(out _entriesContainerId);
                Unsafe.SkipInit(out _itemsLeftOnCurrentPage);
                return;
            }
            
            _entriesContainerId = tx.OpenContainer(Constants.IndexWriter.EntriesContainerSlice);
            _entriesPagesIt = Container.GetAllPagesSet(tx.LowLevelTransaction, _entriesContainerId);
            _offset = 0;
            _itemsLeftOnCurrentPage = 0;
            _currentPage = new Page(null);
        }

        public bool IsBoosting => false;
        public long Count => _count;
        public QueryCountConfidence Confidence => QueryCountConfidence.High;

        public unsafe int Fill(Span<long> matches)
        {
            if (_count == 0)
                return 0;
            
            var results = 0;
            while (true)
            {
                if (_currentPage.IsValid == false || _itemsLeftOnCurrentPage == 0)
                {
                    if (_entriesPagesIt.TryMoveNext(out var pageNum) == false)
                    {
                        return results;
                    }

                    _currentPage = _tx.LowLevelTransaction.GetPage(pageNum);
                    _offset = 0;
                }

                _itemsLeftOnCurrentPage = int.MaxValue;
                while (results < matches.Length && _itemsLeftOnCurrentPage != 0)
                {
                    var read = Container.GetEntriesInto(_entriesContainerId, ref _offset, _currentPage, matches[results..], out _itemsLeftOnCurrentPage);
                    if (read == 0)
                    {
                        _currentPage = new Page(null);
                        break;
                    }

                    results += read;
                }

                if (results == matches.Length)
                    return results;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            // this match *everything*, so ands with everything 
            return matches;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            //there is no sense to add anything here because this would add same value to all items in collection.
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode(nameof(AllEntriesMatch),
                parameters: new Dictionary<string, string>()
                {
                    { nameof(IsBoosting), IsBoosting.ToString() },
                    { nameof(Count), $"{Count} [{Confidence}]" }
                });
        }

        string DebugView => Inspect().ToString();
    }
}
