using System;
using System.Reflection;
using Voron;
using Voron.Data.Containers;
using Voron.Data.Sets;
using Voron.Global;
using Voron.Impl;

namespace Corax.Queries
{
    public struct AllEntriesMatch : IQueryMatch
    {
        private readonly Transaction _tx;
        private readonly long _count;
        private Set.Iterator _entriesPagesIt;
        private int _offset;
        private Page _currentPage;
        private long _entriesContainerId;

        public unsafe AllEntriesMatch(Transaction tx)
        {
            _tx = tx;
            _count = tx.LowLevelTransaction.RootObjects.ReadInt64(IndexWriter.NumberOfEntriesSlice) ?? 0;
            _entriesContainerId = tx.OpenContainer(IndexWriter.EntriesContainerSlice);
            _entriesPagesIt = Container.GetAllPagesSet(tx.LowLevelTransaction, _entriesContainerId).Iterate();
            _offset = 0;
            _currentPage = new Page(null);
        }

        public long Count => _count;
        public QueryCountConfidence Confidence => QueryCountConfidence.High;
        public unsafe int Fill(Span<long> matches)
        {
            var results = 0;
            while (true)
            {
                if (_currentPage.IsValid == false)
                {
                    if (_entriesPagesIt.MoveNext() == false)
                    {
                        return results;
                    }

                    _currentPage = _tx.LowLevelTransaction.GetPage(_entriesPagesIt.Current);
                    _offset = 0;
                }
            
                while (results + 1 < matches.Length)
                {
                    var read = Container.GetEntriesInto(_entriesContainerId, _offset, _currentPage, matches);
                    if (read == 0)
                    {
                        _currentPage = new Page(null);
                        break;
                    }
                    results += read;
                    _offset += read;
                }

                if (results < matches.Length)
                    continue;
                
                return results;
            }
        }

        public int AndWith(Span<long> prevMatches)
        {
            // this match *everything*, so ands with everything 
            return prevMatches.Length;
        }
    }
}
