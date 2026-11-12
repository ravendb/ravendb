using System;
using System.Collections.Generic;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Querying.Matches.TermProviders
{
    public struct ContainsTermProvider<TLookupIterator> : ITermProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly CompactTree _tree;
        private readonly Querying.IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _term;
        private readonly double _averageTermLength;

        private CompactTree.Iterator<TLookupIterator> _iterator;


        public ContainsTermProvider(Querying.IndexSearcher searcher, CompactTree tree, in FieldMetadata field, CompactKey term)
        {
            _tree = tree;
            _searcher = searcher;
            _field = field;
            _averageTermLength = field.HasBoost ? searcher.GetAverageTermLength(field, tree) : 0;
            _iterator = tree.Iterate<TLookupIterator>();
            _iterator.Reset();
            _term = term;
        }

        public bool IsFillSupported => false;

        public int Fill(Span<long> containers)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            _iterator = _tree.Iterate<TLookupIterator>();
            _iterator.Reset();
        }

        public bool Next(out long termId, out double termRatioToWholeCollection)
        {
            var contains = _term.Decoded();
            using var scope = new CompactKeyCacheScope(_searcher._transaction.LowLevelTransaction);
            var key = scope.Key;
            while (_iterator.MoveNext(key, out termId, out _))
            {
                var termSlice = key.Decoded();
                if (!termSlice.Contains(contains))
                {
                    continue;
                }

                termRatioToWholeCollection = Querying.IndexSearcher.GetTermRatioToWholeCollection(key, _averageTermLength);
                return true;
            }

            termRatioToWholeCollection = 1;
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(ContainsTermProvider<TLookupIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { Constants.QueryInspectionNode.FieldName, _field.ToString() },
                                { Constants.QueryInspectionNode.Term, _term.ToString()}
                            });
        }
    }
}
