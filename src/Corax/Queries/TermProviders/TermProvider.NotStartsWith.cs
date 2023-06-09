using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Mappings;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;
using CompactTreeForwardIterator = Voron.Data.CompactTrees.CompactTree.Iterator<Voron.Data.Lookups.Lookup<Voron.Data.CompactTrees.CompactTree.CompactKeyLookup>.ForwardIterator>;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct NotStartWithTermProvider : ITermProvider
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _startWith;

        private CompactTreeForwardIterator _iterator;

        public NotStartWithTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, FieldMetadata field, CompactKey startWith)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate();
            _iterator.Reset();
            _startWith = startWith;
            _tree = tree;
        }

        public void Reset()
        {
            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {
            var startWith = _startWith.Decoded();
            while (_iterator.MoveNext(out var key, out var _))
            {
                var termSlice = key.Decoded();
                if (termSlice.StartsWith(startWith))
                {
                    continue;
                }

                term = _searcher.TermQuery(_field, key, _tree);
                return true;
            }
            
            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(NotStartWithTermProvider)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() },
                                { "Terms", _startWith.ToString()}
                            });
        }

        string DebugView => Inspect().ToString();
    }
}
