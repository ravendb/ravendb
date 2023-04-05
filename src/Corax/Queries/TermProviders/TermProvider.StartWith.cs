using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Corax.Mappings;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public struct StartsWithTermProvider : ITermProvider
    {
        private StartsWithTermProvider<CompactTree.ForwardIterator> _inner;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StartsWithTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, CompactKey startWith)
        {
            _inner = new StartsWithTermProvider<CompactTree.ForwardIterator>(searcher, tree, field, startWith);
        }

        public bool IsOrdered => _inner.IsOrdered;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            _inner.Reset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Next(out TermMatch term)
        {
            return _inner.Next(out term);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }
    }


    [DebuggerDisplay("{DebugView,nq}")]
    public struct StartsWithTermProvider<TIterator> : ITermProvider
        where TIterator : struct, ICompactTreeIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly CompactKey _startWith;

        private TIterator _iterator;

        public bool IsOrdered => true;

        public StartsWithTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, CompactKey startWith)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate<TIterator>();
            _startWith = startWith;
            _tree = tree;

            _iterator.Seek(_startWith);
        }

        public void Reset() => _iterator.Seek(_startWith);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Next(out TermMatch term)
        {
            var result = Next(out term, out var scope);
            scope.Dispose();
            return result;
        } 

        public bool Next(out TermMatch term, out CompactKeyCacheScope termScope)
        {
            if (_iterator.MoveNext(out termScope, out var _) == false)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                return false;
            }

            var key = termScope.Key.Decoded();
            if (key.StartsWith(_startWith.Decoded()) == false)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                return false;
            }

            term = _searcher.TermQuery(_field, termScope.Key, _tree);
            return true;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(StartsWithTermProvider<TIterator>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() },
                                { "Terms", _startWith.ToString()}
                            });
        }

        public string DebugView => Inspect().ToString();
    }
}
