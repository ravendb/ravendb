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
    [DebuggerDisplay("{DebugView,nq}")]
    public struct StartWithTermProvider : ITermProvider
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly Slice _startWith;

        private CompactTree.Iterator _iterator;

        public StartWithTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, Slice startWith)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate();
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
            if (key.StartsWith(_startWith) == false)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                return false;
            }

            term = _searcher.TermQuery(_field, _tree, key);
            return true;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(StartWithTermProvider)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() },
                                { "Terms", _startWith.ToString()}
                            });
        }

        public string DebugView => Inspect().ToString();
    }
}
