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
        private readonly IndexSearcher _searcher;
        private CompactTree.Iterator _iterator;
        private readonly FieldMetadata _field;
        private readonly Slice _startWith;
        private readonly CompactTree _tree;
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
        public bool Next(out TermMatch term) => Next(out term, out _);

        public bool Next(out TermMatch term, out Slice termSlice)
        {
            if (_iterator.MoveNext(out termSlice, out var _) == false || termSlice.StartWith(_startWith) == false)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                return false;
            }

            term = _searcher.TermQuery(_field, _tree, termSlice);
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
