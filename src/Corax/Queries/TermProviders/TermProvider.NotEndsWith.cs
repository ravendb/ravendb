using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax.Mappings;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct NotEndsWithTermProvider : ITermProvider
    {
        private readonly IndexSearcher _searcher;
        private readonly CompactTree.Iterator _iterator;
        private readonly FieldMetadata _field;
        private readonly Slice _endsWith;
        private readonly CompactTree _tree;

        public NotEndsWithTermProvider(IndexSearcher searcher, CompactTree tree, FieldMetadata field, Slice endsWith)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate();
            _iterator.Reset();
            _endsWith = endsWith;
            _tree = tree;
        }

        public void Reset()
        {

            _iterator.Reset();
        }

        public bool Next(out TermMatch term)
        {

            while (_iterator.MoveNext(out Slice termSlice, out var _))
            {
                if (termSlice.EndsWith(_endsWith))
                    continue;

                term = _searcher.TermQuery(_field, _tree, termSlice);
                return true;
            }

            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(NotEndsWithTermProvider)}",
                parameters: new Dictionary<string, string>()
                {
                    { "Field", _field.ToString() },
                    { "Terms", _endsWith.ToString()}
                });
        }

        string DebugView => Inspect().ToString();
    }
}
