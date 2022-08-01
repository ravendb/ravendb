using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        private readonly Slice _fieldName;
        private readonly Slice _endsWith;
        private readonly CompactTree _tree;

        public NotEndsWithTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, Slice fieldName, int fieldId, Slice endsWith)
        {
            _searcher = searcher;
            _fieldName = fieldName;
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

                term = _searcher.TermQuery(_tree, termSlice);
                return true;
            }

            term = TermMatch.CreateEmpty(_searcher.Allocator);
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(NotEndsWithTermProvider)}",
                parameters: new Dictionary<string, string>()
                {
                    { "Field", _fieldName.ToString() },
                    { "Terms", _endsWith.ToString()}
                });
        }

        string DebugView => Inspect().ToString();
    }
}
