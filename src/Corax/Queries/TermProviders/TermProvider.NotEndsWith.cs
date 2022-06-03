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
        private readonly string _field;
        private readonly Slice _endsWith;

        public NotEndsWithTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, string field, int fieldId, Slice endsWith)
        {
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate();
            _iterator.Reset();
            _endsWith = endsWith;
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

                term = _searcher.TermQuery(_field, termSlice);
                return true;
            }

            term = TermMatch.CreateEmpty();
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(NotEndsWithTermProvider)}",
                parameters: new Dictionary<string, string>()
                {
                    { "Field", _field },
                    { "Terms", _endsWith.ToString()}
                });
        }

        string DebugView => Inspect().ToString();
    }
}
