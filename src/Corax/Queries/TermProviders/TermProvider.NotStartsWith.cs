using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct NotStartWithTermProvider : ITermProvider
    {
        private readonly IndexSearcher _searcher;
        private readonly CompactTree.Iterator _iterator;
        private readonly Slice _fieldName;
        private readonly Slice _startWith;
        private readonly CompactTree _tree;

        public NotStartWithTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, Slice fieldName, int fieldId, Slice startWith)
        {
            _searcher = searcher;
            _fieldName = fieldName;
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

            while (_iterator.MoveNext(out Slice termSlice, out var _))
            {
                if (termSlice.StartWith(_startWith))
                    continue;

                term = _searcher.TermQuery(_tree, termSlice);
                return true;
            }
            
            term = TermMatch.CreateEmpty();
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(NotStartWithTermProvider)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _fieldName.ToString() },
                                { "Terms", _startWith.ToString()}
                            });
        }

        string DebugView => Inspect().ToString();
    }
}
