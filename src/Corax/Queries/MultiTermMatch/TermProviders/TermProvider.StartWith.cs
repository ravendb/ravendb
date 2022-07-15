using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries.MultiTermMatch.TermProviders
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct StartWithTermProvider : ITermProvider
    {
        private readonly IndexSearcher _searcher;
        private readonly CompactTree.Iterator _iterator;
        private readonly Slice _fieldName;
        private readonly Slice _startWith;
        private readonly CompactTree _tree;
        public StartWithTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, Slice fieldName, int fieldId, Slice startWith)
        {
            _searcher = searcher;
            _fieldName = fieldName;
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
                term = TermMatch.CreateEmpty();
                return false;
            }

            term = _searcher.TermQuery(_tree, termSlice);
            return true;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(StartWithTermProvider)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _fieldName.ToString() },
                                { "Terms", _startWith.ToString()}
                            });
        }

        public string DebugView => Inspect().ToString();
    }
}
