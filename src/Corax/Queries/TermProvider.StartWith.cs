using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Queries
{
    public struct StartWithTermProvider : ITermProvider
    {
        private readonly IndexSearcher _searcher;
        private readonly CompactTree.Iterator _iterator;
        private readonly string _field;
        private readonly Slice _startWith;
        private readonly int _fieldId;


        public StartWithTermProvider(IndexSearcher searcher, ByteStringContext context, CompactTree tree, string field, int fieldId, string startWith)
        {
            _searcher = searcher;
            _field = field;
            _fieldId = fieldId;
            _iterator = tree.Iterate();

            Slice.From(context, startWith, out _startWith);
            _iterator.Seek(_startWith);
        }

        public void Reset() => _iterator.Seek(_startWith);

        public bool Next(out TermMatch term)
        {

            if (!_iterator.MoveNext(out Slice termSlice, out var _) || !termSlice.StartWith(_startWith))
            {
                term = TermMatch.CreateEmpty();
                return false;
            }

            term = _searcher.TermQuery(_field, termSlice.ToString());
            return true;
        }

        public bool Evaluate(long id)
        {
            throw new NotImplementedException();
        }
    }
}
