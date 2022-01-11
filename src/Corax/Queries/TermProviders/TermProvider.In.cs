using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct InTermProvider : ITermProvider
    {
        private readonly IndexSearcher _searcher;
        private readonly string _field;
        private readonly List<string> _terms;
        private int _termIndex;

        public InTermProvider(IndexSearcher searcher, string field, List<string> terms)
        {
            _searcher = searcher;
            _field = field;
            _terms = terms;
            _termIndex = -1;
        }

        public void Reset() => _termIndex = -1;

        public bool Next(out TermMatch term)
        {
            _termIndex++;
            if (_termIndex >= _terms.Count)
            {
                term = TermMatch.CreateEmpty();
                return false;
            }
            term = _searcher.TermQuery(_field, _terms[_termIndex]);
            return true;
        }
        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(InTermProvider)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field },
                                { "Terms", string.Join(",", _terms)}
                            });
        }

        string DebugView => Inspect().ToString();
    }
}
