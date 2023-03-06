using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Mappings;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct InTermProvider : ITermProvider
    {
        private readonly IndexSearcher _searcher;
        private readonly List<string> _terms;
        private int _termIndex;
        private readonly FieldMetadata _field;

        public InTermProvider(IndexSearcher searcher, FieldMetadata field, List<string> terms)
        {
            _field = field;
            _searcher = searcher;
            _terms = terms;
            _termIndex = -1;
        }

        public void Reset() => _termIndex = -1;

        public bool Next(out TermMatch term)
        {
            _termIndex++;
            if (_termIndex >= _terms.Count)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
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
                                { "Field", _field.ToString() },
                                { "Terms", string.Join(",", _terms)}
                            });
        }

        string DebugView => Inspect().ToString();
    }
}
