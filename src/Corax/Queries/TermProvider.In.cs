using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
    public struct InTermProvider : ITermProvider
    {
        private readonly IndexSearcher _searcher;
        private readonly string _field;
        private readonly int _fieldId;
        private readonly List<string> _terms;
        private int _termIndex;

        public InTermProvider(IndexSearcher searcher, string field, int fieldId, List<string> terms)
        {
            _searcher = searcher;
            _field = field;
            _fieldId = fieldId;
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

        public bool Evaluate(long id)
        {
            var entry = _searcher.GetReaderFor(id);
            var fieldType = entry.GetFieldType(_fieldId);
            if (fieldType.HasFlag(IndexEntryFieldType.List))
            {
                // TODO: Federico fixme please
            }
            if (entry.Read(_fieldId, out var value) == false)
                return false;

            //TODO: fix me, allocations, O(N^2), etc
            return _terms.Contains(Encoding.UTF8.GetString(value));
        }
    }
}
