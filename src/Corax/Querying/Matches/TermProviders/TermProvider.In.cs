using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Voron;

namespace Corax.Querying.Matches.TermProviders
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct InTermProvider<TTermsType> : ITermProvider
    {
        private readonly IndexSearcher _searcher;
        private readonly List<TTermsType> _terms;
        private int _termIndex;
        private readonly FieldMetadata _field;
        private readonly FieldMetadata _exactField;

        public InTermProvider(IndexSearcher searcher, in FieldMetadata field, List<TTermsType> terms)
        {
            _field = field;
            _exactField = field.ChangeAnalyzer(FieldIndexingMode.Exact);
            
            _searcher = searcher;
            _terms = terms;
            _termIndex = -1;
        }

        public bool IsFillSupported { get; }
        public int Fill(Span<long> containers)
        {
            throw new NotImplementedException();
        }

        public void Reset() => _termIndex = -1;

        public bool Next(out long termId, out double termRatioToWholeCollection)
        {
            while (++_termIndex < _terms.Count)
            {
                var found = false;
                termRatioToWholeCollection = 1D;
                if (typeof(TTermsType) == typeof((string Term, bool Exact)) && (object)_terms[_termIndex] is (string stringTerm, bool isExact))
                    found = _searcher.TryGetTermId(isExact ? _exactField : _field, stringTerm, out termId, out termRatioToWholeCollection);
                else if (typeof(TTermsType) == typeof((string Term, bool Exact)) && (object)_terms[_termIndex] is (null, _))
                    found = _searcher.TryGetPostingListForNull(_field, out termId);
                else if (typeof(TTermsType) == typeof(string))
                    found = _searcher.TryGetTermId(_field, (string)(object)_terms[_termIndex], out termId, out termRatioToWholeCollection);
                else if (typeof(TTermsType) == typeof(Slice))
                    found = _searcher.TryGetTermId(_field, (Slice)(object)_terms[_termIndex], out termId, out termRatioToWholeCollection);
                else
                    termId = ThrowInvalidTermType();

                if (found)
                    return true;
            }

            termId = -1;
            termRatioToWholeCollection = 1D;
            return false;
        }
        
        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(InTermProvider<TTermsType>)}",
                            parameters: new Dictionary<string, string>()
                            {
                                { Constants.QueryInspectionNode.FieldName, _field.ToString() },
                                { Constants.QueryInspectionNode.Term, string.Join(",", _terms)}
                            });
        }

        [DoesNotReturn]
        private static long ThrowInvalidTermType()
        {
            throw new InvalidDataException($"In {nameof(InTermProvider<TTermsType>)} type {nameof(TTermsType)} has to be `string` or `Slice`.");
        }
        
        string DebugView => Inspect().ToString();
    }
}
