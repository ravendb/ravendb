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

        public bool Next(out TermMatch term)
        {
            _termIndex++;
            if (_termIndex >= _terms.Count)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                return false;
            }

            if (typeof(TTermsType) == typeof((string Term, bool Exact)) && (object)_terms[_termIndex] is (string stringTerm, bool isExact))
                term = _searcher.TermQuery(isExact ? _exactField : _field, stringTerm);
            else if (typeof(TTermsType) == typeof((string Term, bool Exact)) && (object)_terms[_termIndex] is (null, _))
            {
                term = _searcher.TryGetPostingListForNull(_field, out var postingListId) 
                    ? _searcher.TermQuery(_field, postingListId, 1D) 
                    : TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            }
            else if (typeof(TTermsType) == typeof(string))
                term = _searcher.TermQuery(_field, (string)(object)_terms[_termIndex]);
            else if (typeof(TTermsType) == typeof(Slice))
                term = _searcher.TermQuery(_field, (Slice)(object)_terms[_termIndex]);
            else
                term = ThrowInvalidTermType();
        
            return true;
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
        private static TermMatch ThrowInvalidTermType()
        {
            throw new InvalidDataException($"In {nameof(InTermProvider<TTermsType>)} type {nameof(TTermsType)} has to be `string` or `Slice`.");
        }
        
        string DebugView => Inspect().ToString();
    }
}
