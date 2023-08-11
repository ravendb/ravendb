using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using Corax.Mappings;
using Voron;

namespace Corax.Queries.TermProviders
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct InTermProvider<TTermsType> : ITermProvider
    {
        private readonly IndexSearcher _searcher;
        private readonly List<TTermsType> _terms;
        private int _termIndex;
        private readonly FieldMetadata _field;

        public InTermProvider(IndexSearcher searcher, FieldMetadata field, List<TTermsType> terms)
        {
            _field = field;
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
            
            if (typeof(TTermsType) == typeof(string))
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
                                { "Field", _field.ToString() },
                                { "Terms", string.Join(",", _terms)}
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
