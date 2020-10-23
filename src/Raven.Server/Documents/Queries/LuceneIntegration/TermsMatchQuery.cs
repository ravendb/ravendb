using System;
using System.Collections.Generic;
using System.Linq;

using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Microsoft.Extensions.DependencyInjection;

namespace Raven.Server.Documents.Queries.LuceneIntegration
{
    public sealed class TermsMatchQuery : MultiTermQuery, IRavenLuceneMethodQuery
    {
        public List<string> Matches { get; private set; }

        public string Field { get; }

        public TermsMatchQuery(string field, List<string> matches)
        {
            Field = field;
            Matches = matches;
            Matches.Sort(StringComparer.Ordinal);
            RewriteMethod = CONSTANT_SCORE_FILTER_REWRITE;
        }

        public override string ToString(string fld)
        {
            return "@in<" + Field + ">(" + string.Join(", ", Matches) + ")";
        }

        protected override FilteredTermEnum GetEnum(IndexReader reader, IState state)
        {
            return new RavenTermsFilteredTermEnum(this, reader, state);
        }

        private sealed class RavenTermsFilteredTermEnum : FilteredTermEnum
        {
            private readonly TermsMatchQuery _termsMatchQuery;
            private readonly IndexReader _reader;
            private int _pos;

            public RavenTermsFilteredTermEnum(TermsMatchQuery termsMatchQuery, IndexReader reader, IState state)
            {
                _termsMatchQuery = termsMatchQuery;
                _reader = reader;
                Next(state);
            }

            public override bool Next(IState state)
            {
                for (; _pos < _termsMatchQuery.Matches.Count; _pos++)
                {
                    using TermEnum termEnum = _reader.Terms(new Term(_termsMatchQuery.Field, _termsMatchQuery.Matches[_pos]), state);
                    if (termEnum.Term == null ||
                        termEnum.Term.Field != _termsMatchQuery.Field)
                        break;

                    for (; _pos < _termsMatchQuery.Matches.Count; _pos++)
                    {
                        int cmp = string.CompareOrdinal(_termsMatchQuery.Matches[_pos], termEnum.Term.Text);
                        if (cmp == 0)
                        {
                            currentTerm = termEnum.Term;
                            _pos++; // position for next call
                            return true;
                        }

                        if (cmp > 0)
                        {
                            break; // search the next term
                        }
                    }
                }

                return false;
            }

            protected override bool TermCompare(Term term)
            {
                throw new NotSupportedException("Shouldn't be called");
            }

            public override float Difference()
            {
                return 1.0f;
            }

            public override bool EndEnum()
            {
                return _pos >= _termsMatchQuery.Matches.Count;
            }
        }

        public IRavenLuceneMethodQuery Merge(IRavenLuceneMethodQuery other)
        {
            var termsMatchQuery = (TermsMatchQuery)other;
            Matches.AddRange(termsMatchQuery.Matches);

            Matches = Matches.Distinct()
                             .Where(x => string.IsNullOrWhiteSpace(x) == false)
                             .OrderBy(s => s, StringComparer.Ordinal)
                             .ToList();
            return this;
        }
    }
}
