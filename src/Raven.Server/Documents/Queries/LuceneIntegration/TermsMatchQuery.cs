using System;
using System.Collections.Generic;
using System.Linq;

using Lucene.Net.Index;
using Lucene.Net.Search;

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
            RewriteMethod = SCORING_BOOLEAN_QUERY_REWRITE;
        }

        public override string ToString(string fld)
        {
            return "@in<" + Field + ">(" + string.Join(", ", Matches) + ")";
        }

        protected override FilteredTermEnum GetEnum(IndexReader reader)
        {
            return new RavenTermsFilteredTermEnum(this, reader);
        }

        private sealed class RavenTermsFilteredTermEnum : FilteredTermEnum
        {
            private readonly TermsMatchQuery _termsMatchQuery;
            private readonly IndexReader _reader;
            private bool _endEnum;
            private int _pos;

            public RavenTermsFilteredTermEnum(TermsMatchQuery termsMatchQuery, IndexReader reader)
            {
                this._termsMatchQuery = termsMatchQuery;
                this._reader = reader;
                if (this._termsMatchQuery.Matches.Count == 0)
                {
                    _endEnum = true;
                    return;
                }
                MoveToCurrentTerm();
            }

            private void MoveToCurrentTerm()
            {
                if (actualEnum != null)
                    actualEnum.Dispose();
                SetEnum(_reader.Terms(new Term(_termsMatchQuery.Field, _termsMatchQuery.Matches[_pos])));
                movedEnum = true;
            }

            private bool movedEnum;

            public override bool Next()
            {
                if (actualEnum == null)
                    return false; // the actual enumerator is not initialized!
                currentTerm = null;
                while (EndEnum() == false && actualEnum.Next())
                {
                    do
                    {
                        movedEnum = false;
                        var term = actualEnum.Term;
                        if (CompareTermAndMoveToNext(term, move: true) == false)
                            continue;
                        currentTerm = term;
                        return true;
                    } while (movedEnum);
                }
                currentTerm = null;
                return false;
            }

            protected override bool TermCompare(Term term)
            {
                return CompareTermAndMoveToNext(term, move: false);
            }

            private bool CompareTermAndMoveToNext(Term term, bool move)
            {
                if (term == null || term.Field != _termsMatchQuery.Field)
                {
                    _endEnum = true;
                    return false;
                }
                int last;
                while (true)
                {
                    if (_pos >= _termsMatchQuery.Matches.Count)
                    {
                        _endEnum = true;
                        return false;
                    }
                    last = string.CompareOrdinal(_termsMatchQuery.Matches[_pos], term.Text);
                    if (last >= 0)
                    {
                        break;
                    }
                    _pos++;
                }
                if (last > 0 && move)
                {
                    MoveToCurrentTerm();
                    return false;
                }
                return last == 0;
            }

            public override float Difference()
            {
                return 1.0f;
            }

            public override bool EndEnum()
            {
                return _endEnum;
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