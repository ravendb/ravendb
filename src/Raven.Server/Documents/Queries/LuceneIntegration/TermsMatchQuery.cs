﻿using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;

using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Microsoft.Extensions.DependencyInjection;

namespace Raven.Server.Documents.Queries.LuceneIntegration
{
    public sealed class TermsMatchQuery : Query, IRavenLuceneMethodQuery
    {
        public List<string> Matches { get; private set; }

        public string Field { get; }

        public TermsMatchQuery(string field, List<string> matches)
        {
            Field = field;
            Matches = matches;
            Matches.Sort(StringComparer.Ordinal);
        }


        public override Weight CreateWeight(Searcher searcher, IState state)
        {
            return new TermMatchQueryWeight(this, searcher);
        }

        private class TermMatchQueryWeight : Weight
        {
            private readonly TermsMatchQuery _parent;
            private readonly Searcher _searcher;
            private float _queryWeight = 1.0f;

            public TermMatchQueryWeight(TermsMatchQuery parent, Searcher searcher)
            {
                _parent = parent;
                _searcher = searcher;
            }
            public override Lucene.Net.Search.Explanation Explain(IndexReader reader, int doc, IState state)
            {
                var result = new ComplexExplanation {Description = _parent.ToString(), Value = _queryWeight};
                result.AddDetail(new Lucene.Net.Search.Explanation(_queryWeight, "queryWeight"));
                return result;
            }

            public override void Normalize(float norm)
            {
                _queryWeight *= norm;
            }

            public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
            {
                Similarity similarity = _parent.GetSimilarity(_searcher);
                if(_parent.Matches.Count > 128)
                    return new EagerTermMatchScorer(_parent, reader, state, similarity);

                var scorers = new Scorer[_parent.Matches.Count];
                int index = 0;
                foreach (var match in _parent.Matches)
                {
                    var termDocs = reader.TermDocs(new Term(_parent.Field, match),state);
                    var scorer = new TermScorer(this, termDocs,similarity, reader.Norms(_parent.Field, state));
                    if (scorer.NextDoc(state) != DocIdSetIterator.NO_MORE_DOCS)
                    {
                        scorers[index++] = scorer;
                    }
                }
                return new DisjunctionMaxScorer(1.0f, similarity, scorers, index);
            }

            public override float GetSumOfSquaredWeights()
            {
                return _queryWeight * _queryWeight;
            }

            public override Query Query { get; }
            public override float Value { get; }
        }
        
        private class EagerTermMatchScorer : Scorer
        {
            private FastBitArray _docs;
            private IEnumerator<int> _enum;
            private int _currentDocId;
            internal EagerTermMatchScorer(TermsMatchQuery parent, IndexReader reader, IState state, Similarity similarity) : base(similarity)
            {
                _docs = new FastBitArray(reader.MaxDoc);

                foreach (string match in parent.Matches)
                {
                    using var termDocs = reader.TermDocs(new Term(parent.Field, match), state);
                    while (termDocs.Next(state))
                    {
                        _docs.Set(termDocs.Doc);
                    }
                }

                _enum = _docs.Iterate(0).GetEnumerator();
                if (_enum.MoveNext() == false)
                {
                    _enum.Dispose();
                    _enum = null;
                }
            }

            public override int DocID()
            {
                return _enum?.Current ?? NO_MORE_DOCS;
            }

            public override int NextDoc(IState state)
            {
                if (_enum?.MoveNext() == true)
                    return _enum.Current;
                _enum?.Dispose();
                _docs?.Dispose();
                _docs = null;
                return NO_MORE_DOCS;
            }

            public override int Advance(int target, IState state)
            {
                if (_docs == null) 
                    return NO_MORE_DOCS;
                
                _enum?.Dispose();
                _enum = _docs.Iterate(target).GetEnumerator();
                return NextDoc(state);
            }

            public override float Score(IState state)
            {
                return 1.0f;
            }
        }

        private bool Equals(TermsMatchQuery other)
        {
            if (Matches.Count != other.Matches.Count)
                return false;
            for (int i = 0; i < Matches.Count; i++)
            {
                if (Matches[i] != other.Matches[i])
                    return false;
            }
            return base.Equals(other) && Field == other.Field;
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is TermsMatchQuery other && Equals(other);
        }

        public override int GetHashCode()
        {
            var combiner = new HashCode();
            combiner.Add(base.GetHashCode());
            combiner.Add(Matches.Count);
            foreach (var t in Matches)
            {
                combiner.Add(t);
            }
            combiner.Add(Field);
            return combiner.ToHashCode();
        }

        public override string ToString(string fld)
        {
            return "@in<" + Field + ">(" + string.Join(", ", Matches) + ")";
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
