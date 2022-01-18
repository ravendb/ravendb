using System;
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
    public sealed class InQuery : Query
    {
        public List<string> Matches;

        public string Field;

        public InQuery(string field, List<string> matches)
        {
            Field = field;
            Matches = matches;
            Matches.Sort(StringComparer.Ordinal);
        }


        public override Weight CreateWeight(Searcher searcher, IState state)
        {
            return new InQueryWeight(this, searcher);
        }

        private class SharedArrayDisjunctionMaxScorer : DisjunctionMaxScorer
        {
            private Scorer[] _subScorers;

            public SharedArrayDisjunctionMaxScorer(float tieBreakerMultiplier, Similarity similarity, Scorer[] subScorers, int numScorers) : base(tieBreakerMultiplier, similarity, subScorers, numScorers)
            {
                _subScorers = subScorers;
            }

            public override int NextDoc(IState state)
            {
                var result = base.NextDoc(state);
                if (result == NO_MORE_DOCS && _subScorers != null)
                {
                    ArrayPool<Scorer>.Shared.Return(_subScorers);
                    _subScorers = null;
                }
                return result;
            }

            public override int Advance(int target, IState state)
            {
                int result = base.Advance(target, state);
                if (result == NO_MORE_DOCS && _subScorers != null)
                {
                    ArrayPool<Scorer>.Shared.Return(_subScorers);
                    _subScorers = null;
                }
                return result;
            }
        }
        
        private class InQueryWeight : Weight
        {
            private readonly InQuery _parent;
            private readonly Searcher _searcher;
            private float _queryWeight;

            public InQueryWeight(InQuery parent, Searcher searcher)
            {
                _parent = parent;
                _searcher = searcher;
                _queryWeight = parent.Boost;
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
                    return new LazyInitInScorer(_parent, reader, state, similarity);

                var scorers = ArrayPool<Scorer>.Shared.Rent(_parent.Matches.Count);
                int index = 0;
                byte[] norms = reader.Norms(_parent.Field, state);
                foreach (var match in _parent.Matches)
                {
                    var termDocs = reader.TermDocs(new Term(_parent.Field, match),state);
                    var scorer = new TermScorer(this, termDocs,similarity, norms);
                    if (scorer.NextDoc(state) != DocIdSetIterator.NO_MORE_DOCS)
                    {
                        scorers[index++] = scorer;
                    }
                }
                return new SharedArrayDisjunctionMaxScorer(1.0f, similarity, scorers, index);
            }

            public override float GetSumOfSquaredWeights()
            {
                return _queryWeight * _queryWeight;
            }

            public override Query Query => _parent;
            public override float Value => _queryWeight;
        }
        
        private class LazyInitInScorer : Scorer
        {
            private readonly InQuery _parent;
            private readonly IndexReader _reader;
            private readonly IState _state;
            private readonly Similarity _similarity;
            private FastBitArrayScorer  _inner;

            internal LazyInitInScorer(InQuery parent, IndexReader reader, IState state, Similarity similarity) : base(similarity)
            {
                _parent = parent;
                _reader = reader;
                _state = state;
                _similarity = similarity;
            }

            private FastBitArrayScorer InitIfNeeded()
            {
                if (_inner != null)
                    return _inner;
                
                var docs = new FastBitArray(_reader.MaxDoc);
                bool hasValue = false;
                foreach (string match in _parent.Matches)
                {
                    using var termDocs = _reader.TermDocs(new Term(_parent.Field, match), _state);
                    while (termDocs.Next(_state))
                    {
                        if (hasValue == false)
                        {
                            hasValue = true;
                        }

                        docs.Set(termDocs.Doc);
                    }
                }

                return _inner = new FastBitArrayScorer(docs, _similarity, disposeArray: true);
            }

            public override int DocID()
            {
                return InitIfNeeded().DocID();
            }

            public override int NextDoc(IState state)
            {
                return InitIfNeeded().NextDoc(state);
            }

            public override int Advance(int target, IState state)
            {
                return InitIfNeeded().Advance(target, state);
            }

            public override float Score(IState state)
            {
                return InitIfNeeded().Score(state);
            }
        }

        private bool Equals(InQuery other)
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
            return ReferenceEquals(this, obj) || obj is InQuery other && Equals(other);
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
    }
}

