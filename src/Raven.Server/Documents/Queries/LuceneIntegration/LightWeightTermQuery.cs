using System;
using System.Collections.Generic;
using System.Text;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace Raven.Server.Documents.Queries.LuceneIntegration
{
    [Serializable]
    public class LightWeightTermQuery : TermQuery
    {
        public LightWeightTermQuery(Term t) : base(t)
        {
            
        }

        public override Weight CreateWeight(Searcher searcher, IState state)
        {
            return new LightTermWeight(this, searcher, state);
        }

        [Serializable]
        private class LightTermWeight : Weight
        {
            private void InitBlock(TermQuery enclosingInstance)
            {
                _enclosingInstance = enclosingInstance;
            }
            private TermQuery _enclosingInstance;
            public TermQuery Enclosing_Instance => _enclosingInstance;
            private Similarity similarity;
            private float value_Renamed;
            private float idf;
            private float queryNorm;
            private float queryWeight;

            public LightTermWeight(TermQuery enclosingInstance, Searcher searcher, IState state)
            {
                InitBlock(enclosingInstance);
                this.similarity = Enclosing_Instance.GetSimilarity(searcher);
                idf = 1.0f;
            }

            public override System.String ToString()
            {
                return "weight(" + Enclosing_Instance + ")";
            }

            public override Query Query
            {
                get { return Enclosing_Instance; }
            }

            public override float Value
            {
                get { return value_Renamed; }
            }

            public override float GetSumOfSquaredWeights()
            {
                queryWeight = idf * Enclosing_Instance.Boost; // compute query weight
                return queryWeight * queryWeight; // square it
            }

            public override void Normalize(float queryNorm)
            {
                this.queryNorm = queryNorm;
                queryWeight *= queryNorm; // normalize query weight
                value_Renamed = queryWeight * idf; // idf for document
            }

            public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
            {
                TermDocs termDocs = reader.TermDocs(Enclosing_Instance.Term, state);

                if (termDocs == null)
                    return null;

                //TODO: see if we need to simplify the scorer/similarity
                return new TermScorer(this, termDocs, similarity, reader.Norms(Enclosing_Instance.Term.Field, state));
            }

            public override Lucene.Net.Search.Explanation Explain(IndexReader reader, int doc, IState state)
            {
                throw new NotImplementedException("LightWeightTermQuery doesn't implement explanation this is probably a bug");
            }
        }
    }
}
