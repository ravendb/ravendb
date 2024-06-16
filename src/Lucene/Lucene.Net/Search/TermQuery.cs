/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Lucene.Net.Store;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using TermDocs = Lucene.Net.Index.TermDocs;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;
using IDFExplanation = Lucene.Net.Search.Explanation.IDFExplanation;

namespace Lucene.Net.Search
{

    /// <summary>A Query that matches documents containing a term.
    /// This may be combined with other terms with a <see cref="BooleanQuery" />.
    /// </summary>

        [Serializable]
    public class TermQuery:Query
	{
		private Term term;


        [Serializable]
        private class TermWeight:Weight
		{
			private void  InitBlock(TermQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TermQuery enclosingInstance;
			public TermQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private Similarity similarity;
			private float value_Renamed;
			private float idf;
			private float queryNorm;
			private float queryWeight;
			private IDFExplanation idfExp;
			
			public TermWeight(TermQuery enclosingInstance, Searcher searcher, IState state)
			{
				InitBlock(enclosingInstance);
				this.similarity = Enclosing_Instance.GetSimilarity(searcher);
				idfExp = similarity.IdfExplain(Enclosing_Instance.term, searcher, state);
				idf = idfExp.Idf;
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
		        queryWeight = idf*Enclosing_Instance.Boost; // compute query weight
		        return queryWeight*queryWeight; // square it
		    }

		    public override void  Normalize(float queryNorm)
			{
				this.queryNorm = queryNorm;
				queryWeight *= queryNorm; // normalize query weight
				value_Renamed = queryWeight * idf; // idf for document
			}
			
			public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
			{
				TermDocs termDocs = reader.TermDocs(Enclosing_Instance.term, state);
				
				if (termDocs == null)
					return null;
				
				return new TermScorer(this, termDocs, similarity, reader.Norms(Enclosing_Instance.term.Field, state));
			}
			
			public override Explanation Explain(IndexReader reader, int doc, IState state)
			{
				
				ComplexExplanation result = new ComplexExplanation();
				result.Description = "weight(" + Query + " in " + doc + "), product of:";
				
				Explanation expl = new Explanation(idf, idfExp.Explain());
				
				// explain query weight
				Explanation queryExpl = new Explanation();
				queryExpl.Description = "queryWeight(" + Query + "), product of:";
				
				Explanation boostExpl = new Explanation(Enclosing_Instance.Boost, "boost");
				if (Enclosing_Instance.Boost != 1.0f)
					queryExpl.AddDetail(boostExpl);
				queryExpl.AddDetail(expl);
				
				Explanation queryNormExpl = new Explanation(queryNorm, "queryNorm");
				queryExpl.AddDetail(queryNormExpl);
				
				queryExpl.Value = boostExpl.Value * expl.Value * queryNormExpl.Value;
				
				result.AddDetail(queryExpl);
				
				// explain field weight
				System.String field = Enclosing_Instance.term.Field;
				ComplexExplanation fieldExpl = new ComplexExplanation();
				fieldExpl.Description = "fieldWeight(" + Enclosing_Instance.term + " in " + doc + "), product of:";

                Explanation tfExplanation = new Explanation();
                int tf = 0;
                TermDocs termDocs = reader.TermDocs(enclosingInstance.term, state);
                if (termDocs != null)
                {
                    try
                    {
                        if (termDocs.SkipTo(doc, state) && termDocs.Doc == doc)
                        {
                            tf = termDocs.Freq;
                        }
                    }
                    finally
                    {
                        termDocs.Close();
                    }
                    tfExplanation.Value = similarity.Tf(tf);
                    tfExplanation.Description = "tf(termFreq(" + enclosingInstance.term + ")=" + tf + ")";
                }
                else
                {
                    tfExplanation.Value = 0.0f;
                    tfExplanation.Description = "no matching term";
                }
                fieldExpl.AddDetail(tfExplanation);
				fieldExpl.AddDetail(expl);
				
				Explanation fieldNormExpl = new Explanation();
				byte[] fieldNorms = reader.Norms(field, state);
				float fieldNorm = fieldNorms != null?Similarity.DecodeNorm(fieldNorms[doc]):1.0f;
				fieldNormExpl.Value = fieldNorm;
				fieldNormExpl.Description = "fieldNorm(field=" + field + ", doc=" + doc + ")";
				fieldExpl.AddDetail(fieldNormExpl);

                fieldExpl.Match = tfExplanation.IsMatch;
                fieldExpl.Value = tfExplanation.Value * expl.Value * fieldNormExpl.Value;
				
				result.AddDetail(fieldExpl);
				System.Boolean? tempAux = fieldExpl.Match;
				result.Match = tempAux;
				
				// combine them
				result.Value = queryExpl.Value * fieldExpl.Value;
				
				if (queryExpl.Value == 1.0f)
					return fieldExpl;
				
				return result;
			}
		}
		
		/// <summary>Constructs a query for the term <c>t</c>. </summary>
		public TermQuery(Term t)
		{
			term = t;
		}

	    /// <summary>Returns the term of this query. </summary>
	    public virtual Term Term
	    {
	        get { return term; }
	    }

	    public override Weight CreateWeight(Searcher searcher, IState state)
		{
			return new TermWeight(this, searcher, state);
		}
		
		public override void  ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
		    terms.Add(Term);
		}
		
		/// <summary>Prints a user-readable version of this query. </summary>
		public override System.String ToString(System.String field)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			if (!term.Field.Equals(field))
			{
				buffer.Append(term.Field);
				buffer.Append(":");
			}
			buffer.Append(term.Text);
			buffer.Append(ToStringUtils.Boost(Boost));
			return buffer.ToString();
		}
		
		/// <summary>Returns true iff <c>o</c> is equal to this. </summary>
		public  override bool Equals(System.Object o)
		{
			if (!(o is TermQuery))
				return false;
			TermQuery other = (TermQuery) o;
			return (this.Boost == other.Boost) && this.term.Equals(other.term);
		}
		
		/// <summary>Returns a hash code value for this object.</summary>
		public override int GetHashCode()
		{
			return BitConverter.ToInt32(BitConverter.GetBytes(Boost), 0) ^ term.GetHashCode();
        }
	}
}