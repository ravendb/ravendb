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
using Lucene.Net.Support;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using TermPositions = Lucene.Net.Index.TermPositions;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;
using IDFExplanation = Lucene.Net.Search.Explanation.IDFExplanation;

namespace Lucene.Net.Search
{

    /// <summary>A Query that matches documents containing a particular sequence of terms.
    /// A PhraseQuery is built by QueryParser for input like <c>"new york"</c>.
    /// 
    /// <p/>This query may be combined with other terms or queries with a <see cref="BooleanQuery" />.
    /// </summary>

        [Serializable]
    public class PhraseQuery:Query
	{
		private System.String field;
        private EquatableList<Term> terms = new EquatableList<Term>(4);
        private EquatableList<int> positions = new EquatableList<int>(4);
		private int maxPosition = 0;
		private int slop = 0;
		
		/// <summary>Constructs an empty phrase query. </summary>
		public PhraseQuery()
		{
		}

	    /// <summary>Sets the number of other words permitted between words in query phrase.
	    /// If zero, then this is an exact phrase search.  For larger values this works
	    /// like a <c>WITHIN</c> or <c>NEAR</c> operator.
	    /// <p/>The slop is in fact an edit-distance, where the units correspond to
	    /// moves of terms in the query phrase out of position.  For example, to switch
	    /// the order of two words requires two moves (the first move places the words
	    /// atop one another), so to permit re-orderings of phrases, the slop must be
	    /// at least two.
	    /// <p/>More exact matches are scored higher than sloppier matches, thus search
	    /// results are sorted by exactness.
	    /// <p/>The slop is zero by default, requiring exact matches.
	    /// </summary>
	    public virtual int Slop
	    {
	        get { return slop; }
	        set { slop = value; }
	    }

	    /// <summary> Adds a term to the end of the query phrase.
		/// The relative position of the term is the one immediately after the last term added.
		/// </summary>
		public virtual void  Add(Term term)
		{
			int position = 0;
			if (positions.Count > 0)
				position = positions[positions.Count - 1] + 1;
			
			Add(term, position);
		}
		
		/// <summary> Adds a term to the end of the query phrase.
		/// The relative position of the term within the phrase is specified explicitly.
		/// This allows e.g. phrases with more than one term at the same position
		/// or phrases with gaps (e.g. in connection with stopwords).
		/// 
		/// </summary>
		/// <param name="term">
		/// </param>
		/// <param name="position">
		/// </param>
		public virtual void  Add(Term term, int position)
		{
			if (terms.Count == 0)
				field = term.Field;
			else if ((System.Object) term.Field != (System.Object) field)
			{
				throw new System.ArgumentException("All phrase terms must be in the same field: " + term);
			}
			
			terms.Add(term);
			positions.Add(position);
			if (position > maxPosition)
				maxPosition = position;
		}
		
		/// <summary>Returns the set of terms in this phrase. </summary>
		public virtual Term[] GetTerms()
		{
			return terms.ToArray();
		}
		
		/// <summary> Returns the relative positions of terms in this phrase.</summary>
		public virtual int[] GetPositions()
		{
			int[] result = new int[positions.Count];
			for (int i = 0; i < positions.Count; i++)
				result[i] = positions[i];
			return result;
		}


        [Serializable]
        private class PhraseWeight:Weight
		{
			private void  InitBlock(PhraseQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private PhraseQuery enclosingInstance;
			public PhraseQuery Enclosing_Instance
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
			
			public PhraseWeight(PhraseQuery enclosingInstance, Searcher searcher, IState state)
			{
				InitBlock(enclosingInstance);
				this.similarity = Enclosing_Instance.GetSimilarity(searcher);
				
				idfExp = similarity.IdfExplain(Enclosing_Instance.terms, searcher, state);
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
				if (Enclosing_Instance.terms.Count == 0)
				// optimize zero-term case
					return null;
				
				TermPositions[] tps = new TermPositions[Enclosing_Instance.terms.Count];
				for (int i = 0; i < Enclosing_Instance.terms.Count; i++)
				{
					TermPositions p = reader.TermPositions(Enclosing_Instance.terms[i], state);
					if (p == null)
						return null;
					tps[i] = p;
				}
				
				if (Enclosing_Instance.slop == 0)
				// optimize exact case
					return new ExactPhraseScorer(this, tps, Enclosing_Instance.GetPositions(), similarity, reader.Norms(Enclosing_Instance.field, state));
				else
					return new SloppyPhraseScorer(this, tps, Enclosing_Instance.GetPositions(), similarity, Enclosing_Instance.slop, reader.Norms(Enclosing_Instance.field, state));
			}
			
			public override Explanation Explain(IndexReader reader, int doc, IState state)
			{
				
				Explanation result = new Explanation();
				result.Description = "weight(" + Query + " in " + doc + "), product of:";
				
				System.Text.StringBuilder docFreqs = new System.Text.StringBuilder();
				System.Text.StringBuilder query = new System.Text.StringBuilder();
				query.Append('\"');
				docFreqs.Append(idfExp.Explain());
				for (int i = 0; i < Enclosing_Instance.terms.Count; i++)
				{
					if (i != 0)
					{
						query.Append(" ");
					}
					
					Term term = Enclosing_Instance.terms[i];
					
					query.Append(term.Text);
				}
				query.Append('\"');
				
				Explanation idfExpl = new Explanation(idf, "idf(" + Enclosing_Instance.field + ":" + docFreqs + ")");
				
				// explain query weight
				Explanation queryExpl = new Explanation();
				queryExpl.Description = "queryWeight(" + Query + "), product of:";
				
				Explanation boostExpl = new Explanation(Enclosing_Instance.Boost, "boost");
				if (Enclosing_Instance.Boost != 1.0f)
					queryExpl.AddDetail(boostExpl);
				queryExpl.AddDetail(idfExpl);
				
				Explanation queryNormExpl = new Explanation(queryNorm, "queryNorm");
				queryExpl.AddDetail(queryNormExpl);
				
				queryExpl.Value = boostExpl.Value * idfExpl.Value * queryNormExpl.Value;
				
				result.AddDetail(queryExpl);
				
				// explain field weight
				Explanation fieldExpl = new Explanation();
				fieldExpl.Description = "fieldWeight(" + Enclosing_Instance.field + ":" + query + " in " + doc + "), product of:";
				
				PhraseScorer scorer = (PhraseScorer)Scorer(reader, true, false, state);
				if (scorer == null)
				{
					return new Explanation(0.0f, "no matching docs");
				}
                Explanation tfExplanation = new Explanation();
                int d = scorer.Advance(doc, state);
                float phraseFreq = (d == doc) ? scorer.CurrentFreq() : 0.0f;
                tfExplanation.Value = similarity.Tf(phraseFreq);
                tfExplanation.Description = "tf(phraseFreq=" + phraseFreq + ")";

                fieldExpl.AddDetail(tfExplanation);
				fieldExpl.AddDetail(idfExpl);
				
				Explanation fieldNormExpl = new Explanation();
				byte[] fieldNorms = reader.Norms(Enclosing_Instance.field, state);
				float fieldNorm = fieldNorms != null?Similarity.DecodeNorm(fieldNorms[doc]):1.0f;
				fieldNormExpl.Value = fieldNorm;
				fieldNormExpl.Description = "fieldNorm(field=" + Enclosing_Instance.field + ", doc=" + doc + ")";
				fieldExpl.AddDetail(fieldNormExpl);

                fieldExpl.Value = tfExplanation.Value * idfExpl.Value * fieldNormExpl.Value;
				
				result.AddDetail(fieldExpl);
				
				// combine them
				result.Value = queryExpl.Value * fieldExpl.Value;
				
				if (queryExpl.Value == 1.0f)
					return fieldExpl;
				
				return result;
			}
		}
		
		public override Weight CreateWeight(Searcher searcher, IState state)
		{
			if (terms.Count == 1)
			{
				// optimize one-term case
				Term term = terms[0];
				Query termQuery = new TermQuery(term);
				termQuery.Boost = Boost;
				return termQuery.CreateWeight(searcher, state);
			}
			return new PhraseWeight(this, searcher, state);
		}
		
		/// <seealso cref="Lucene.Net.Search.Query.ExtractTerms(System.Collections.Generic.ISet{Term})">
		/// </seealso>
		public override void ExtractTerms(System.Collections.Generic.ISet<Term> queryTerms)
		{
		    queryTerms.UnionWith(terms);
		}
		
		/// <summary>Prints a user-readable version of this query. </summary>
		public override System.String ToString(System.String f)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			if (field != null && !field.Equals(f))
			{
				buffer.Append(field);
				buffer.Append(":");
			}
			
			buffer.Append("\"");
			System.String[] pieces = new System.String[maxPosition + 1];
			for (int i = 0; i < terms.Count; i++)
			{
				int pos = positions[i];
				System.String s = pieces[pos];
				if (s == null)
				{
					s = terms[i].Text;
				}
				else
				{
					s = s + "|" + terms[i].Text;
				}
				pieces[pos] = s;
			}
			for (int i = 0; i < pieces.Length; i++)
			{
				if (i > 0)
				{
					buffer.Append(' ');
				}
				System.String s = pieces[i];
				if (s == null)
				{
					buffer.Append('?');
				}
				else
				{
					buffer.Append(s);
				}
			}
			buffer.Append("\"");
			
			if (slop != 0)
			{
				buffer.Append("~");
				buffer.Append(slop);
			}
			
			buffer.Append(ToStringUtils.Boost(Boost));
			
			return buffer.ToString();
		}
		
		/// <summary>Returns true iff <c>o</c> is equal to this. </summary>
		public  override bool Equals(System.Object o)
		{
			if (!(o is PhraseQuery))
				return false;
			PhraseQuery other = (PhraseQuery) o;
			return (this.Boost == other.Boost) && (this.slop == other.slop) && this.terms.Equals(other.terms) && this.positions.Equals(other.positions);
		}
		
		/// <summary>Returns a hash code value for this object.</summary>
		public override int GetHashCode()
		{
			return BitConverter.ToInt32(BitConverter.GetBytes(Boost), 0) ^ slop ^ terms.GetHashCode() ^ positions.GetHashCode();
		}
	}
}