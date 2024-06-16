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
using MultipleTermPositions = Lucene.Net.Index.MultipleTermPositions;
using Term = Lucene.Net.Index.Term;
using TermPositions = Lucene.Net.Index.TermPositions;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;

namespace Lucene.Net.Search
{

    /// <summary> MultiPhraseQuery is a generalized version of PhraseQuery, with an added
    /// method <see cref="Add(Term[])" />.
    /// To use this class, to search for the phrase "Microsoft app*" first use
    /// add(Term) on the term "Microsoft", then find all terms that have "app" as
    /// prefix using IndexReader.terms(Term), and use MultiPhraseQuery.add(Term[]
    /// terms) to add them to the query.
    /// 
    /// </summary>
    /// <version>  1.0
    /// </version>

        [Serializable]
    public class MultiPhraseQuery:Query
	{
		private System.String field;
        private System.Collections.Generic.List<Term[]> termArrays = new System.Collections.Generic.List<Term[]>();
        private System.Collections.Generic.List<int> positions = new System.Collections.Generic.List<int>();
		
		private int slop = 0;

	    /// <summary>Gets or sets the phrase slop for this query.</summary>
	    /// <seealso cref="PhraseQuery.Slop">
	    /// </seealso>
	    public virtual int Slop
	    {
	        get { return slop; }
	        set { slop = value; }
	    }

	    /// <summary>Add a single term at the next position in the phrase.</summary>
		/// <seealso cref="PhraseQuery.Add(Term)">
		/// </seealso>
		public virtual void  Add(Term term)
		{
			Add(new Term[]{term});
		}
		
		/// <summary>Add multiple terms at the next position in the phrase.  Any of the terms
		/// may match.
		/// 
		/// </summary>
		/// <seealso cref="PhraseQuery.Add(Term)">
		/// </seealso>
		public virtual void  Add(Term[] terms)
		{
			int position = 0;
			if (positions.Count > 0)
				position = positions[positions.Count - 1] + 1;
			
			Add(terms, position);
		}
		
		/// <summary> Allows to specify the relative position of terms within the phrase.
		/// 
		/// </summary>
		/// <seealso cref="PhraseQuery.Add(Term, int)">
		/// </seealso>
		/// <param name="terms">
		/// </param>
		/// <param name="position">
		/// </param>
		public virtual void  Add(Term[] terms, int position)
		{
			if (termArrays.Count == 0)
				field = terms[0].Field;
			
			for (int i = 0; i < terms.Length; i++)
			{
				if ((System.Object) terms[i].Field != (System.Object) field)
				{
					throw new System.ArgumentException("All phrase terms must be in the same field (" + field + "): " + terms[i]);
				}
			}
			
			termArrays.Add(terms);
			positions.Add(position);
		}

        /// <summary> Returns a List&lt;Term[]&gt; of the terms in the multiphrase.
		/// Do not modify the List or its contents.
		/// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public virtual System.Collections.Generic.IList<Term[]> GetTermArrays()
        {
            return termArrays.AsReadOnly();
        }
		
		/// <summary> Returns the relative positions of terms in this phrase.</summary>
		public virtual int[] GetPositions()
		{
			int[] result = new int[positions.Count];
			for (int i = 0; i < positions.Count; i++)
				result[i] = positions[i];
			return result;
		}
		
		// inherit javadoc
		public override void ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
			foreach(Term[] arr in termArrays)
			{
			    terms.UnionWith(arr);
			}
		}



        [Serializable]
        private class MultiPhraseWeight:Weight
		{
			private void  InitBlock(MultiPhraseQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private MultiPhraseQuery enclosingInstance;
			public MultiPhraseQuery Enclosing_Instance
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
			
			public MultiPhraseWeight(MultiPhraseQuery enclosingInstance, Searcher searcher, IState state)
			{
				InitBlock(enclosingInstance);
				this.similarity = Enclosing_Instance.GetSimilarity(searcher);
				
				// compute idf
			    int maxDoc = searcher.MaxDoc;
                foreach (Term[] terms in enclosingInstance.termArrays)
                {
                    foreach (Term term in terms)
                    {
                        idf += similarity.Idf(searcher.DocFreq(term, state), maxDoc);
                    }
                }
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
				if (Enclosing_Instance.termArrays.Count == 0)
				// optimize zero-term case
					return null;
				
				TermPositions[] tps = new TermPositions[Enclosing_Instance.termArrays.Count];
				for (int i = 0; i < tps.Length; i++)
				{
					Term[] terms = Enclosing_Instance.termArrays[i];
					
					TermPositions p;
					if (terms.Length > 1)
						p = new MultipleTermPositions(reader, terms, state);
					else
						p = reader.TermPositions(terms[0], state);
					
					if (p == null)
						return null;
					
					tps[i] = p;
				}
				
				if (Enclosing_Instance.slop == 0)
					return new ExactPhraseScorer(this, tps, Enclosing_Instance.GetPositions(), similarity, reader.Norms(Enclosing_Instance.field, state));
				else
					return new SloppyPhraseScorer(this, tps, Enclosing_Instance.GetPositions(), similarity, Enclosing_Instance.slop, reader.Norms(Enclosing_Instance.field, state));
			}
			
			public override Explanation Explain(IndexReader reader, int doc, IState state)
			{
				ComplexExplanation result = new ComplexExplanation();
				result.Description = "weight(" + Query + " in " + doc + "), product of:";
				
				Explanation idfExpl = new Explanation(idf, "idf(" + Query + ")");
				
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
				ComplexExplanation fieldExpl = new ComplexExplanation();
				fieldExpl.Description = "fieldWeight(" + Query + " in " + doc + "), product of:";

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
				
				fieldExpl.Match = tfExplanation.IsMatch;
                fieldExpl.Value = tfExplanation.Value * idfExpl.Value * fieldNormExpl.Value;
				
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
		
		public override Query Rewrite(IndexReader reader, IState state)
		{
			if (termArrays.Count == 1)
			{
				// optimize one-term case
				Term[] terms = termArrays[0];
				BooleanQuery boq = new BooleanQuery(true);
				for (int i = 0; i < terms.Length; i++)
				{
					boq.Add(new TermQuery(terms[i]), Occur.SHOULD);
				}
				boq.Boost = Boost;
				return boq;
			}
			else
			{
				return this;
			}
		}
		
		public override Weight CreateWeight(Searcher searcher, IState state)
		{
			return new MultiPhraseWeight(this, searcher, state);
		}
		
		/// <summary>Prints a user-readable version of this query. </summary>
		public override System.String ToString(System.String f)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			if (!field.Equals(f))
			{
				buffer.Append(field);
				buffer.Append(":");
			}
			
			buffer.Append("\"");
			System.Collections.Generic.IEnumerator<Term[]> i = termArrays.GetEnumerator();
            bool first = true;
			while (i.MoveNext())
			{
                if (first)
                {
                    first = false;
                }
                else
                {
                    buffer.Append(" ");
                }

				Term[] terms = i.Current;
				if (terms.Length > 1)
				{
					buffer.Append("(");
					for (int j = 0; j < terms.Length; j++)
					{
						buffer.Append(terms[j].Text);
						if (j < terms.Length - 1)
							buffer.Append(" ");
					}
					buffer.Append(")");
				}
				else
				{
					buffer.Append(terms[0].Text);
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
		
		
		/// <summary>Returns true if <c>o</c> is equal to this. </summary>
		public  override bool Equals(System.Object o)
		{
			if (!(o is MultiPhraseQuery))
				return false;
			MultiPhraseQuery other = (MultiPhraseQuery) o;
            bool eq = this.Boost == other.Boost && this.slop == other.slop;
            if(!eq)
            {
                return false;
            }
            eq = this.termArrays.Count.Equals(other.termArrays.Count);
            if (!eq)
            {
                return false;
            }

            for (int i = 0; i < this.termArrays.Count; i++)
            {
                if (!Compare.CompareTermArrays((Term[])this.termArrays[i], (Term[])other.termArrays[i]))
                {
                    return false;
                }
            }
            if(!eq)
            {
                return false;
            }
            eq = this.positions.Count.Equals(other.positions.Count);
            if (!eq)
            {
                return false;
            }
            for (int i = 0; i < this.positions.Count; i++)
            {
                if (!((int)this.positions[i] == (int)other.positions[i]))
                {
                    return false;
                }
            }
            return true;
        }
		
		/// <summary>Returns a hash code value for this object.</summary>
		public override int GetHashCode()
		{
            int posHash = 0;
            foreach(int pos in positions)
            {
                posHash += pos.GetHashCode();
            }
			return BitConverter.ToInt32(BitConverter.GetBytes(Boost), 0) ^ slop ^ TermArraysHashCode() ^ posHash ^ 0x4AC65113;
		}
		
		// Breakout calculation of the termArrays hashcode
		private int TermArraysHashCode()
		{
			int hashCode = 1;
			foreach(Term[] termArray in termArrays)
			{
                // Java uses Arrays.hashCode(termArray)
			    hashCode = 31*hashCode + (termArray == null ? 0 : ArraysHashCode(termArray));
			}
			return hashCode;
		}
		
		private int ArraysHashCode(Term[] termArray)
		{
			if (termArray == null)
				return 0;
			
			int result = 1;
			
			for (int i = 0; i < termArray.Length; i++)
			{
				Term term = termArray[i];
				result = 31 * result + (term == null?0:term.GetHashCode());
			}
			
			return result;
		}
		
		// Breakout calculation of the termArrays equals
        private bool TermArraysEquals(System.Collections.Generic.List<Term[]> termArrays1, System.Collections.Generic.List<Term[]> termArrays2)
		{
			if (termArrays1.Count != termArrays2.Count)
			{
				return false;
			}
			var iterator1 = termArrays1.GetEnumerator();
			var iterator2 = termArrays2.GetEnumerator();
			while (iterator1.MoveNext())
			{
				Term[] termArray1 = iterator1.Current;
				Term[] termArray2 = iterator2.Current;
				if (!(termArray1 == null ? termArray2 == null : TermEquals(termArray1, termArray2)))
				{
					return false;
				}
			}
			return true;
		}

        public static bool TermEquals(System.Array array1, System.Array array2)
        {
            bool result = false;
            if ((array1 == null) && (array2 == null))
                result = true;
            else if ((array1 != null) && (array2 != null))
            {
                if (array1.Length == array2.Length)
                {
                    int length = array1.Length;
                    result = true;
                    for (int index = 0; index < length; index++)
                    {
                        if (!(array1.GetValue(index).Equals(array2.GetValue(index))))
                        {
                            result = false;
                            break;
                        }
                    }
                }
            }
            return result;
        }
	}
}