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
using Lucene.Net.Index;
using IndexReader = Lucene.Net.Index.IndexReader;
using TermDocs = Lucene.Net.Index.TermDocs;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace Lucene.Net.Search.Function
{

    /// <summary> Expert: A Query that sets the scores of document to the
    /// values obtained from a <see cref="Lucene.Net.Search.Function.ValueSource">ValueSource</see>.
    /// <p/>
    /// This query provides a score for <em>each and every</em> undeleted document in the index.    
    /// <p/>
    /// The value source can be based on a (cached) value of an indexed field, but it
    /// can also be based on an external source, e.g. values read from an external database. 
    /// <p/>
    /// Score is set as: Score(doc,query) = query.getBoost()<sup>2</sup> * valueSource(doc).  
    /// 
    /// <p/><font color="#FF0000">
    /// WARNING: The status of the <b>Search.Function</b> package is experimental. 
    /// The APIs introduced here might change in the future and will not be 
    /// supported anymore in such a case.</font>
    /// </summary>
    [Serializable]
    public class ValueSourceQuery:Query
	{
		internal ValueSource valSrc;
		
		/// <summary> Create a value source query</summary>
		/// <param name="valSrc">provides the values defines the function to be used for scoring
		/// </param>
		public ValueSourceQuery(ValueSource valSrc)
		{
			this.valSrc = valSrc;
		}
		
		/*(non-Javadoc) <see cref="Lucene.Net.Search.Query.rewrite(Lucene.Net.Index.IndexReader) */
		public override Query Rewrite(IndexReader reader, IState state)
		{
			return this;
		}
		
		/*(non-Javadoc) <see cref="Lucene.Net.Search.Query.extractTerms(java.util.Set) */
		public override void  ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
			// no terms involved here
		}

        [Serializable]
        internal class ValueSourceWeight:Weight
		{
			private void  InitBlock(ValueSourceQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private ValueSourceQuery enclosingInstance;
			public ValueSourceQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal Similarity similarity;
			internal float queryNorm;
			internal float queryWeight;
			
			public ValueSourceWeight(ValueSourceQuery enclosingInstance, Searcher searcher)
			{
				InitBlock(enclosingInstance);
				this.similarity = Enclosing_Instance.GetSimilarity(searcher);
			}
			
			/*(non-Javadoc) <see cref="Lucene.Net.Search.Weight.getQuery() */

		    public override Query Query
		    {
		        get { return Enclosing_Instance; }
		    }

		    /*(non-Javadoc) <see cref="Lucene.Net.Search.Weight.getValue() */

		    public override float Value
		    {
		        get { return queryWeight; }
		    }

		    /*(non-Javadoc) <see cref="Lucene.Net.Search.Weight.sumOfSquaredWeights() */

		    public override float GetSumOfSquaredWeights()
		    {
		        queryWeight = Enclosing_Instance.Boost;
		        return queryWeight*queryWeight;
		    }

		    /*(non-Javadoc) <see cref="Lucene.Net.Search.Weight.normalize(float) */
			public override void  Normalize(float norm)
			{
				this.queryNorm = norm;
				queryWeight *= this.queryNorm;
			}
			
			public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
			{
				return new ValueSourceScorer(enclosingInstance, similarity, reader, this, state);
			}
			
			/*(non-Javadoc) <see cref="Lucene.Net.Search.Weight.explain(Lucene.Net.Index.IndexReader, int) */
			public override Explanation Explain(IndexReader reader, int doc, IState state)
			{
			    DocValues vals = enclosingInstance.valSrc.GetValues(reader, state);
			    float sc = queryWeight*vals.FloatVal(doc);

                Explanation result = new ComplexExplanation(true, sc, enclosingInstance.ToString() + ", product of:")
			    ;
                result.AddDetail(vals.Explain(doc));
			    result.AddDetail(new Explanation(enclosingInstance.Boost, "boost"));
			    result.AddDetail(new Explanation(queryNorm, "queryNorm"));
			    return result;
			}
		}
		
		/// <summary> A scorer that (simply) matches all documents, and scores each document with 
		/// the value of the value soure in effect. As an example, if the value source 
		/// is a (cached) field source, then value of that field in that document will 
		/// be used. (assuming field is indexed for this doc, with a single token.)   
		/// </summary>
        private class ValueSourceScorer : Scorer
        {
            private void InitBlock(ValueSourceQuery enclosingInstance)
            {
                this.enclosingInstance = enclosingInstance;
            }
            private ValueSourceQuery enclosingInstance;
            public ValueSourceQuery Enclosing_Instance
            {
                get
                {
                    return enclosingInstance;
                }

            }
            private ValueSourceWeight weight;
            private float qWeight;
            private DocValues vals;
            private TermDocs termDocs;
            private int doc = -1;

            // constructor
            internal ValueSourceScorer(ValueSourceQuery enclosingInstance, Similarity similarity, IndexReader reader, ValueSourceWeight w, IState state)
                : base(similarity)
            {
                InitBlock(enclosingInstance);
                this.weight = w;
                this.qWeight = w.Value;
                // this is when/where the values are first created.
                vals = Enclosing_Instance.valSrc.GetValues(reader, state);
                termDocs = reader.TermDocs(null, state);
            }

            public override int NextDoc(IState state)
            {
                return doc = termDocs.Next(state) ? termDocs.Doc : NO_MORE_DOCS;
            }

            public override int DocID()
            {
                return doc;
            }

            public override int Advance(int target, IState state)
            {
                return doc = termDocs.SkipTo(target, state) ? termDocs.Doc : NO_MORE_DOCS;
            }

            /*(non-Javadoc) <see cref="Lucene.Net.Search.Scorer.explain(int) */
            public override float Score(IState state)
            {
                return qWeight * vals.FloatVal(termDocs.Doc);
            }
        }

		public override Weight CreateWeight(Searcher searcher, IState state)
		{
			return new ValueSourceQuery.ValueSourceWeight(this, searcher);
		}
		
		public override System.String ToString(System.String field)
		{
			return valSrc.ToString() + ToStringUtils.Boost(Boost);
		}
		
		/// <summary>Returns true if <c>o</c> is equal to this. </summary>
		public  override bool Equals(System.Object o)
		{
			if (GetType() != o.GetType())
			{
				return false;
			}
			ValueSourceQuery other = (ValueSourceQuery) o;
			return this.Boost == other.Boost && this.valSrc.Equals(other.valSrc);
		}
		
		/// <summary>Returns a hash code value for this object. </summary>
		public override int GetHashCode()
		{
			return (GetType().GetHashCode() + valSrc.GetHashCode()) ^ BitConverter.ToInt32(BitConverter.GetBytes(Boost), 0);
        }

		override public System.Object Clone()
		{
			return this.MemberwiseClone();
		}
	}
}