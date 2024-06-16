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
using Lucene.Net.Store;
using IndexReader = Lucene.Net.Index.IndexReader;

namespace Lucene.Net.Search
{

    /// <summary> A query that wraps a filter and simply returns a constant score equal to the
    /// query boost for every document in the filter.
    /// </summary>
    [Serializable]
    public class ConstantScoreQuery:Query
	{
		protected internal Filter internalFilter;
		
		public ConstantScoreQuery(Filter filter)
		{
			this.internalFilter = filter;
		}

	    /// <summary>Returns the encapsulated filter </summary>
	    public virtual Filter Filter
	    {
	        get { return internalFilter; }
	    }

	    public override Query Rewrite(IndexReader reader, IState state)
		{
			return this;
		}
		
		public override void ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
			// OK to not add any terms when used for MultiSearcher,
			// but may not be OK for highlighting
		}

        [Serializable]
        protected internal class ConstantWeight:Weight
		{
			private void  InitBlock(ConstantScoreQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private ConstantScoreQuery enclosingInstance;
			public ConstantScoreQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private readonly Similarity similarity;
			private float queryNorm;
			private float queryWeight;
			
			public ConstantWeight(ConstantScoreQuery enclosingInstance, Searcher searcher)
			{
				InitBlock(enclosingInstance);
				this.similarity = Enclosing_Instance.GetSimilarity(searcher);
			}

		    public override Query Query
		    {
		        get { return Enclosing_Instance; }
		    }

		    public override float Value
		    {
		        get { return queryWeight; }
		    }

		    public override float GetSumOfSquaredWeights()
		    {
		        queryWeight = Enclosing_Instance.Boost;
		        return queryWeight*queryWeight;
		    }

		    public override void  Normalize(float norm)
			{
				this.queryNorm = norm;
				queryWeight *= this.queryNorm;
			}
			
			public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
			{
				return new ConstantScorer(enclosingInstance, similarity, reader, this, state);
			}
			
			public override Explanation Explain(IndexReader reader, int doc, IState state)
			{
				
				var cs = new ConstantScorer(enclosingInstance, similarity, reader, this, state);
				bool exists = cs.docIdSetIterator.Advance(doc, state) == doc;
				
				var result = new ComplexExplanation();
				
				if (exists)
				{
					result.Description = "ConstantScoreQuery(" + Enclosing_Instance.internalFilter + "), product of:";
					result.Value = queryWeight;
					System.Boolean tempAux = true;
					result.Match = tempAux;
					result.AddDetail(new Explanation(Enclosing_Instance.Boost, "boost"));
					result.AddDetail(new Explanation(queryNorm, "queryNorm"));
				}
				else
				{
					result.Description = "ConstantScoreQuery(" + Enclosing_Instance.internalFilter + ") doesn't match id " + doc;
					result.Value = 0;
					System.Boolean tempAux2 = false;
					result.Match = tempAux2;
				}
				return result;
			}
		}
		
		protected internal class ConstantScorer : Scorer
		{
			private void  InitBlock(ConstantScoreQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private ConstantScoreQuery enclosingInstance;
			public ConstantScoreQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal DocIdSetIterator docIdSetIterator;
			internal float theScore;
			internal int doc = - 1;
			
			public ConstantScorer(ConstantScoreQuery enclosingInstance, Similarity similarity, IndexReader reader, Weight w, IState state) :base(similarity)
			{
				InitBlock(enclosingInstance);
				theScore = w.Value;
				DocIdSet docIdSet = Enclosing_Instance.internalFilter.GetDocIdSet(reader, state);
				if (docIdSet == null)
				{
					docIdSetIterator = DocIdSet.EMPTY_DOCIDSET.Iterator(state);
				}
				else
				{
					DocIdSetIterator iter = docIdSet.Iterator(state);
					if (iter == null)
					{
						docIdSetIterator = DocIdSet.EMPTY_DOCIDSET.Iterator(state);
					}
					else
					{
						docIdSetIterator = iter;
					}
				}
			}
			
			public override int NextDoc(IState state)
			{
				return docIdSetIterator.NextDoc(state);
			}
			
			public override int DocID()
			{
				return docIdSetIterator.DocID();
			}
			
			public override float Score(IState state)
			{
				return theScore;
			}
			
			public override int Advance(int target, IState state)
			{
				return docIdSetIterator.Advance(target, state);
			}
		}
		
		public override Weight CreateWeight(Searcher searcher, IState state)
		{
			return new ConstantScoreQuery.ConstantWeight(this, searcher);
		}
		
		/// <summary>Prints a user-readable version of this query. </summary>
		public override System.String ToString(string field)
		{
			return "ConstantScore(" + internalFilter + (Boost == 1.0?")":"^" + Boost);
		}
		
		/// <summary>Returns true if <c>o</c> is equal to this. </summary>
		public  override bool Equals(System.Object o)
		{
			if (this == o)
				return true;
			if (!(o is ConstantScoreQuery))
				return false;
			ConstantScoreQuery other = (ConstantScoreQuery) o;
			return this.Boost == other.Boost && internalFilter.Equals(other.internalFilter);
		}
		
		/// <summary>Returns a hash code value for this object. </summary>
		public override int GetHashCode()
		{
			// Simple add is OK since no existing filter hashcode has a float component.
			return internalFilter.GetHashCode() + BitConverter.ToInt32(BitConverter.GetBytes(Boost), 0);
        }

		override public System.Object Clone()
		{
            // {{Aroush-1.9}} is this all that we need to clone?!
            ConstantScoreQuery clone = (ConstantScoreQuery)base.Clone();
            clone.internalFilter = (Filter)this.internalFilter;
            return clone;
        }
	}
}