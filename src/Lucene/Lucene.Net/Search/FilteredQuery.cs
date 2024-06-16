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
using ToStringUtils = Lucene.Net.Util.ToStringUtils;

namespace Lucene.Net.Search
{


    /// <summary> A query that applies a filter to the results of another query.
    /// 
    /// <p/>Note: the bits are retrieved from the filter each time this
    /// query is used in a search - use a CachingWrapperFilter to avoid
    /// regenerating the bits every time.
    /// 
    /// <p/>Created: Apr 20, 2004 8:58:29 AM
    /// 
    /// </summary>
    /// <since>1.4</since>
    /// <seealso cref="CachingWrapperFilter"/>

        [Serializable]
    public class FilteredQuery:Query
	{

        [Serializable]
        private class AnonymousClassWeight:Weight
		{
			public AnonymousClassWeight(Lucene.Net.Search.Weight weight, Lucene.Net.Search.Similarity similarity, FilteredQuery enclosingInstance)
			{
				InitBlock(weight, similarity, enclosingInstance);
			}
			private class AnonymousClassScorer:Scorer
			{
				private void  InitBlock(Lucene.Net.Search.Scorer scorer, Lucene.Net.Search.DocIdSetIterator docIdSetIterator, AnonymousClassWeight enclosingInstance)
				{
					this.scorer = scorer;
					this.docIdSetIterator = docIdSetIterator;
					this.enclosingInstance = enclosingInstance;
				}
				private Lucene.Net.Search.Scorer scorer;
				private Lucene.Net.Search.DocIdSetIterator docIdSetIterator;
				private AnonymousClassWeight enclosingInstance;
				public AnonymousClassWeight Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				internal AnonymousClassScorer(Lucene.Net.Search.Scorer scorer, Lucene.Net.Search.DocIdSetIterator docIdSetIterator, AnonymousClassWeight enclosingInstance, Lucene.Net.Search.Similarity Param1):base(Param1)
				{
					InitBlock(scorer, docIdSetIterator, enclosingInstance);
				}
				
				private int doc = - 1;
				
				private int AdvanceToCommon(int scorerDoc, int disiDoc, IState state)
				{
					while (scorerDoc != disiDoc)
					{
						if (scorerDoc < disiDoc)
						{
							scorerDoc = scorer.Advance(disiDoc, state);
						}
						else
						{
							disiDoc = docIdSetIterator.Advance(scorerDoc, state);
						}
					}
					return scorerDoc;
				}
				
				public override int NextDoc(IState state)
				{
					int scorerDoc, disiDoc;
					return doc = (disiDoc = docIdSetIterator.NextDoc(state)) != NO_MORE_DOCS && (scorerDoc = scorer.NextDoc(state)) != NO_MORE_DOCS && AdvanceToCommon(scorerDoc, disiDoc, state) != NO_MORE_DOCS?scorer.DocID():NO_MORE_DOCS;
				}
				public override int DocID()
				{
					return doc;
				}
				
				public override int Advance(int target, IState state)
				{
					int disiDoc, scorerDoc;
					return doc = (disiDoc = docIdSetIterator.Advance(target, state)) != NO_MORE_DOCS && (scorerDoc = scorer.Advance(disiDoc, state)) != NO_MORE_DOCS && AdvanceToCommon(scorerDoc, disiDoc, state) != NO_MORE_DOCS?scorer.DocID():NO_MORE_DOCS;
				}
				
				public override float Score(IState state)
				{
					return Enclosing_Instance.Enclosing_Instance.Boost * scorer.Score(state);
				}
			}
			private void  InitBlock(Lucene.Net.Search.Weight weight, Lucene.Net.Search.Similarity similarity, FilteredQuery enclosingInstance)
			{
				this.weight = weight;
				this.similarity = similarity;
				this.enclosingInstance = enclosingInstance;
			}
			private Lucene.Net.Search.Weight weight;
			private Lucene.Net.Search.Similarity similarity;
			private FilteredQuery enclosingInstance;
			public FilteredQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private float value_Renamed;
			
			// pass these methods through to enclosed query's weight

		    public override float Value
		    {
		        get { return value_Renamed; }
		    }

		    public override float GetSumOfSquaredWeights()
		    {
		        return weight.GetSumOfSquaredWeights()*Enclosing_Instance.Boost*Enclosing_Instance.Boost;
		    }

		    public override void  Normalize(float v)
			{
				weight.Normalize(v);
				value_Renamed = weight.Value * Enclosing_Instance.Boost;
			}
			public override Explanation Explain(IndexReader ir, int i, IState state)
			{
				Explanation inner = weight.Explain(ir, i, state);
				if (Enclosing_Instance.Boost != 1)
				{
					Explanation preBoost = inner;
					inner = new Explanation(inner.Value * Enclosing_Instance.Boost, "product of:");
					inner.AddDetail(new Explanation(Enclosing_Instance.Boost, "boost"));
					inner.AddDetail(preBoost);
				}
				Filter f = Enclosing_Instance.filter;
				DocIdSet docIdSet = f.GetDocIdSet(ir, state);
				DocIdSetIterator docIdSetIterator = docIdSet == null?DocIdSet.EMPTY_DOCIDSET.Iterator(state):docIdSet.Iterator(state);
				if (docIdSetIterator == null)
				{
					docIdSetIterator = DocIdSet.EMPTY_DOCIDSET.Iterator(state);
				}
				if (docIdSetIterator.Advance(i, state) == i)
				{
					return inner;
				}
				else
				{
					Explanation result = new Explanation(0.0f, "failure to match filter: " + f.ToString());
					result.AddDetail(inner);
					return result;
				}
			}
			
			// return this query

		    public override Query Query
		    {
		        get { return Enclosing_Instance; }
		    }

		    // return a filtering scorer
			public override Scorer Scorer(IndexReader indexReader, bool scoreDocsInOrder, bool topScorer, IState state)
			{
				Scorer scorer = weight.Scorer(indexReader, true, false, state);
				if (scorer == null)
				{
					return null;
				}
				DocIdSet docIdSet = Enclosing_Instance.filter.GetDocIdSet(indexReader, state);
				if (docIdSet == null)
				{
					return null;
				}
				DocIdSetIterator docIdSetIterator = docIdSet.Iterator(state);
				if (docIdSetIterator == null)
				{
					return null;
				}
				
				return new AnonymousClassScorer(scorer, docIdSetIterator, this, similarity);
			}
		}
		
		internal Query query;
		internal Filter filter;
		
		/// <summary> Constructs a new query which applies a filter to the results of the original query.
		/// Filter.getDocIdSet() will be called every time this query is used in a search.
		/// </summary>
		/// <param name="query"> Query to be filtered, cannot be <c>null</c>.
		/// </param>
		/// <param name="filter">Filter to apply to query results, cannot be <c>null</c>.
		/// </param>
		public FilteredQuery(Query query, Filter filter)
		{
			this.query = query;
			this.filter = filter;
		}
		
		/// <summary> Returns a Weight that applies the filter to the enclosed query's Weight.
		/// This is accomplished by overriding the Scorer returned by the Weight.
		/// </summary>
		public override Weight CreateWeight(Searcher searcher, IState state)
		{
			Weight weight = query.CreateWeight(searcher, state);
			Similarity similarity = query.GetSimilarity(searcher);
			return new AnonymousClassWeight(weight, similarity, this);
		}
		
		/// <summary>Rewrites the wrapped query. </summary>
		public override Query Rewrite(IndexReader reader, IState state)
		{
			Query rewritten = query.Rewrite(reader, state);
			if (rewritten != query)
			{
				FilteredQuery clone = (FilteredQuery) this.Clone();
				clone.query = rewritten;
				return clone;
			}
			else
			{
				return this;
			}
		}

	    public virtual Query Query
	    {
	        get { return query; }
	    }

	    public virtual Filter Filter
	    {
	        get { return filter; }
	    }

	    // inherit javadoc
		public override void  ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
			Query.ExtractTerms(terms);
		}
		
		/// <summary>Prints a user-readable version of this query. </summary>
		public override System.String ToString(System.String s)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			buffer.Append("filtered(");
			buffer.Append(query.ToString(s));
			buffer.Append(")->");
			buffer.Append(filter);
			buffer.Append(ToStringUtils.Boost(Boost));
			return buffer.ToString();
		}
		
		/// <summary>Returns true iff <c>o</c> is equal to this. </summary>
		public  override bool Equals(System.Object o)
		{
			if (o is FilteredQuery)
			{
				FilteredQuery fq = (FilteredQuery) o;
				return (query.Equals(fq.query) && filter.Equals(fq.filter) && Boost == fq.Boost);
			}
			return false;
		}
		
		/// <summary>Returns a hash code value for this object. </summary>
		public override int GetHashCode()
		{
			return query.GetHashCode() ^ filter.GetHashCode() + System.Convert.ToInt32(Boost);
		}
	}
}