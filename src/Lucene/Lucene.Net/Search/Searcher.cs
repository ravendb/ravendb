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
using Lucene.Net.Documents;
using Lucene.Net.Store;
using Document = Lucene.Net.Documents.Document;
using CorruptIndexException = Lucene.Net.Index.CorruptIndexException;
using Term = Lucene.Net.Index.Term;

namespace Lucene.Net.Search
{
	
	/// <summary> An abstract base class for search implementations. Implements the main search
	/// methods.
	/// 
	/// <p/>
	/// Note that you can only access hits from a Searcher as long as it is not yet
	/// closed, otherwise an IOException will be thrown.
	/// </summary>
	public abstract class Searcher :
        System.MarshalByRefObject, 
        Searchable, System.IDisposable
	{
	    protected Searcher()
		{
			InitBlock();
		}
		private void  InitBlock()
		{
			similarity = Net.Search.Similarity.Default;
		}
		
		/// <summary>Search implementation with arbitrary sorting.  Finds
		/// the top <c>n</c> hits for <c>query</c>, applying
		/// <c>filter</c> if non-null, and sorting the hits by the criteria in
		/// <c>sort</c>.
		/// 
		/// <p/>NOTE: this does not compute scores by default; use
		/// <see cref="IndexSearcher.SetDefaultFieldSortScoring(bool,bool)" /> to enable scoring.
		/// 
		/// </summary>
		/// <throws>  BooleanQuery.TooManyClauses </throws>
		public virtual TopFieldDocs Search(Query query, Filter filter, int n, Sort sort, IState state)
		{
			return Search(CreateWeight(query, state), filter, n, sort, state);
		}
		
		/// <summary>Lower-level search API.
		/// 
		/// <p/><see cref="Collector.Collect(int)" /> is called for every matching document.
		/// 
		/// <p/>Applications should only use this if they need <i>all</i> of the matching
		/// documents. The high-level search API (<see cref="Searcher.Search(Query, int)" />
		/// ) is usually more efficient, as it skips non-high-scoring hits.
		/// <p/>Note: The <c>score</c> passed to this method is a raw score.
		/// In other words, the score will not necessarily be a float whose value is
		/// between 0 and 1.
		/// </summary>
		/// <throws>  BooleanQuery.TooManyClauses </throws>
		public virtual void  Search(Query query, Collector results, IState state)
		{
			Search(CreateWeight(query, state), null, results, state);
		}
		
		/// <summary>Lower-level search API.
		/// 
		/// <p/><see cref="Collector.Collect(int)" /> is called for every matching
		/// document.
		/// <br/>Collector-based access to remote indexes is discouraged.
		/// 
		/// <p/>Applications should only use this if they need <i>all</i> of the
		/// matching documents.  The high-level search API (<see cref="Searcher.Search(Query, Filter, int)" />)
		/// is usually more efficient, as it skips
		/// non-high-scoring hits.
		/// 
		/// </summary>
		/// <param name="query">to match documents
		/// </param>
		/// <param name="filter">if non-null, used to permit documents to be collected.
		/// </param>
		/// <param name="results">to receive hits
		/// </param>
		/// <throws>  BooleanQuery.TooManyClauses </throws>
		public virtual void  Search(Query query, Filter filter, Collector results, IState state)
		{
			Search(CreateWeight(query, state), filter, results, state);
		}
		
		/// <summary>Finds the top <c>n</c>
		/// hits for <c>query</c>, applying <c>filter</c> if non-null.
		/// 
		/// </summary>
		/// <throws>  BooleanQuery.TooManyClauses </throws>
		public virtual TopDocs Search(Query query, Filter filter, int n, IState state)
		{
			return Search(CreateWeight(query, state), filter, n, state);
		}
		
		/// <summary>Finds the top <c>n</c>
		/// hits for <c>query</c>.
		/// 
		/// </summary>
		/// <throws>  BooleanQuery.TooManyClauses </throws>
		public virtual TopDocs Search(Query query, int n, IState state)
		{
			return Search(query, null, n, state);
		}
		
		/// <summary>Returns an Explanation that describes how <c>doc</c> scored against
		/// <c>query</c>.
		/// 
		/// <p/>This is intended to be used in developing Similarity implementations,
		/// and, for good performance, should not be displayed with every hit.
		/// Computing an explanation is as expensive as executing the query over the
		/// entire index.
		/// </summary>
		public virtual Explanation Explain(Query query, int doc, IState state)
		{
			return Explain(CreateWeight(query, state), doc, state);
		}
		
		/// <summary>The Similarity implementation used by this searcher. </summary>
		private Similarity similarity;

	    /// <summary>Expert: Gets or Sets the Similarity implementation used by this Searcher.
	    /// 
	    /// </summary>
	    /// <seealso cref="Lucene.Net.Search.Similarity.Default">
	    /// </seealso>
	    public virtual Similarity Similarity
        {
            get
            {
                if ( usingLightWeightSimilarity)
                {
                    return LightWeightSimilarity.Instance;
                }

                return similarity;
            }
            set => similarity = value;
        }

        /// <summary>
        /// ThreadStatic flag to indicate if we want to disable idfExplanation
        /// This is allow two concurrent calls to the searcher with/without light weight similarity.
        /// This also enables/disables the usage for new Searcher instances that are been generated during 'search' operation e.g QueryWrapperFilter.GetDocIdSet. 
        /// </summary>
        [ThreadStatic]
        protected static bool usingLightWeightSimilarity;

        /// <summary>
        /// Enable the light weight similarity feature
        /// </summary>
        /// <returns>A disposable object that disable the light weight feature on dispose</returns>
        public static LightWeightSimilarityScope EnableLightWeightSimilarity()
        {
            return new LightWeightSimilarityScope(enableLightWeightSimilarity:true);
        }

        /// <summary>
        /// A thread local scope that will enable us to use a lighter version of the similarity object
        /// </summary>
        public struct LightWeightSimilarityScope : IDisposable
        {
            public LightWeightSimilarityScope(bool enableLightWeightSimilarity)
            {
                usingLightWeightSimilarity = enableLightWeightSimilarity;
            }

            public void Dispose()
            {
                usingLightWeightSimilarity = false;
            }
        }

        /// <summary> creates a weight for <c>query</c></summary>
        /// <returns> new weight
        /// </returns>
        public /*protected internal*/ virtual Weight CreateWeight(Query query, IState state)
		{
			return query.Weight(this, state);
		}
		
		// inherit javadoc
		public virtual int[] DocFreqs(Term[] terms, IState state)
		{
			int[] result = new int[terms.Length];
			for (int i = 0; i < terms.Length; i++)
			{
				result[i] = DocFreq(terms[i], state);
			}
			return result;
		}

		public abstract void  Search(Weight weight, Filter filter, Collector results, IState state);

        [Obsolete("Use Dispose() instead")]
		public void Close()
        {
            Dispose();
        }

        public void Dispose()
        {
            Dispose(true);
        }

	    protected abstract void Dispose(bool disposing);

		public abstract int DocFreq(Term term, IState state);
	    public abstract int MaxDoc { get; }
		public abstract TopDocs Search(Weight weight, Filter filter, int n, IState state);
		public abstract Document Doc(int i, IState state);
	    public abstract Document Doc(int docid, FieldSelector fieldSelector, IState state);
		public abstract Query Rewrite(Query query, IState state);
		public abstract Explanation Explain(Weight weight, int doc, IState state);
		public abstract TopFieldDocs Search(Weight weight, Filter filter, int n, Sort sort, IState state);
		/* End patch for GCJ bug #15411. */
	}
}