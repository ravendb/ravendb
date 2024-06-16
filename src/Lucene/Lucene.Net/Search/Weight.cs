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

namespace Lucene.Net.Search
{

    /// <summary> Expert: Calculate query weights and build query scorers.
    /// <p/>
    /// The purpose of <see cref="Weight" /> is to ensure searching does not
    /// modify a <see cref="Query" />, so that a <see cref="Query" /> instance can be reused. <br/>
    /// <see cref="Searcher" /> dependent state of the query should reside in the
    /// <see cref="Weight" />. <br/>
    /// <see cref="IndexReader" /> dependent state should reside in the <see cref="Scorer" />.
    /// <p/>
    /// A <c>Weight</c> is used in the following way:
    /// <list type="bullet">
    /// <item>A <c>Weight</c> is constructed by a top-level query, given a
    /// <c>Searcher</c> (<see cref="Lucene.Net.Search.Query.CreateWeight(Searcher)" />).</item>
    /// <item>The <see cref="GetSumOfSquaredWeights()" /> method is called on the
    /// <c>Weight</c> to compute the query normalization factor
    /// <see cref="Similarity.QueryNorm(float)" /> of the query clauses contained in the
    /// query.</item>
    /// <item>The query normalization factor is passed to <see cref="Normalize(float)" />. At
    /// this point the weighting is complete.</item>
    /// <item>A <c>Scorer</c> is constructed by <see cref="Scorer(IndexReader,bool,bool)" />.</item>
    /// </list>
    /// 
    /// </summary>
    /// <since> 2.9
    /// </since>

        [Serializable]
    public abstract class Weight
	{
		
		/// <summary> An explanation of the score computation for the named document.
		/// 
		/// </summary>
		/// <param name="reader">sub-reader containing the give doc
		/// </param>
		/// <param name="doc">
		/// </param>
		/// <returns> an Explanation for the score
		/// </returns>
		/// <throws>  IOException </throws>
		public abstract Explanation Explain(IndexReader reader, int doc, IState state);

	    /// <summary>The query that this concerns. </summary>
	    public abstract Query Query { get; }

	    /// <summary>The weight for this query. </summary>
	    public abstract float Value { get; }

	    /// <summary>Assigns the query normalization factor to this. </summary>
		public abstract void  Normalize(float norm);
		
		/// <summary> Returns a <see cref="Scorer" /> which scores documents in/out-of order according
		/// to <c>scoreDocsInOrder</c>.
		/// <p/>
		/// <b>NOTE:</b> even if <c>scoreDocsInOrder</c> is false, it is
		/// recommended to check whether the returned <c>Scorer</c> indeed scores
		/// documents out of order (i.e., call <see cref="GetScoresDocsOutOfOrder()" />), as
		/// some <c>Scorer</c> implementations will always return documents
		/// in-order.<br/>
		/// <b>NOTE:</b> null can be returned if no documents will be scored by this
		/// query.
		/// 
		/// </summary>
		/// <param name="reader">
        /// the <see cref="IndexReader" /> for which to return the <see cref="Lucene.Net.Search.Scorer" />.
		/// </param>
		/// <param name="scoreDocsInOrder">specifies whether in-order scoring of documents is required. Note
		/// that if set to false (i.e., out-of-order scoring is required),
		/// this method can return whatever scoring mode it supports, as every
		/// in-order scorer is also an out-of-order one. However, an
        /// out-of-order scorer may not support <see cref="DocIdSetIterator.NextDoc" />
        /// and/or <see cref="DocIdSetIterator.Advance(int)" />, therefore it is recommended to
		/// request an in-order scorer if use of these methods is required.
		/// </param>
		/// <param name="topScorer">
        /// if true, <see cref="Lucene.Net.Search.Scorer.Score(Lucene.Net.Search.Collector)" /> will be called; if false,
        /// <see cref="DocIdSetIterator.NextDoc" /> and/or <see cref="DocIdSetIterator.Advance(int)" /> will
		/// be called.
		/// </param>
		/// <returns> a <see cref="Scorer" /> which scores documents in/out-of order.
		/// </returns>
		/// <throws>  IOException </throws>
		public abstract Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state);

	    /// <summary>The sum of squared weights of contained query clauses. </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public abstract float GetSumOfSquaredWeights();

	    /// <summary> Returns true iff this implementation scores docs only out of order. This
	    /// method is used in conjunction with <see cref="Collector" />'s 
	    /// <see cref="Collector.AcceptsDocsOutOfOrder()">AcceptsDocsOutOfOrder</see> and
	    /// <see cref="Scorer(Lucene.Net.Index.IndexReader, bool, bool)" /> to
	    /// create a matching <see cref="Scorer" /> instance for a given <see cref="Collector" />, or
	    /// vice versa.
	    /// <p/>
	    /// <b>NOTE:</b> the default implementation returns <c>false</c>, i.e.
	    /// the <c>Scorer</c> scores documents in-order.
	    /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public virtual bool GetScoresDocsOutOfOrder()
	    {
	        return false;
	    }
	}
}