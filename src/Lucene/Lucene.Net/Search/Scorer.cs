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

namespace Lucene.Net.Search
{
	
	/// <summary> Expert: Common scoring functionality for different types of queries.
	/// 
	/// <p/>
	/// A <c>Scorer</c> iterates over documents matching a
	/// query in increasing order of doc Id.
	/// <p/>
	/// <p/>
	/// Document scores are computed using a given <c>Similarity</c>
	/// implementation.
	/// <p/>
	/// 
	/// <p/><b>NOTE</b>: The values Float.Nan,
	/// Float.NEGATIVE_INFINITY and Float.POSITIVE_INFINITY are
	/// not valid scores.  Certain collectors (eg <see cref="TopScoreDocCollector" />
	///) will not properly collect hits
	/// with these scores.
	/// </summary>
	public abstract class Scorer:DocIdSetIterator
	{
		private Similarity similarity;
		
		/// <summary>Constructs a Scorer.</summary>
		/// <param name="similarity">The <c>Similarity</c> implementation used by this scorer.
		/// </param>
		protected internal Scorer(Similarity similarity)
		{
			this.similarity = similarity;
		}

	    /// <summary>Returns the Similarity implementation used by this scorer. </summary>
	    public virtual Similarity Similarity
	    {
	        get { return this.similarity; }
	    }

	    /// <summary>Scores and collects all matching documents.</summary>
		/// <param name="collector">The collector to which all matching documents are passed.
		/// </param>
		public virtual void  Score(Collector collector, IState state)
		{
			collector.SetScorer(this);
			int doc;
			while ((doc = NextDoc(state)) != NO_MORE_DOCS)
			{
				collector.Collect(doc, state);
			}
		}
		
		/// <summary> Expert: Collects matching documents in a range. Hook for optimization.
		/// Note, <paramref name="firstDocID" /> is added to ensure that <see cref="DocIdSetIterator.NextDoc()" />
		/// was called before this method.
		/// 
		/// </summary>
		/// <param name="collector">The collector to which all matching documents are passed.
		/// </param>
		/// <param name="max">Do not score documents past this.
		/// </param>
		/// <param name="firstDocID">
        /// The first document ID (ensures <see cref="DocIdSetIterator.NextDoc()" /> is called before
		/// this method.
		/// </param>
		/// <returns> true if more matching documents may remain.
		/// </returns>
		public /*protected internal*/ virtual bool Score(Collector collector, int max, int firstDocID, IState state)
		{
			collector.SetScorer(this);
			int doc = firstDocID;
			while (doc < max)
			{
				collector.Collect(doc, state);
				doc = NextDoc(state);
			}
			return doc != NO_MORE_DOCS;
		}
		
		/// <summary>Returns the score of the current document matching the query.
        /// Initially invalid, until <see cref="DocIdSetIterator.NextDoc()" /> or <see cref="DocIdSetIterator.Advance(int)" />
		/// is called the first time, or when called from within
		/// <see cref="Collector.Collect(int)" />.
		/// </summary>
		public abstract float Score(IState state);
	}
}