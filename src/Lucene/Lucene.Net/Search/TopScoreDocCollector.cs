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
	
	/// <summary> A <see cref="Collector" /> implementation that collects the top-scoring hits,
	/// returning them as a <see cref="TopDocs" />. This is used by <see cref="IndexSearcher" /> to
	/// implement <see cref="TopDocs" />-based search. Hits are sorted by score descending
	/// and then (when the scores are tied) docID ascending. When you create an
	/// instance of this collector you should know in advance whether documents are
	/// going to be collected in doc Id order or not.
	/// 
	/// <p/><b>NOTE</b>: The values <see cref="float.NaN" /> and
    /// <see cref="float.NegativeInfinity" /> are not valid scores.  This
    /// collector will not properly collect hits with such
    /// scores.
	/// </summary>
	public abstract class TopScoreDocCollector : TopDocsCollector<ScoreDoc>
	{
		
		// Assumes docs are scored in order.
		private class InOrderTopScoreDocCollector:TopScoreDocCollector
		{
			internal InOrderTopScoreDocCollector(int numHits):base(numHits)
			{
			}
			
			public override void  Collect(int doc, IState state)
			{
				float score = scorer.Score(state);
                
                // This collector cannot handle these scores:
                System.Diagnostics.Debug.Assert(score != float.NegativeInfinity);
                System.Diagnostics.Debug.Assert(!float.IsNaN(score));

				internalTotalHits++;
				if (score <= pqTop.Score)
				{
					// Since docs are returned in-order (i.e., increasing doc Id), a document
					// with equal score to pqTop.score cannot compete since HitQueue favors
					// documents with lower doc Ids. Therefore reject those docs too.
					return ;
				}
				pqTop.Doc = doc + docBase;
				pqTop.Score = score;
				pqTop = pq.UpdateTop();
			}

		    public override bool AcceptsDocsOutOfOrder
		    {
		        get { return false; }
		    }
		}
		
		// Assumes docs are scored out of order.
		private class OutOfOrderTopScoreDocCollector:TopScoreDocCollector
		{
			internal OutOfOrderTopScoreDocCollector(int numHits):base(numHits)
			{
			}
			
			public override void  Collect(int doc, IState state)
			{
				float score = scorer.Score(state);

                // This collector cannot handle NaN
                System.Diagnostics.Debug.Assert(!float.IsNaN(score));

				internalTotalHits++;
				doc += docBase;
				if (score < pqTop.Score || (score == pqTop.Score && doc > pqTop.Doc))
				{
					return ;
				}
				pqTop.Doc = doc;
				pqTop.Score = score;
				pqTop = pq.UpdateTop();
			}

		    public override bool AcceptsDocsOutOfOrder
		    {
		        get { return true; }
		    }
		}
		
		/// <summary> Creates a new <see cref="TopScoreDocCollector" /> given the number of hits to
		/// collect and whether documents are scored in order by the input
		/// <see cref="Scorer" /> to <see cref="SetScorer(Scorer)" />.
		/// 
		/// <p/><b>NOTE</b>: The instances returned by this method
		/// pre-allocate a full array of length
		/// <c>numHits</c>, and fill the array with sentinel
		/// objects.
		/// </summary>
		public static TopScoreDocCollector Create(int numHits, bool docsScoredInOrder)
		{
			
			if (docsScoredInOrder)
			{
				return new InOrderTopScoreDocCollector(numHits);
			}
			else
			{
				return new OutOfOrderTopScoreDocCollector(numHits);
			}
		}
		
		internal ScoreDoc pqTop;
		internal int docBase = 0;
		internal Scorer scorer;
		
		// prevents instantiation
		private TopScoreDocCollector(int numHits):base(new HitQueue(numHits, true))
		{
			// HitQueue implements getSentinelObject to return a ScoreDoc, so we know
			// that at this point top() is already initialized.
			pqTop = pq.Top();
		}
		
		public /*protected internal*/ override TopDocs NewTopDocs(ScoreDoc[] results, int start)
		{
			if (results == null)
			{
				return EMPTY_TOPDOCS;
			}
			
			// We need to compute maxScore in order to set it in TopDocs. If start == 0,
			// it means the largest element is already in results, use its score as
			// maxScore. Otherwise pop everything else, until the largest element is
			// extracted and use its score as maxScore.
			float maxScore = System.Single.NaN;
			if (start == 0)
			{
				maxScore = results[0].Score;
			}
			else
			{
				for (int i = pq.Size(); i > 1; i--)
				{
					pq.Pop();
				}
				maxScore = pq.Pop().Score;
			}
			
			return new TopDocs(internalTotalHits, results, maxScore);
		}
		
		public override void SetNextReader(IndexReader reader, int base_Renamed, IState state)
		{
			docBase = base_Renamed;
		}
		
		public override void SetScorer(Scorer scorer)
		{
			this.scorer = scorer;
		}
	}
}