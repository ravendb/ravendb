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
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Store;
using ScorerDocQueue = Lucene.Net.Util.ScorerDocQueue;

namespace Lucene.Net.Search
{
	
	/// <summary>A Scorer for OR like queries, counterpart of <c>ConjunctionScorer</c>.
	/// This Scorer implements <see cref="DocIdSetIterator.Advance(int)" /> and uses skipTo() on the given Scorers.
	/// </summary>
	class DisjunctionSumScorer:Scorer
	{
		/// <summary>The number of subscorers. </summary>
		private int nrScorers;
		
		/// <summary>The subscorers. </summary>
		protected internal System.Collections.Generic.IList<Scorer> subScorers;
		
		/// <summary>The minimum number of scorers that should match. </summary>
		private int minimumNrMatchers;
		
		/// <summary>The scorerDocQueue contains all subscorers ordered by their current doc(),
		/// with the minimum at the top.
		/// <br/>The scorerDocQueue is initialized the first time next() or skipTo() is called.
		/// <br/>An exhausted scorer is immediately removed from the scorerDocQueue.
		/// <br/>If less than the minimumNrMatchers scorers
		/// remain in the scorerDocQueue next() and skipTo() return false.
		/// <p/>
		/// After each to call to next() or skipTo()
		/// <c>currentSumScore</c> is the total score of the current matching doc,
		/// <c>nrMatchers</c> is the number of matching scorers,
		/// and all scorers are after the matching doc, or are exhausted.
		/// </summary>
		private ScorerDocQueue scorerDocQueue;
		
		/// <summary>The document number of the current match. </summary>
		private int currentDoc = - 1;
		
		/// <summary>The number of subscorers that provide the current match. </summary>
		protected internal int nrMatchers = - 1;
		
		private float currentScore = System.Single.NaN;
		
		/// <summary>Construct a <c>DisjunctionScorer</c>.</summary>
		/// <param name="subScorers">A collection of at least two subscorers.
		/// </param>
		/// <param name="minimumNrMatchers">The positive minimum number of subscorers that should
		/// match to match this query.
		/// <br/>When <c>minimumNrMatchers</c> is bigger than
		/// the number of <c>subScorers</c>,
		/// no matches will be produced.
		/// <br/>When minimumNrMatchers equals the number of subScorers,
		/// it more efficient to use <c>ConjunctionScorer</c>.
		/// </param>
		public DisjunctionSumScorer(System.Collections.Generic.IList<Scorer> subScorers, int minimumNrMatchers, IState state) :base(null)
		{
			
			nrScorers = subScorers.Count;
			
			if (minimumNrMatchers <= 0)
			{
				throw new System.ArgumentException("Minimum nr of matchers must be positive");
			}
			if (nrScorers <= 1)
			{
				throw new System.ArgumentException("There must be at least 2 subScorers");
			}
			
			this.minimumNrMatchers = minimumNrMatchers;
			this.subScorers = subScorers;
			
			InitScorerDocQueue(state);
		}
		
		/// <summary>Construct a <c>DisjunctionScorer</c>, using one as the minimum number
		/// of matching subscorers.
		/// </summary>
        public DisjunctionSumScorer(System.Collections.Generic.IList<Scorer> subScorers, IState state)
            : this(subScorers, 1, state)
		{
		}
		
		/// <summary>Called the first time next() or skipTo() is called to
		/// initialize <c>scorerDocQueue</c>.
		/// </summary>
		private void  InitScorerDocQueue(IState state)
		{
			scorerDocQueue = new ScorerDocQueue(nrScorers);
			foreach(Scorer se in subScorers)
			{
				if (se.NextDoc(state) != NO_MORE_DOCS)
				{
					// doc() method will be used in scorerDocQueue.
					scorerDocQueue.Insert(se);
				}
			}
		}
		
		/// <summary>Scores and collects all matching documents.</summary>
		/// <param name="collector">The collector to which all matching documents are passed through.</param>
		public override void  Score(Collector collector, IState state)
		{
			collector.SetScorer(this);
			while (NextDoc(state) != NO_MORE_DOCS)
			{
				collector.Collect(currentDoc, state);
			}
		}

	    /// <summary>Expert: Collects matching documents in a range.  Hook for optimization.
	    /// Note that <see cref="NextDoc()" /> must be called once before this method is called
	    /// for the first time.
	    /// </summary>
	    /// <param name="collector">The collector to which all matching documents are passed through.
	    /// </param>
	    /// <param name="max">Do not score documents past this.
	    /// </param>
	    /// <param name="firstDocID"></param>
	    /// <returns> true if more matching documents may remain.
	    /// </returns>
	    public /*protected internal*/ override bool Score(Collector collector, int max, int firstDocID, IState state)
		{
			// firstDocID is ignored since nextDoc() sets 'currentDoc'
			collector.SetScorer(this);
			while (currentDoc < max)
			{
				collector.Collect(currentDoc, state);
				if (NextDoc(state) == NO_MORE_DOCS)
				{
					return false;
				}
			}
			return true;
		}
		
		public override int NextDoc(IState state)
		{
			if (scorerDocQueue.Size() < minimumNrMatchers || !AdvanceAfterCurrent(state))
			{
				currentDoc = NO_MORE_DOCS;
			}
			return currentDoc;
		}

		/// <summary>Advance all subscorers after the current document determined by the
		/// top of the <c>scorerDocQueue</c>.
		/// Repeat until at least the minimum number of subscorers match on the same
		/// document and all subscorers are after that document or are exhausted.
		/// <br/>On entry the <c>scorerDocQueue</c> has at least <c>minimumNrMatchers</c>
		/// available. At least the scorer with the minimum document number will be advanced.
		/// </summary>
		/// <returns> true iff there is a match.
		/// <br/>In case there is a match, <c>currentDoc</c>, <c>currentSumScore</c>,
		/// and <c>nrMatchers</c> describe the match.
		/// 
		/// TODO: Investigate whether it is possible to use skipTo() when
		/// the minimum number of matchers is bigger than one, ie. try and use the
		/// character of ConjunctionScorer for the minimum number of matchers.
		/// Also delay calling score() on the sub scorers until the minimum number of
		/// matchers is reached.
		/// <br/>For this, a Scorer array with minimumNrMatchers elements might
		/// hold Scorers at currentDoc that are temporarily popped from scorerQueue.
		/// </returns>

		protected internal virtual bool AdvanceAfterCurrent(IState state)
		{
			do 
			{
				// repeat until minimum nr of matchers
				currentDoc = scorerDocQueue.TopDoc();
                var buffer = ArrayPool<float>.Shared.Rent(scorerDocQueue.MaxSize);
                buffer[0] = scorerDocQueue.TopScore(state);

				nrMatchers = 1;
				do 
				{
					// Until all subscorers are after currentDoc
					if (!scorerDocQueue.TopNextAndAdjustElsePop(state))
					{
						if (scorerDocQueue.Size() == 0)
						{
							break; // nothing more to advance, check for last match.
						}
					}
					if (scorerDocQueue.TopDoc() != currentDoc)
					{
						break; // All remaining subscorers are after currentDoc.
					}

                    buffer[nrMatchers++] = scorerDocQueue.TopScore(state);
                }
				while (true);

                Array.Sort(buffer, 0, nrMatchers);
                currentScore = 0;
                for (int i = 0; i < nrMatchers; i++)
                {
                    currentScore += buffer[i];
                }

                ArrayPool<float>.Shared.Return(buffer);

				if (nrMatchers >= minimumNrMatchers)
				{
					return true;
				}
				else if (scorerDocQueue.Size() < minimumNrMatchers)
				{
					return false;
				}
			}
			while (true);
		}
		
		/// <summary>Returns the score of the current document matching the query.
		/// Initially invalid, until <see cref="NextDoc()" /> is called the first time.
		/// </summary>
		public override float Score(IState state)
		{
			return currentScore;
		}
		
		public override int DocID()
		{
			return currentDoc;
		}
		
		/// <summary>Returns the number of subscorers matching the current document.
		/// Initially invalid, until <see cref="NextDoc()" /> is called the first time.
		/// </summary>
		public virtual int NrMatchers()
		{
			return nrMatchers;
		}
		
		/// <summary> Advances to the first match beyond the current whose document number is
		/// greater than or equal to a given target. <br/>
		/// The implementation uses the skipTo() method on the subscorers.
		/// 
		/// </summary>
		/// <param name="target">The target document number.
		/// </param>
		/// <returns> the document whose number is greater than or equal to the given
		/// target, or -1 if none exist.
		/// </returns>
		public override int Advance(int target, IState state)
		{
			if (scorerDocQueue.Size() < minimumNrMatchers)
			{
				return currentDoc = NO_MORE_DOCS;
			}
			if (target <= currentDoc)
			{
				return currentDoc;
			}
			do 
			{
				if (scorerDocQueue.TopDoc() >= target)
				{
					return AdvanceAfterCurrent(state)?currentDoc:(currentDoc = NO_MORE_DOCS);
				}
				else if (!scorerDocQueue.TopSkipToAndAdjustElsePop(target, state))
				{
					if (scorerDocQueue.Size() < minimumNrMatchers)
					{
						return currentDoc = NO_MORE_DOCS;
					}
				}
			}
			while (true);
		}
	}
}