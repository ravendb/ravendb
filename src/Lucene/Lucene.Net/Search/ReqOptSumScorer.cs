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

namespace Lucene.Net.Search
{
	
	/// <summary>A Scorer for queries with a required part and an optional part.
	/// Delays skipTo() on the optional part until a score() is needed.
	/// <br/>
	/// This <c>Scorer</c> implements <see cref="DocIdSetIterator.Advance(int)" />.
	/// </summary>
	class ReqOptSumScorer:Scorer
	{
		/// <summary>The scorers passed from the constructor.
		/// These are set to null as soon as their next() or skipTo() returns false.
		/// </summary>
		private Scorer reqScorer;
		private Scorer optScorer;
		
		/// <summary>Construct a <c>ReqOptScorer</c>.</summary>
		/// <param name="reqScorer">The required scorer. This must match.
		/// </param>
		/// <param name="optScorer">The optional scorer. This is used for scoring only.
		/// </param>
		public ReqOptSumScorer(Scorer reqScorer, Scorer optScorer):base(null)
		{ // No similarity used.
			this.reqScorer = reqScorer;
			this.optScorer = optScorer;
		}
		
		public override int NextDoc(IState state)
		{
			return reqScorer.NextDoc(state);
		}
		
		public override int Advance(int target, IState state)
		{
			return reqScorer.Advance(target, state);
		}
		
		public override int DocID()
		{
			return reqScorer.DocID();
		}
		
		/// <summary>Returns the score of the current document matching the query.
		/// Initially invalid, until <see cref="NextDoc()" /> is called the first time.
		/// </summary>
		/// <returns> The score of the required scorer, eventually increased by the score
		/// of the optional scorer when it also matches the current document.
		/// </returns>
		public override float Score(IState state)
		{
			int curDoc = reqScorer.DocID();
			float reqScore = reqScorer.Score(state);
			if (optScorer == null)
			{
				return reqScore;
			}
			
			int optScorerDoc = optScorer.DocID();
			if (optScorerDoc < curDoc && (optScorerDoc = optScorer.Advance(curDoc, state)) == NO_MORE_DOCS)
			{
				optScorer = null;
				return reqScore;
			}
			
			return optScorerDoc == curDoc?reqScore + optScorer.Score(state):reqScore;
		}
	}
}