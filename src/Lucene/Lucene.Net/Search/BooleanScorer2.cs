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
	
	/* See the description in BooleanScorer.java, comparing
	* BooleanScorer & BooleanScorer2 */
	
	/// <summary>An alternative to BooleanScorer that also allows a minimum number
	/// of optional scorers that should match.
	/// <br/>Implements skipTo(), and has no limitations on the numbers of added scorers.
	/// <br/>Uses ConjunctionScorer, DisjunctionScorer, ReqOptScorer and ReqExclScorer.
	/// </summary>
	class BooleanScorer2 : Scorer
	{
		private class AnonymousClassDisjunctionSumScorer:DisjunctionSumScorer
		{
			private void  InitBlock(BooleanScorer2 enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private BooleanScorer2 enclosingInstance;
			public BooleanScorer2 Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassDisjunctionSumScorer(BooleanScorer2 enclosingInstance, System.Collections.Generic.IList<Scorer> scorers, int minNrShouldMatch, IState state)
                : base(scorers, minNrShouldMatch, state)
			{
				InitBlock(enclosingInstance);
			}
			private int lastScoredDoc = - 1;
			// Save the score of lastScoredDoc, so that we don't compute it more than
			// once in score().
			private float lastDocScore = System.Single.NaN;
			public override float Score(IState state)
			{
				int doc = DocID();
				if (doc >= lastScoredDoc)
				{
					if (doc > lastScoredDoc)
					{
						lastDocScore = base.Score(state);
						lastScoredDoc = doc;
					}
					Enclosing_Instance.coordinator.nrMatchers += base.nrMatchers;
				}
				return lastDocScore;
			}
		}
		private class AnonymousClassConjunctionScorer:ConjunctionScorer
		{
			private void  InitBlock(int requiredNrMatchers, BooleanScorer2 enclosingInstance)
			{
				this.requiredNrMatchers = requiredNrMatchers;
				this.enclosingInstance = enclosingInstance;
			}
			private int requiredNrMatchers;
			private BooleanScorer2 enclosingInstance;
			public BooleanScorer2 Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassConjunctionScorer(int requiredNrMatchers, BooleanScorer2 enclosingInstance, Lucene.Net.Search.Similarity defaultSimilarity, IState state, System.Collections.Generic.IList<Scorer> requiredScorers)
                : base(defaultSimilarity, state, requiredScorers)
			{
				InitBlock(requiredNrMatchers, enclosingInstance);
			}
			private int lastScoredDoc = - 1;
			// Save the score of lastScoredDoc, so that we don't compute it more than
			// once in score().
			private float lastDocScore = System.Single.NaN;
			public override float Score(IState state)
			{
				int doc = DocID();
				if (doc >= lastScoredDoc)
				{
					if (doc > lastScoredDoc)
					{
						lastDocScore = base.Score(state);
						lastScoredDoc = doc;
					}
					Enclosing_Instance.coordinator.nrMatchers += requiredNrMatchers;
				}
				// All scorers match, so defaultSimilarity super.score() always has 1 as
				// the coordination factor.
				// Therefore the sum of the scores of the requiredScorers
				// is used as score.
				return lastDocScore;
			}
		}

        private System.Collections.Generic.List<Scorer> requiredScorers;
        private System.Collections.Generic.List<Scorer> optionalScorers;
        private System.Collections.Generic.List<Scorer> prohibitedScorers;
		
		private class Coordinator
		{
			public Coordinator(BooleanScorer2 enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(BooleanScorer2 enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private BooleanScorer2 enclosingInstance;
			public BooleanScorer2 Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal float[] coordFactors = null;
			internal int maxCoord = 0; // to be increased for each non prohibited scorer
			internal int nrMatchers; // to be increased by score() of match counting scorers.
			
			internal virtual void  Init()
			{
				// use after all scorers have been added.
				coordFactors = new float[maxCoord + 1];
				Similarity sim = Enclosing_Instance.Similarity;
				for (int i = 0; i <= maxCoord; i++)
				{
					coordFactors[i] = sim.Coord(i, maxCoord);
				}
			}
		}
		
		private Coordinator coordinator;
		
		/// <summary>The scorer to which all scoring will be delegated,
		/// except for computing and using the coordination factor.
		/// </summary>
		private Scorer countingSumScorer;
		
		/// <summary>The number of optionalScorers that need to match (if there are any) </summary>
		private int minNrShouldMatch;
		
		private int doc = - 1;
		
		/// <summary> Creates a <see cref="Scorer" /> with the given similarity and lists of required,
		/// prohibited and optional scorers. In no required scorers are added, at least
		/// one of the optional scorers will have to match during the search.
		/// 
		/// </summary>
		/// <param name="similarity">The similarity to be used.
		/// </param>
		/// <param name="minNrShouldMatch">The minimum number of optional added scorers that should match
		/// during the search. In case no required scorers are added, at least
		/// one of the optional scorers will have to match during the search.
		/// </param>
		/// <param name="required">the list of required scorers.
		/// </param>
		/// <param name="prohibited">the list of prohibited scorers.
		/// </param>
		/// <param name="optional">the list of optional scorers.
		/// </param>
		public BooleanScorer2(Similarity similarity, int minNrShouldMatch, 
                                System.Collections.Generic.List<Scorer> required,
                                System.Collections.Generic.List<Scorer> prohibited,
                                System.Collections.Generic.List<Scorer> optional,
                                IState state)
            : base(similarity)
		{
			if (minNrShouldMatch < 0)
			{
				throw new System.ArgumentException("Minimum number of optional scorers should not be negative");
			}
			coordinator = new Coordinator(this);
			this.minNrShouldMatch = minNrShouldMatch;
			
			optionalScorers = optional;
			coordinator.maxCoord += optional.Count;
			
			requiredScorers = required;
			coordinator.maxCoord += required.Count;
			
			prohibitedScorers = prohibited;
			
			coordinator.Init();
			countingSumScorer = MakeCountingSumScorer(state);
		}
		
		/// <summary>Count a scorer as a single match. </summary>
		private class SingleMatchScorer:Scorer
		{
			private void  InitBlock(BooleanScorer2 enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private BooleanScorer2 enclosingInstance;
			public BooleanScorer2 Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private Scorer scorer;
			private int lastScoredDoc = - 1;
			// Save the score of lastScoredDoc, so that we don't compute it more than
			// once in score().
			private float lastDocScore = System.Single.NaN;
			
			internal SingleMatchScorer(BooleanScorer2 enclosingInstance, Scorer scorer):base(scorer.Similarity)
			{
				InitBlock(enclosingInstance);
				this.scorer = scorer;
			}
			public override float Score(IState state)
			{
				int doc = DocID();
				if (doc >= lastScoredDoc)
				{
					if (doc > lastScoredDoc)
					{
						lastDocScore = scorer.Score(state);
						lastScoredDoc = doc;
					}
					Enclosing_Instance.coordinator.nrMatchers++;
				}
				return lastDocScore;
			}

			public override int DocID()
			{
				return scorer.DocID();
			}

			public override int NextDoc(IState state)
			{
				return scorer.NextDoc(state);
			}

			public override int Advance(int target, IState state)
			{
				return scorer.Advance(target, state);
			}
		}
		
		private Scorer CountingDisjunctionSumScorer(System.Collections.Generic.List<Scorer> scorers, int minNrShouldMatch, IState state)
		{
			// each scorer from the list counted as a single matcher
			return new AnonymousClassDisjunctionSumScorer(this, scorers, minNrShouldMatch, state);
		}
		
		private static readonly Similarity defaultSimilarity;
		
		private Scorer CountingConjunctionSumScorer(System.Collections.Generic.List<Scorer> requiredScorers, IState state)
		{
			// each scorer from the list counted as a single matcher
			int requiredNrMatchers = requiredScorers.Count;
			return new AnonymousClassConjunctionScorer(requiredNrMatchers, this, defaultSimilarity, state, requiredScorers);
		}
		
		private Scorer DualConjunctionSumScorer(Scorer req1, Scorer req2, IState state)
		{
			// non counting.
			return new ConjunctionScorer(defaultSimilarity, state, new Scorer[]{req1, req2});
			// All scorers match, so defaultSimilarity always has 1 as
			// the coordination factor.
			// Therefore the sum of the scores of two scorers
			// is used as score.
		}
		
		/// <summary>Returns the scorer to be used for match counting and score summing.
		/// Uses requiredScorers, optionalScorers and prohibitedScorers.
		/// </summary>
		private Scorer MakeCountingSumScorer(IState state)
		{
			// each scorer counted as a single matcher
			return (requiredScorers.Count == 0)?MakeCountingSumScorerNoReq(state):MakeCountingSumScorerSomeReq(state);
		}
		
		private Scorer MakeCountingSumScorerNoReq(IState state)
		{
			// No required scorers
			// minNrShouldMatch optional scorers are required, but at least 1
			int nrOptRequired = (minNrShouldMatch < 1)?1:minNrShouldMatch;
			Scorer requiredCountingSumScorer;
			if (optionalScorers.Count > nrOptRequired)
				requiredCountingSumScorer = CountingDisjunctionSumScorer(optionalScorers, nrOptRequired, state);
			else if (optionalScorers.Count == 1)
				requiredCountingSumScorer = new SingleMatchScorer(this, optionalScorers[0]);
			else
				requiredCountingSumScorer = CountingConjunctionSumScorer(optionalScorers, state);
			return AddProhibitedScorers(requiredCountingSumScorer, state);
		}
		
		private Scorer MakeCountingSumScorerSomeReq(IState state)
		{
			// At least one required scorer.
			if (optionalScorers.Count == minNrShouldMatch)
			{
				// all optional scorers also required.
                var allReq = new System.Collections.Generic.List<Scorer>(requiredScorers);
				allReq.AddRange(optionalScorers);
				return AddProhibitedScorers(CountingConjunctionSumScorer(allReq, state), state);
			}
			else
			{
				// optionalScorers.size() > minNrShouldMatch, and at least one required scorer
				Scorer requiredCountingSumScorer = 
                                    requiredScorers.Count == 1
                                    ? new SingleMatchScorer(this, requiredScorers[0])
                                    : CountingConjunctionSumScorer(requiredScorers, state);
				if (minNrShouldMatch > 0)
				{
					// use a required disjunction scorer over the optional scorers
					return AddProhibitedScorers(DualConjunctionSumScorer(requiredCountingSumScorer, CountingDisjunctionSumScorer(optionalScorers, minNrShouldMatch, state), state), state);
				}
				else
				{
					// minNrShouldMatch == 0
					return new ReqOptSumScorer(AddProhibitedScorers(requiredCountingSumScorer, state), 
                                               optionalScorers.Count == 1
                                               ? new SingleMatchScorer(this, optionalScorers[0])
                                               : CountingDisjunctionSumScorer(optionalScorers, 1, state));
				}
			}
		}
		
		/// <summary>Returns the scorer to be used for match counting and score summing.
		/// Uses the given required scorer and the prohibitedScorers.
		/// </summary>
		/// <param name="requiredCountingSumScorer">A required scorer already built.
		/// </param>
		private Scorer AddProhibitedScorers(Scorer requiredCountingSumScorer, IState state)
		{
			return (prohibitedScorers.Count == 0) 
                   ? requiredCountingSumScorer
                   : new ReqExclScorer(requiredCountingSumScorer, 
                                       ((prohibitedScorers.Count == 1)
                                        ? prohibitedScorers[0]
                                        : new DisjunctionSumScorer(prohibitedScorers, state)));
		}
		
		/// <summary>Scores and collects all matching documents.</summary>
		/// <param name="collector">The collector to which all matching documents are passed through.
		/// </param>
		public override void  Score(Collector collector, IState state)
		{
			collector.SetScorer(this);
			while ((doc = countingSumScorer.NextDoc(state)) != NO_MORE_DOCS)
			{
				collector.Collect(doc, state);
			}
		}
		
		public /*protected internal*/ override bool Score(Collector collector, int max, int firstDocID, IState state)
		{
			doc = firstDocID;
			collector.SetScorer(this);
			while (doc < max)
			{
				collector.Collect(doc, state);
				doc = countingSumScorer.NextDoc(state);
			}
			return doc != NO_MORE_DOCS;
		}
		
		public override int DocID()
		{
			return doc;
		}
		
		public override int NextDoc(IState state)
		{
			return doc = countingSumScorer.NextDoc(state);
		}
		
		public override float Score(IState state)
		{
			coordinator.nrMatchers = 0;
			float sum = countingSumScorer.Score(state);
			return sum * coordinator.coordFactors[coordinator.nrMatchers];
		}
		
		public override int Advance(int target, IState state)
		{
			return doc = countingSumScorer.Advance(target, state);
		}
		
		static BooleanScorer2()
		{
			defaultSimilarity = Search.Similarity.Default;
		}
	}
}