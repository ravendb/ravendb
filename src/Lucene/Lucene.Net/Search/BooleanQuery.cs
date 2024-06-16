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
using System.Collections;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Support;
using IndexReader = Lucene.Net.Index.IndexReader;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;
using Occur = Lucene.Net.Search.Occur;

namespace Lucene.Net.Search
{

    /// <summary>A Query that matches documents matching boolean combinations of other
    /// queries, e.g. <see cref="TermQuery" />s, <see cref="PhraseQuery" />s or other
    /// BooleanQuerys.
    /// </summary>
    [Serializable]
    public class BooleanQuery : Query, System.Collections.Generic.IEnumerable<BooleanClause>, System.ICloneable
	{
        [Serializable]
        private class AnonymousClassSimilarityDelegator:SimilarityDelegator
		{
			private void  InitBlock(BooleanQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private BooleanQuery enclosingInstance;
			public BooleanQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassSimilarityDelegator(BooleanQuery enclosingInstance, Lucene.Net.Search.Similarity Param1):base(Param1)
			{
				InitBlock(enclosingInstance);
			}
			public override float Coord(int overlap, int maxOverlap)
			{
				return 1.0f;
			}
		}
		
		private static int _maxClauses = 1024;

        /// <summary>Thrown when an attempt is made to add more than <see cref="MaxClauseCount" />
        /// clauses. This typically happens if
        /// a PrefixQuery, FuzzyQuery, WildcardQuery, or TermRangeQuery 
        /// is expanded to many terms during search. 
        /// </summary>
        [Serializable]
        public class TooManyClauses:System.SystemException
		{
			public override System.String Message
			{
				get
				{
					return "maxClauseCount is set to " + Lucene.Net.Search.BooleanQuery._maxClauses;
				}
				
			}
		}

	    /// <summary>Gets or sets the maximum number of clauses permitted, 1024 by default.
	    /// Attempts to add more than the permitted number of clauses cause <see cref="TooManyClauses" />
	    /// to be thrown.
	    /// </summary>
	    public static int MaxClauseCount
	    {
	        get { return _maxClauses; }
	        set
	        {
	            if (value < 1)
	                throw new ArgumentException("maxClauseCount must be >= 1");
	            _maxClauses = value;
	        }
	    }

	    private EquatableList<BooleanClause> clauses = new EquatableList<BooleanClause>();
		private bool disableCoord;
		
		/// <summary>Constructs an empty boolean query. </summary>
		public BooleanQuery()
		{
		}
		
		/// <summary>Constructs an empty boolean query.
		/// 
		/// <see cref="Similarity.Coord(int,int)" /> may be disabled in scoring, as
		/// appropriate. For example, this score factor does not make sense for most
		/// automatically generated queries, like <see cref="WildcardQuery" /> and <see cref="FuzzyQuery" />
		///.
		/// 
		/// </summary>
		/// <param name="disableCoord">disables <see cref="Similarity.Coord(int,int)" /> in scoring.
		/// </param>
		public BooleanQuery(bool disableCoord)
		{
			this.disableCoord = disableCoord;
		}
		
		/// <summary>Returns true iff <see cref="Similarity.Coord(int,int)" /> is disabled in
		/// scoring for this query instance.
		/// </summary>
		/// <seealso cref="BooleanQuery(bool)">
		/// </seealso>
		public virtual bool IsCoordDisabled()
		{
			return disableCoord;
		}
		
		// Implement coord disabling.
		// Inherit javadoc.
		public override Similarity GetSimilarity(Searcher searcher)
		{
			Similarity result = base.GetSimilarity(searcher);
			if (disableCoord)
			{
				// disable coord as requested
				result = new AnonymousClassSimilarityDelegator(this, result);
			}
			return result;
		}

        protected internal int minNrShouldMatch = 0;

	    /// <summary>
	    /// Specifies a minimum number of the optional BooleanClauses
	    /// which must be satisfied.
	    /// <para>
	    /// By default no optional clauses are necessary for a match
	    /// (unless there are no required clauses).  If this method is used,
	    /// then the specified number of clauses is required.
	    /// </para>
	    /// <para>
	    /// Use of this method is totally independent of specifying that
	    /// any specific clauses are required (or prohibited).  This number will
	    /// only be compared against the number of matching optional clauses.
	    /// </para>
	    /// </summary>
	    public virtual int MinimumNumberShouldMatch
	    {
	        set { this.minNrShouldMatch = value; }
	        get { return minNrShouldMatch; }
	    }

	    /// <summary>Adds a clause to a boolean query.
		/// 
		/// </summary>
		/// <throws>  TooManyClauses if the new number of clauses exceeds the maximum clause number </throws>
		/// <seealso cref="MaxClauseCount">
		/// </seealso>
		public virtual void  Add(Query query, Occur occur)
		{
			Add(new BooleanClause(query, occur));
		}
		
		/// <summary>Adds a clause to a boolean query.</summary>
		/// <throws>  TooManyClauses if the new number of clauses exceeds the maximum clause number </throws>
		/// <seealso cref="MaxClauseCount">
		/// </seealso>
		public virtual void  Add(BooleanClause clause)
		{
			if (clauses.Count >= _maxClauses)
				throw new TooManyClauses();
			
			clauses.Add(clause);
		}

		/// <summary>Returns the set of clauses in this query. </summary>
		public virtual BooleanClause[] GetClauses()
		{
			return clauses.ToArray();
		}

        /// <summary>Returns the list of clauses in this query. </summary>
	    public virtual System.Collections.Generic.List<BooleanClause> Clauses
	    {
            get { return clauses; }
	    }
		
        /// <summary>
        /// Returns an iterator on the clauses in this query.
        /// </summary>
        /// <returns></returns>
        public System.Collections.Generic.IEnumerator<BooleanClause> GetEnumerator()
        {
            return clauses.GetEnumerator();
        }
        /// <summary> Expert: the Weight for BooleanQuery, used to
        /// normalize, score and explain these queries.
        /// 
        /// <p/>NOTE: this API and implementation is subject to
        /// change suddenly in the next release.<p/>
        /// </summary>
        [Serializable]
        protected internal class BooleanWeight:Weight
		{
			private void  InitBlock(BooleanQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private BooleanQuery enclosingInstance;
			public BooleanQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			/// <summary>The Similarity implementation. </summary>
			protected internal Similarity similarity;
			protected internal System.Collections.Generic.List<Weight> weights;
			
			public BooleanWeight(BooleanQuery enclosingInstance, Searcher searcher, IState state)
			{
				InitBlock(enclosingInstance);
				this.similarity = Enclosing_Instance.GetSimilarity(searcher);
                weights = new System.Collections.Generic.List<Weight>(Enclosing_Instance.clauses.Count);
				for (int i = 0; i < Enclosing_Instance.clauses.Count; i++)
				{
				    weights.Add(Enclosing_Instance.clauses[i].Query.CreateWeight(searcher, state));
				}
			}

		    public override Query Query
		    {
		        get { return Enclosing_Instance; }
		    }

		    public override float Value
		    {
		        get { return Enclosing_Instance.Boost; }
		    }

		    public override float GetSumOfSquaredWeights()
		    {
		        float sum = 0.0f;
		        for (int i = 0; i < weights.Count; i++)
		        {
		            // call sumOfSquaredWeights for all clauses in case of side effects
		            float s = weights[i].GetSumOfSquaredWeights(); // sum sub weights
                    if (!Enclosing_Instance.clauses[i].IsProhibited)
		                // only add to sum for non-prohibited clauses
		                sum += s;
		        }

		        sum *= Enclosing_Instance.Boost*Enclosing_Instance.Boost; // boost each sub-weight

		        return sum;
		    }


		    public override void  Normalize(float norm)
			{
				norm *= Enclosing_Instance.Boost; // incorporate boost
				foreach (Weight w in weights)
				{
					// normalize all clauses, (even if prohibited in case of side affects)
					w.Normalize(norm);
				}
			}
			
			public override Explanation Explain(IndexReader reader, int doc, IState state)
			{
				int minShouldMatch = Enclosing_Instance.MinimumNumberShouldMatch;
				ComplexExplanation sumExpl = new ComplexExplanation();
				sumExpl.Description = "sum of:";
				int coord = 0;
				int maxCoord = 0;
				float sum = 0.0f;
				bool fail = false;
				int shouldMatchCount = 0;
			    System.Collections.Generic.IEnumerator<BooleanClause> cIter = Enclosing_Instance.clauses.GetEnumerator();
				for (System.Collections.Generic.IEnumerator<Weight> wIter = weights.GetEnumerator(); wIter.MoveNext(); )
				{
                    cIter.MoveNext();
                    Weight w = wIter.Current;
					BooleanClause c = cIter.Current;
					if (w.Scorer(reader, true, true, state) == null)
					{
						continue;
					}
					Explanation e = w.Explain(reader, doc, state);
                    if (!c.IsProhibited)
						maxCoord++;
					if (e.IsMatch)
					{
                        if (!c.IsProhibited)
						{
							sumExpl.AddDetail(e);
							sum += e.Value;
							coord++;
						}
						else
						{
                            Explanation r = new Explanation(0.0f, "match on prohibited clause (" + c.Query.ToString() + ")");
							r.AddDetail(e);
							sumExpl.AddDetail(r);
							fail = true;
						}
						if (c.Occur == Occur.SHOULD)
							shouldMatchCount++;
					}
                    else if (c.IsRequired)
					{
                        Explanation r = new Explanation(0.0f, "no match on required clause (" + c.Query.ToString() + ")");
						r.AddDetail(e);
						sumExpl.AddDetail(r);
						fail = true;
					}
				}
				if (fail)
				{
					System.Boolean tempAux = false;
					sumExpl.Match = tempAux;
					sumExpl.Value = 0.0f;
					sumExpl.Description = "Failure to meet condition(s) of required/prohibited clause(s)";
					return sumExpl;
				}
				else if (shouldMatchCount < minShouldMatch)
				{
					System.Boolean tempAux2 = false;
					sumExpl.Match = tempAux2;
					sumExpl.Value = 0.0f;
					sumExpl.Description = "Failure to match minimum number " + "of optional clauses: " + minShouldMatch;
					return sumExpl;
				}
				
				sumExpl.Match = 0 < coord?true:false;
				sumExpl.Value = sum;
				
				float coordFactor = similarity.Coord(coord, maxCoord);
				if (coordFactor == 1.0f)
				// coord is no-op
					return sumExpl;
				// eliminate wrapper
				else
				{
					ComplexExplanation result = new ComplexExplanation(sumExpl.IsMatch, sum * coordFactor, "product of:");
					result.AddDetail(sumExpl);
					result.AddDetail(new Explanation(coordFactor, "coord(" + coord + "/" + maxCoord + ")"));
					return result;
				}
			}
			
			public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
			{
				var required = new System.Collections.Generic.List<Scorer>();
                var prohibited = new System.Collections.Generic.List<Scorer>();
                var optional = new System.Collections.Generic.List<Scorer>();

			    System.Collections.Generic.IEnumerator<BooleanClause> cIter = Enclosing_Instance.clauses.GetEnumerator();
				foreach (Weight w in weights)
				{
                    cIter.MoveNext();
					BooleanClause c = (BooleanClause) cIter.Current;
					Scorer subScorer = w.Scorer(reader, true, false, state);
					if (subScorer == null)
					{
                        if (c.IsRequired)
						{
							return null;
						}
					}
                    else if (c.IsRequired)
					{
						required.Add(subScorer);
					}
                    else if (c.IsProhibited)
					{
						prohibited.Add(subScorer);
					}
					else
					{
						optional.Add(subScorer);
					}
				}
				
				// Check if we can return a BooleanScorer
				if (!scoreDocsInOrder && topScorer && required.Count == 0 && prohibited.Count < 32)
				{
					return new BooleanScorer(similarity, Enclosing_Instance.minNrShouldMatch, optional, prohibited, state);
				}
				
				if (required.Count == 0 && optional.Count == 0)
				{
					// no required and optional clauses.
					return null;
				}
				else if (optional.Count < Enclosing_Instance.minNrShouldMatch)
				{
					// either >1 req scorer, or there are 0 req scorers and at least 1
					// optional scorer. Therefore if there are not enough optional scorers
					// no documents will be matched by the query
					return null;
				}
				
				// Return a BooleanScorer2
				return new BooleanScorer2(similarity, Enclosing_Instance.minNrShouldMatch, required, prohibited, optional, state);
			}

		    public override bool GetScoresDocsOutOfOrder()
		    {
		        int numProhibited = 0;
		        foreach (BooleanClause c in Enclosing_Instance.clauses)
		        {
                    if (c.IsRequired)
		            {
		                return false; // BS2 (in-order) will be used by scorer()
		            }
                    else if (c.IsProhibited)
		            {
		                ++numProhibited;
		            }
		        }

		        if (numProhibited > 32)
		        {
		            // cannot use BS
		            return false;
		        }

		        // scorer() will return an out-of-order scorer if requested.
		        return true;
		    }
		}
		
		public override Weight CreateWeight(Searcher searcher, IState state)
		{
			return new BooleanWeight(this, searcher, state);
		}
		
		public override Query Rewrite(IndexReader reader, IState state)
		{
			if (minNrShouldMatch == 0 && clauses.Count == 1)
			{
				// optimize 1-clause queries
				BooleanClause c = clauses[0];
                if (!c.IsProhibited)
				{
					// just return clause

                    Query query = c.Query.Rewrite(reader, state); // rewrite first
					
					if (Boost != 1.0f)
					{
						// incorporate boost
                        if (query == c.Query)
						// if rewrite was no-op
							query = (Query) query.Clone(); // then clone before boost
						query.Boost = Boost * query.Boost;
					}
					
					return query;
				}
			}
			
			BooleanQuery clone = null; // recursively rewrite
			for (int i = 0; i < clauses.Count; i++)
			{
				BooleanClause c = clauses[i];
                Query query = c.Query.Rewrite(reader, state);
                if (query != c.Query)
				{
					// clause rewrote: must clone
					if (clone == null)
						clone = (BooleanQuery) this.Clone();
					clone.clauses[i] = new BooleanClause(query, c.Occur);
				}
			}
			if (clone != null)
			{
				return clone; // some clauses rewrote
			}
			else
				return this; // no clauses rewrote
		}
		
		// inherit javadoc
		public override void ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
			foreach(BooleanClause clause in clauses)
			{
                clause.Query.ExtractTerms(terms);
			}
		}
		
		public override System.Object Clone()
		{
			BooleanQuery clone = (BooleanQuery) base.Clone();
			clone.clauses = (EquatableList<BooleanClause>) this.clauses.Clone();
			return clone;
		}
		
		/// <summary>Prints a user-readable version of this query. </summary>
		public override System.String ToString(System.String field)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			bool needParens = (Boost != 1.0) || (MinimumNumberShouldMatch > 0);
			if (needParens)
			{
				buffer.Append("(");
			}
			
			for (int i = 0; i < clauses.Count; i++)
			{
				BooleanClause c = clauses[i];
                if (c.IsProhibited)
					buffer.Append("-");
                else if (c.IsRequired)
					buffer.Append("+");

                Query subQuery = c.Query;
				if (subQuery != null)
				{
					if (subQuery is BooleanQuery)
					{
						// wrap sub-bools in parens
						buffer.Append("(");
						buffer.Append(subQuery.ToString(field));
						buffer.Append(")");
					}
					else
					{
						buffer.Append(subQuery.ToString(field));
					}
				}
				else
				{
					buffer.Append("null");
				}
				
				if (i != clauses.Count - 1)
					buffer.Append(" ");
			}
			
			if (needParens)
			{
				buffer.Append(")");
			}
			
			if (MinimumNumberShouldMatch > 0)
			{
				buffer.Append('~');
				buffer.Append(MinimumNumberShouldMatch);
			}
			
			if (Boost != 1.0f)
			{
				buffer.Append(ToStringUtils.Boost(Boost));
			}
			
			return buffer.ToString();
		}
		
		/// <summary>Returns true iff <c>o</c> is equal to this. </summary>
		public  override bool Equals(System.Object o)
		{
            if (!(o is BooleanQuery))
                return false;
            BooleanQuery other = (BooleanQuery)o;
            return (this.Boost == other.Boost)
                    && this.clauses.Equals(other.clauses)
                    && this.MinimumNumberShouldMatch == other.MinimumNumberShouldMatch
                    && this.disableCoord == other.disableCoord;
		}
		
		/// <summary>Returns a hash code value for this object.</summary>
		public override int GetHashCode()
		{
            return BitConverter.ToInt32(BitConverter.GetBytes(Boost), 0) ^ clauses.GetHashCode() + MinimumNumberShouldMatch + (disableCoord ? 17 : 0);
		}

	    IEnumerator IEnumerable.GetEnumerator()
	    {
	        return GetEnumerator();
	    }
	}
}