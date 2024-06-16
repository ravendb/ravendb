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
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Lucene.Net.Util;
using IndexReader = Lucene.Net.Index.IndexReader;
using Single = Lucene.Net.Support.Single;
using Term = Lucene.Net.Index.Term;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;

namespace Lucene.Net.Search
{

    /// <summary>Implements the fuzzy search query. The similarity measurement
    /// is based on the Levenshtein (edit distance) algorithm.
    /// 
    /// Warning: this query is not very scalable with its default prefix
    /// length of 0 - in this case, *every* term will be enumerated and
    /// cause an edit score calculation.
    /// 
    /// </summary>

        [Serializable]
    public class FuzzyQuery : MultiTermQuery
	{
		
		public const float defaultMinSimilarity = 0.5f;
		public const int defaultPrefixLength = 0;
		
		private float minimumSimilarity;
		private int prefixLength;
		private bool termLongEnough = false;

        /// <summary> Returns the pattern term.</summary>
	    public Term Term { get; protected internal set; }

	    /// <summary> Create a new FuzzyQuery that will match terms with a similarity 
		/// of at least <c>minimumSimilarity</c> to <c>term</c>.
		/// If a <c>prefixLength</c> &gt; 0 is specified, a common prefix
		/// of that length is also required.
		/// 
		/// </summary>
		/// <param name="term">the term to search for
		/// </param>
		/// <param name="minimumSimilarity">a value between 0 and 1 to set the required similarity
		/// between the query term and the matching terms. For example, for a
		/// <c>minimumSimilarity</c> of <c>0.5</c> a term of the same length
		/// as the query term is considered similar to the query term if the edit distance
		/// between both terms is less than <c>length(term)*0.5</c>
		/// </param>
		/// <param name="prefixLength">length of common (non-fuzzy) prefix
		/// </param>
		/// <throws>  IllegalArgumentException if minimumSimilarity is &gt;= 1 or &lt; 0 </throws>
		/// <summary> or if prefixLength &lt; 0
		/// </summary>
		public FuzzyQuery(Term term, float minimumSimilarity, int prefixLength)
		{
			this.Term = term;
			
			if (minimumSimilarity >= 1.0f)
				throw new System.ArgumentException("minimumSimilarity >= 1");
			else if (minimumSimilarity < 0.0f)
				throw new System.ArgumentException("minimumSimilarity < 0");
			if (prefixLength < 0)
				throw new System.ArgumentException("prefixLength < 0");
			
			if (term.Text.Length > 1.0f / (1.0f - minimumSimilarity))
			{
				this.termLongEnough = true;
			}
			
			this.minimumSimilarity = minimumSimilarity;
			this.prefixLength = prefixLength;
			internalRewriteMethod = SCORING_BOOLEAN_QUERY_REWRITE;
		}

        /// <summary> Calls <see cref="FuzzyQuery(Index.Term, float)">FuzzyQuery(term, minimumSimilarity, 0)</see>.</summary>
		public FuzzyQuery(Term term, float minimumSimilarity):this(term, minimumSimilarity, defaultPrefixLength)
		{
		}

        /// <summary> Calls <see cref="FuzzyQuery(Index.Term, float)">FuzzyQuery(term, 0.5f, 0)</see>.</summary>
		public FuzzyQuery(Term term):this(term, defaultMinSimilarity, defaultPrefixLength)
		{
		}

	    /// <summary> Returns the minimum similarity that is required for this query to match.</summary>
	    /// <value> float value between 0.0 and 1.0 </value>
	    public virtual float MinSimilarity
	    {
	        get { return minimumSimilarity; }
	    }

	    /// <summary> Returns the non-fuzzy prefix length. This is the number of characters at the start
	    /// of a term that must be identical (not fuzzy) to the query term if the query
	    /// is to match that term. 
	    /// </summary>
	    public virtual int PrefixLength
	    {
	        get { return prefixLength; }
	    }

	    protected internal override FilteredTermEnum GetEnum(IndexReader reader, IState state)
		{
			return new FuzzyTermEnum(reader, Term, minimumSimilarity, prefixLength, state);
		}

	    public override RewriteMethod RewriteMethod
	    {
	        set { throw new System.NotSupportedException("FuzzyQuery cannot change rewrite method"); }
	    }

	    public override Query Rewrite(IndexReader reader, IState state)
		{
			if (!termLongEnough)
			{
				// can only match if it's exact
				return new TermQuery(Term);
			}

		    int maxSize = BooleanQuery.MaxClauseCount;

            // TODO: Java uses a PriorityQueue.  Using Linq, we can emulate it, 
            //       however it's considerable slower than the java counterpart.
            //       this should be a temporary thing, fixed before release
            SortedList<ScoreTerm, ScoreTerm> stQueue = new SortedList<ScoreTerm, ScoreTerm>();
			FilteredTermEnum enumerator = GetEnum(reader, state);
			
			try
			{
                ScoreTerm st = new ScoreTerm();
				do 
				{
					Term t = enumerator.Term;
                    if (t == null) break;
				    float score = enumerator.Difference();
                    //ignore uncompetetive hits
                    if (stQueue.Count >= maxSize && score <= stQueue.Keys.First().score)
                        continue;
                    // add new entry in PQ
				    st.term = t;
				    st.score = score;
				    stQueue.Add(st, st);
                    // possibly drop entries from queue
                    if (stQueue.Count > maxSize)
                    {
                        st = stQueue.Keys.First();
                        stQueue.Remove(st);
                    }
                    else
                    {
                        st = new ScoreTerm();
                    }
				}
				while (enumerator.Next(state));
			}
			finally
			{
				enumerator.Close();
			}
			
			BooleanQuery query = new BooleanQuery(true);
			foreach(ScoreTerm st in stQueue.Keys)
			{
				TermQuery tq = new TermQuery(st.term); // found a match
				tq.Boost = Boost * st.score; // set the boost
				query.Add(tq, Occur.SHOULD); // add to query
			}
			
			return query;
		}
		
		public override System.String ToString(System.String field)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			if (!Term.Field.Equals(field))
			{
				buffer.Append(Term.Field);
				buffer.Append(":");
			}
			buffer.Append(Term.Text);
			buffer.Append('~');
			buffer.Append(Single.ToString(minimumSimilarity));
			buffer.Append(ToStringUtils.Boost(Boost));
			return buffer.ToString();
		}
		
		protected internal class ScoreTerm : IComparable<ScoreTerm>
		{
			public Term term;
			public float score;

		    public int CompareTo(ScoreTerm other)
		    {
                if (Comparer<float>.Default.Compare(this.score, other.score) == 0)
                {
                    return other.term.CompareTo(this.term);
                }
                else
                {
                    return Comparer<float>.Default.Compare(this.score, other.score);
                }
		    }
		}
		
		public override int GetHashCode()
		{
			int prime = 31;
			int result = base.GetHashCode();
			result = prime * result + BitConverter.ToInt32(BitConverter.GetBytes(minimumSimilarity), 0);
			result = prime * result + prefixLength;
			result = prime * result + ((Term == null)?0:Term.GetHashCode());
			return result;
		}
		
		public  override bool Equals(System.Object obj)
		{
			if (this == obj)
				return true;
			if (!base.Equals(obj))
				return false;
			if (GetType() != obj.GetType())
				return false;
			FuzzyQuery other = (FuzzyQuery) obj;
			if (BitConverter.ToInt32(BitConverter.GetBytes(minimumSimilarity), 0) != BitConverter.ToInt32(BitConverter.GetBytes(other.minimumSimilarity), 0))
				return false;
			if (prefixLength != other.prefixLength)
				return false;
			if (Term == null)
			{
				if (other.Term != null)
					return false;
			}
			else if (!Term.Equals(other.Term))
				return false;
			return true;
		}
	}
}