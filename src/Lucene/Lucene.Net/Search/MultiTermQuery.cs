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
using System.Runtime.InteropServices;
using Lucene.Net.Store;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using ToStringUtils = Lucene.Net.Util.ToStringUtils;

namespace Lucene.Net.Search
{

    /// <summary> An abstract <see cref="Query" /> that matches documents
    /// containing a subset of terms provided by a <see cref="FilteredTermEnum" />
    /// enumeration.
    /// 
    /// <p/>This query cannot be used directly; you must subclass
    /// it and define <see cref="GetEnum" /> to provide a <see cref="FilteredTermEnum" />
    /// that iterates through the terms to be
    /// matched.
    /// 
    /// <p/><b>NOTE</b>: if <see cref="RewriteMethod" /> is either
    /// <see cref="CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE" /> or <see cref="SCORING_BOOLEAN_QUERY_REWRITE" />
    ///, you may encounter a
    /// <see cref="BooleanQuery.TooManyClauses" /> exception during
    /// searching, which happens when the number of terms to be
    /// searched exceeds <see cref="BooleanQuery.MaxClauseCount" />
    ///.  Setting <see cref="RewriteMethod" />
    /// to <see cref="CONSTANT_SCORE_FILTER_REWRITE" />
    /// prevents this.
    /// 
    /// <p/>The recommended rewrite method is <see cref="CONSTANT_SCORE_AUTO_REWRITE_DEFAULT" />
    ///: it doesn't spend CPU
    /// computing unhelpful scores, and it tries to pick the most
    /// performant rewrite method given the query.
    /// 
    /// Note that <see cref="QueryParser" /> produces
    /// MultiTermQueries using <see cref="CONSTANT_SCORE_AUTO_REWRITE_DEFAULT" />
    /// by default.
    /// </summary>

        [Serializable]
    public abstract class MultiTermQuery:Query
	{

        [Serializable]
        public class AnonymousClassConstantScoreAutoRewrite:ConstantScoreAutoRewrite
		{
		    public override int TermCountCutoff
		    {
		        set { throw new System.NotSupportedException("Please create a private instance"); }
		    }

		    public override double DocCountPercent
		    {
		        set { throw new System.NotSupportedException("Please create a private instance"); }
		    }

		    // Make sure we are still a singleton even after deserializing
			protected internal virtual System.Object ReadResolve()
			{
				return Lucene.Net.Search.MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;
			}
		}
		protected internal RewriteMethod internalRewriteMethod = CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;
		[NonSerialized]
		internal int numberOfTerms = 0;


        [Serializable]
        private sealed class ConstantScoreFilterRewrite:RewriteMethod
		{
			public override Query Rewrite(IndexReader reader, MultiTermQuery query, IState state)
			{
				Query result = new ConstantScoreQuery(new MultiTermQueryWrapperFilter<MultiTermQuery>(query));
				result.Boost = query.Boost;
				return result;
			}
			
			// Make sure we are still a singleton even after deserializing
			internal System.Object ReadResolve()
			{
				return Lucene.Net.Search.MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE;
			}
		}
		
		/// <summary>A rewrite method that first creates a private Filter,
		/// by visiting each term in sequence and marking all docs
		/// for that term.  Matching documents are assigned a
		/// constant score equal to the query's boost.
		/// 
		/// <p/> This method is faster than the BooleanQuery
		/// rewrite methods when the number of matched terms or
		/// matched documents is non-trivial. Also, it will never
		/// hit an errant <see cref="BooleanQuery.TooManyClauses" />
		/// exception.
		/// 
		/// </summary>
		/// <seealso cref="RewriteMethod">
		/// </seealso>
		public static readonly RewriteMethod CONSTANT_SCORE_FILTER_REWRITE = new ConstantScoreFilterRewrite();


        [Serializable]
        private class ScoringBooleanQueryRewrite:RewriteMethod
		{
			public override Query Rewrite(IndexReader reader, MultiTermQuery query, IState state)
			{
				
				FilteredTermEnum enumerator = query.GetEnum(reader, state);
				BooleanQuery result = new BooleanQuery(true);
				int count = 0;
				try
				{
					do 
					{
						Term t = enumerator.Term;
						if (t != null)
						{
							TermQuery tq = new TermQuery(t); // found a match
							tq.Boost = query.Boost * enumerator.Difference(); // set the boost
							result.Add(tq, Occur.SHOULD); // add to query
							count++;
						}
					}
					while (enumerator.Next(state));
				}
				finally
				{
					enumerator.Close();
				}
				query.IncTotalNumberOfTerms(count);
				return result;
			}
			
			// Make sure we are still a singleton even after deserializing
			protected internal virtual System.Object ReadResolve()
			{
				return Lucene.Net.Search.MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE;
			}
		}
		
		/// <summary>A rewrite method that first translates each term into
		/// <see cref="Occur.SHOULD" /> clause in a
		/// BooleanQuery, and keeps the scores as computed by the
		/// query.  Note that typically such scores are
		/// meaningless to the user, and require non-trivial CPU
		/// to compute, so it's almost always better to use <see cref="CONSTANT_SCORE_AUTO_REWRITE_DEFAULT" />
		/// instead.
		/// 
		/// <p/><b>NOTE</b>: This rewrite method will hit <see cref="BooleanQuery.TooManyClauses" />
		/// if the number of terms
		/// exceeds <see cref="BooleanQuery.MaxClauseCount" />.
		/// 
		/// </summary>
		/// <seealso cref="RewriteMethod">
		/// </seealso>
		public static readonly RewriteMethod SCORING_BOOLEAN_QUERY_REWRITE = new ScoringBooleanQueryRewrite();


        [Serializable]
        private class ConstantScoreBooleanQueryRewrite:ScoringBooleanQueryRewrite
		{
			public override Query Rewrite(IndexReader reader, MultiTermQuery query, IState state)
			{
				// strip the scores off
				Query result = new ConstantScoreQuery(new QueryWrapperFilter(base.Rewrite(reader, query, state)));
				result.Boost = query.Boost;
				return result;
			}
			
			// Make sure we are still a singleton even after deserializing
			protected internal override System.Object ReadResolve()
			{
				return Lucene.Net.Search.MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE;
			}
		}
		
		/// <summary>Like <see cref="SCORING_BOOLEAN_QUERY_REWRITE" /> except
		/// scores are not computed.  Instead, each matching
		/// document receives a constant score equal to the
		/// query's boost.
		/// 
		/// <p/><b>NOTE</b>: This rewrite method will hit <see cref="BooleanQuery.TooManyClauses" />
		/// if the number of terms
		/// exceeds <see cref="BooleanQuery.MaxClauseCount" />.
		/// 
		/// </summary>
		/// <seealso cref="RewriteMethod">
		/// </seealso>
		public static readonly RewriteMethod CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE = new ConstantScoreBooleanQueryRewrite();


        /// <summary>A rewrite method that tries to pick the best
        /// constant-score rewrite method based on term and
        /// document counts from the query.  If both the number of
        /// terms and documents is small enough, then <see cref="CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE" />
        /// is used.
        /// Otherwise, <see cref="CONSTANT_SCORE_FILTER_REWRITE" /> is
        /// used.
        /// </summary>

        [Serializable]
        public class ConstantScoreAutoRewrite:RewriteMethod
		{
			public ConstantScoreAutoRewrite()
			{
				InitBlock();
			}
			private void  InitBlock()
			{
				termCountCutoff = DEFAULT_TERM_COUNT_CUTOFF;
				docCountPercent = DEFAULT_DOC_COUNT_PERCENT;
			}
			
			// Defaults derived from rough tests with a 20.0 million
			// doc Wikipedia index.  With more than 350 terms in the
			// query, the filter method is fastest:
			public static int DEFAULT_TERM_COUNT_CUTOFF = 350;
			
			// If the query will hit more than 1 in 1000 of the docs
			// in the index (0.1%), the filter method is fastest:
			public static double DEFAULT_DOC_COUNT_PERCENT = 0.1;
			
			private int termCountCutoff;
			private double docCountPercent;

		    /// <summary>If the number of terms in this query is equal to or
		    /// larger than this setting then <see cref="CONSTANT_SCORE_FILTER_REWRITE" />
		    /// is used. 
		    /// </summary>
		    public virtual int TermCountCutoff
		    {
		        get { return termCountCutoff; }
		        set { termCountCutoff = value; }
		    }

		    /// <summary>If the number of documents to be visited in the
		    /// postings exceeds this specified percentage of the
		    /// MaxDoc for the index, then <see cref="CONSTANT_SCORE_FILTER_REWRITE" />
		    /// is used.
		    /// </summary>
		    /// <value> 0.0 to 100.0 </value>
		    public virtual double DocCountPercent
		    {
		        get { return docCountPercent; }
		        set { docCountPercent = value; }
		    }

		    public override Query Rewrite(IndexReader reader, MultiTermQuery query, IState state)
			{
				// Get the enum and start visiting terms.  If we
				// exhaust the enum before hitting either of the
				// cutoffs, we use ConstantBooleanQueryRewrite; else,
				// ConstantFilterRewrite:
				ICollection<Term> pendingTerms = new List<Term>();
				int docCountCutoff = (int) ((docCountPercent / 100.0) * reader.MaxDoc);
				int termCountLimit = System.Math.Min(BooleanQuery.MaxClauseCount, termCountCutoff);
				int docVisitCount = 0;
				
				FilteredTermEnum enumerator = query.GetEnum(reader, state);
				try
				{
					while (true)
					{
						Term t = enumerator.Term;
						if (t != null)
						{
							pendingTerms.Add(t);
							// Loading the TermInfo from the terms dict here
							// should not be costly, because 1) the
							// query/filter will load the TermInfo when it
							// runs, and 2) the terms dict has a cache:
							docVisitCount += reader.DocFreq(t, state);
						}
						
						if (pendingTerms.Count >= termCountLimit || docVisitCount >= docCountCutoff)
						{
							// Too many terms -- make a filter.
							Query result = new ConstantScoreQuery(new MultiTermQueryWrapperFilter<MultiTermQuery>(query));
							result.Boost = query.Boost;
							return result;
						}
						else if (!enumerator.Next(state))
						{
							// Enumeration is done, and we hit a small
							// enough number of terms & docs -- just make a
							// BooleanQuery, now
							BooleanQuery bq = new BooleanQuery(true);
							foreach(Term term in pendingTerms)
							{
								TermQuery tq = new TermQuery(term);
								bq.Add(tq, Occur.SHOULD);
							}
							// Strip scores
							Query result = new ConstantScoreQuery(new QueryWrapperFilter(bq));
							result.Boost = query.Boost;
							query.IncTotalNumberOfTerms(pendingTerms.Count);
							return result;
						}
					}
				}
				finally
				{
					enumerator.Close();
				}
			}
			
			public override int GetHashCode()
			{
				int prime = 1279;
				return (int) (prime * termCountCutoff + BitConverter.DoubleToInt64Bits(docCountPercent));
			}
			
			public  override bool Equals(System.Object obj)
			{
				if (this == obj)
					return true;
				if (obj == null)
					return false;
				if (GetType() != obj.GetType())
					return false;
				
				ConstantScoreAutoRewrite other = (ConstantScoreAutoRewrite) obj;
				if (other.termCountCutoff != termCountCutoff)
				{
					return false;
				}
				
				if (BitConverter.DoubleToInt64Bits(other.docCountPercent) != BitConverter.DoubleToInt64Bits(docCountPercent))
				{
					return false;
				}
				
				return true;
			}
		}
		
		/// <summary>Read-only default instance of <see cref="ConstantScoreAutoRewrite" />
		///, with <see cref="ConstantScoreAutoRewrite.TermCountCutoff" />
		/// set to
		/// <see cref="ConstantScoreAutoRewrite.DEFAULT_TERM_COUNT_CUTOFF" />
		///
		/// and <see cref="ConstantScoreAutoRewrite.DocCountPercent" />
		/// set to
		/// <see cref="ConstantScoreAutoRewrite.DEFAULT_DOC_COUNT_PERCENT" />
		///.
		/// Note that you cannot alter the configuration of this
		/// instance; you'll need to create a private instance
		/// instead. 
		/// </summary>
		public static readonly RewriteMethod CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;
		
		/// <summary> Constructs a query matching terms that cannot be represented with a single
		/// Term.
		/// </summary>
		protected MultiTermQuery()
		{
		}
		
		/// <summary>Construct the enumeration to be used, expanding the pattern term. </summary>
		protected internal abstract FilteredTermEnum GetEnum(IndexReader reader, IState state);

	    /// <summary> Expert: Return the number of unique terms visited during execution of the query.
	    /// If there are many of them, you may consider using another query type
	    /// or optimize your total term count in index.
	    /// <p/>This method is not thread safe, be sure to only call it when no query is running!
	    /// If you re-use the same query instance for another
	    /// search, be sure to first reset the term counter
	    /// with <see cref="ClearTotalNumberOfTerms" />.
	    /// <p/>On optimized indexes / no MultiReaders, you get the correct number of
	    /// unique terms for the whole index. Use this number to compare different queries.
	    /// For non-optimized indexes this number can also be achived in
	    /// non-constant-score mode. In constant-score mode you get the total number of
	    /// terms seeked for all segments / sub-readers.
	    /// </summary>
	    /// <seealso cref="ClearTotalNumberOfTerms">
	    /// </seealso>
	    public virtual int TotalNumberOfTerms
	    {
	        get { return numberOfTerms; }
	    }

	    /// <summary> Expert: Resets the counting of unique terms.
		/// Do this before executing the query/filter.
		/// </summary>
		/// <seealso cref="TotalNumberOfTerms">
		/// </seealso>
		public virtual void  ClearTotalNumberOfTerms()
		{
			numberOfTerms = 0;
		}
		
		protected internal virtual void  IncTotalNumberOfTerms(int inc)
		{
			numberOfTerms += inc;
		}
		
		public override Query Rewrite(IndexReader reader, IState state)
		{
			return internalRewriteMethod.Rewrite(reader, this, state);
		}
		
	    /// <summary> Sets the rewrite method to be used when executing the
	    /// query.  You can use one of the four core methods, or
	    /// implement your own subclass of <see cref="Search.RewriteMethod" />. 
	    /// </summary>
	    public virtual RewriteMethod RewriteMethod
	    {
            get { return internalRewriteMethod; }
	        set { internalRewriteMethod = value; }
	    }

	    //@Override
		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + System.Convert.ToInt32(Boost);
			result = prime * result;
			result += internalRewriteMethod.GetHashCode();
			return result;
		}
		
		//@Override
		public  override bool Equals(System.Object obj)
		{
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (GetType() != obj.GetType())
				return false;
			MultiTermQuery other = (MultiTermQuery) obj;
			if (System.Convert.ToInt32(Boost) != System.Convert.ToInt32(other.Boost))
				return false;
			if (!internalRewriteMethod.Equals(other.internalRewriteMethod))
			{
				return false;
			}
			return true;
		}
		static MultiTermQuery()
		{
			CONSTANT_SCORE_AUTO_REWRITE_DEFAULT = new AnonymousClassConstantScoreAutoRewrite();
		}
	}

    /// <summary>Abstract class that defines how the query is rewritten. </summary>

        [Serializable]
    public abstract class RewriteMethod
    {
        public abstract Query Rewrite(IndexReader reader, MultiTermQuery query, IState state);
    }
}