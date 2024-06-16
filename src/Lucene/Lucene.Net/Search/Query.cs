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
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Store;
using IndexReader = Lucene.Net.Index.IndexReader;

namespace Lucene.Net.Search
{

    /// <summary>The abstract base class for queries.
    /// <p/>Instantiable subclasses are:
    /// <list type="bullet">
    /// <item> <see cref="TermQuery" /> </item>
    /// <item> <see cref="MultiTermQuery" /> </item>
    /// <item> <see cref="BooleanQuery" /> </item>
    /// <item> <see cref="WildcardQuery" /> </item>
    /// <item> <see cref="PhraseQuery" /> </item>
    /// <item> <see cref="PrefixQuery" /> </item>
    /// <item> <see cref="MultiPhraseQuery" /> </item>
    /// <item> <see cref="FuzzyQuery" /> </item>
    /// <item> <see cref="TermRangeQuery" /> </item>
    /// <item> <see cref="NumericRangeQuery{T}" /> </item>
    /// <item> <see cref="Lucene.Net.Search.Spans.SpanQuery" /> </item>
    /// </list>
    /// <p/>A parser for queries is contained in:
    /// <list type="bullet">
    /// <item><see cref="Lucene.Net.QueryParsers.QueryParser">QueryParser</see> </item>
    /// </list>
    /// </summary>

        [Serializable]
    public abstract class Query : System.ICloneable
	{
		private float boost = 1.0f; // query boost factor

	    /// <summary>Gets or sets the boost for this query clause to <c>b</c>.  Documents
	    /// matching this clause will (in addition to the normal weightings) have
	    /// their score multiplied by <c>b</c>.  The boost is 1.0 by default.
	    /// </summary>
	    public virtual float Boost
	    {
	        get { return boost; }
	        set { boost = value; }
	    }

	    /// <summary>Prints a query to a string, with <c>field</c> assumed to be the 
		/// default field and omitted.
		/// <p/>The representation used is one that is supposed to be readable
		/// by <see cref="Lucene.Net.QueryParsers.QueryParser">QueryParser</see>. However,
		/// there are the following limitations:
		/// <list type="bullet">
		/// <item>If the query was created by the parser, the printed
		/// representation may not be exactly what was parsed. For example,
		/// characters that need to be escaped will be represented without
		/// the required backslash.</item>
		/// <item>Some of the more complicated queries (e.g. span queries)
		/// don't have a representation that can be parsed by QueryParser.</item>
		/// </list>
		/// </summary>
		public abstract System.String ToString(System.String field);
		
		/// <summary>Prints a query to a string. </summary>
		public override System.String ToString()
		{
			return ToString("");
		}
		
		/// <summary> Expert: Constructs an appropriate Weight implementation for this query.
		/// 
		/// <p/>
		/// Only implemented by primitive queries, which re-write to themselves.
		/// </summary>
		public virtual Weight CreateWeight(Searcher searcher, IState state)
		{
			throw new System.NotSupportedException();
		}
		
		/// <summary> Expert: Constructs and initializes a Weight for a top-level query.</summary>
		public virtual Weight Weight(Searcher searcher, IState state)
		{
			Query query = searcher.Rewrite(this, state);
			Weight weight = query.CreateWeight(searcher, state);
		    float sum = weight.GetSumOfSquaredWeights();
            float norm = GetSimilarity(searcher).QueryNorm(sum);
            if (float.IsInfinity(norm) || float.IsNaN(norm))
                norm = 1.0f;
			weight.Normalize(norm);
			return weight;
		}
		
		
		/// <summary>Expert: called to re-write queries into primitive queries. For example,
		/// a PrefixQuery will be rewritten into a BooleanQuery that consists
		/// of TermQuerys.
		/// </summary>
		public virtual Query Rewrite(IndexReader reader, IState state)
		{
			return this;
		}
		
		
		/// <summary>Expert: called when re-writing queries under MultiSearcher.
		/// 
		/// Create a single query suitable for use by all subsearchers (in 1-1
		/// correspondence with queries). This is an optimization of the OR of
		/// all queries. We handle the common optimization cases of equal
		/// queries and overlapping clauses of boolean OR queries (as generated
		/// by MultiTermQuery.rewrite()).
		/// Be careful overriding this method as queries[0] determines which
		/// method will be called and is not necessarily of the same type as
		/// the other queries.
		/// </summary>
		public virtual Query Combine(Query[] queries)
		{
            var uniques = new System.Collections.Generic.HashSet<Query>();
			for (int i = 0; i < queries.Length; i++)
			{
				Query query = queries[i];
				BooleanClause[] clauses = null;
				// check if we can split the query into clauses
				bool splittable = (query is BooleanQuery);
				if (splittable)
				{
					BooleanQuery bq = (BooleanQuery) query;
					splittable = bq.IsCoordDisabled();
					clauses = bq.GetClauses();
					for (int j = 0; splittable && j < clauses.Length; j++)
					{
						splittable = (clauses[j].Occur == Occur.SHOULD);
					}
				}
				if (splittable)
				{
					for (int j = 0; j < clauses.Length; j++)
					{
                        uniques.Add(clauses[j].Query);
					}
				}
				else
				{
				    uniques.Add(query);
				}
			}
			// optimization: if we have just one query, just return it
			if (uniques.Count == 1)
			{
			    return uniques.First();
			}
			BooleanQuery result = new BooleanQuery(true);
            foreach (Query key in uniques)
            {
                result.Add(key, Occur.SHOULD);
            }
			return result;
		}
		
		
		/// <summary> Expert: adds all terms occuring in this query to the terms set. Only
		/// works if this query is in its <see cref="Rewrite">rewritten</see> form.
		/// 
		/// </summary>
		/// <throws>  UnsupportedOperationException if this query is not yet rewritten </throws>
		public virtual void ExtractTerms(System.Collections.Generic.ISet<Term> terms)
		{
			// needs to be implemented by query subclasses
			throw new System.NotSupportedException();
		}
		
		
		
		/// <summary>Expert: merges the clauses of a set of BooleanQuery's into a single
		/// BooleanQuery.
		/// 
		/// <p/>A utility for use by <see cref="Combine(Query[])" /> implementations.
		/// </summary>
		public static Query MergeBooleanQueries(params BooleanQuery[] queries)
		{
            var allClauses = new System.Collections.Generic.HashSet<BooleanClause>();
			foreach (BooleanQuery booleanQuery in queries)
			{
                foreach (BooleanClause clause in booleanQuery)
                {
                    allClauses.Add(clause);
                }
			}

		    bool coordDisabled = queries.Length == 0?false:queries[0].IsCoordDisabled();
			BooleanQuery result = new BooleanQuery(coordDisabled);
			foreach(BooleanClause clause in allClauses)
			{
                result.Add(clause);
			}
			return result;
		}
		
		
		/// <summary>Expert: Returns the Similarity implementation to be used for this query.
		/// Subclasses may override this method to specify their own Similarity
		/// implementation, perhaps one that delegates through that of the Searcher.
		/// By default the Searcher's Similarity implementation is returned.
		/// </summary>
		public virtual Similarity GetSimilarity(Searcher searcher)
		{
			return searcher.Similarity;
		}
		
		/// <summary>Returns a clone of this query. </summary>
		public virtual System.Object Clone()
		{
			try
			{
				return base.MemberwiseClone();
			}
			catch (System.Exception e)
			{
				throw new System.SystemException("Clone not supported: " + e.Message);
			}
		}
		
		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + BitConverter.ToInt32(BitConverter.GetBytes(boost), 0);
			return result;
		}
		
		public  override bool Equals(System.Object obj)
		{
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (GetType() != obj.GetType())
				return false;
			Query other = (Query) obj;
			if (BitConverter.ToInt32(BitConverter.GetBytes(boost), 0) != BitConverter.ToInt32(BitConverter.GetBytes(other.boost), 0))
				return false;
			return true;
		}
	}
}