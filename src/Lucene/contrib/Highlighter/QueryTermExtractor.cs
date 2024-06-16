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
using System.IO;
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Util;

namespace Lucene.Net.Search.Highlight
{
	
	/// <summary> Utility class used to extract the terms used in a query, plus any weights.
	/// This class will not find terms for MultiTermQuery, RangeQuery and PrefixQuery classes
	/// so the caller must pass a rewritten query (see Query.rewrite) to obtain a list of 
	/// expanded terms.</summary>
	public static class QueryTermExtractor
	{
		
		/// <summary> Extracts all terms texts of a given Query into an array of WeightedTerms
		/// 
		/// </summary>
		/// <param name="query">     Query to extract term texts from
		/// </param>
		/// <returns> an array of the terms used in a query, plus their weights.
		/// </returns>
		public static WeightedTerm[] GetTerms(Query query)
		{
			return GetTerms(query, false);
		}
		
		/// <summary> Extracts all terms texts of a given Query into an array of WeightedTerms
		/// 
		/// </summary>
		/// <param name="query">Query to extract term texts from</param>
		/// <param name="reader">used to compute IDF which can be used to a) score selected fragments better 
		/// b) use graded highlights eg chaning intensity of font color</param>
		/// <param name="fieldName">the field on which Inverse Document Frequency (IDF) calculations are based</param>
		/// <returns> an array of the terms used in a query, plus their weights.</returns>
		public static WeightedTerm[] GetIdfWeightedTerms(Query query, IndexReader reader, string fieldName)
		{
            WeightedTerm[] terms = GetTerms(query, false, fieldName);
            int totalNumDocs = reader.NumDocs();
            foreach (WeightedTerm t in terms)
            {
                try
                {
                    int docFreq = reader.DocFreq(new Term(fieldName, t.Term));
                    // docFreq counts deletes
                    if (totalNumDocs < docFreq)
                    {
                        docFreq = totalNumDocs;
                    }
                    //IDF algorithm taken from DefaultSimilarity class
                    var idf = (float)(Math.Log((float)totalNumDocs / (double)(docFreq + 1)) + 1.0);
                    t.Weight *= idf;
                }
                catch (IOException e)
                {
                    //ignore 
                }
            }
		    return terms;
		}
		
		/// <summary>Extracts all terms texts of a given Query into an array of WeightedTerms</summary>
		/// <param name="query">Query to extract term texts from</param>
		/// <param name="prohibited"><c>true</c> to extract "prohibited" terms, too </param>
		/// <param name="fieldName"> The fieldName used to filter query terms</param>
		/// <returns>an array of the terms used in a query, plus their weights.</returns>
		public static WeightedTerm[] GetTerms(Query query, bool prohibited, string fieldName)
		{
            var terms = new HashSet<WeightedTerm>();
            if (fieldName != null)
            {
                fieldName = StringHelper.Intern(fieldName);
            }
            GetTerms(query, terms, prohibited, fieldName);
            return terms.ToArray();
		}
		
		/// <summary> Extracts all terms texts of a given Query into an array of WeightedTerms
		/// 
		/// </summary>
		/// <param name="query">     Query to extract term texts from
		/// </param>
		/// <param name="prohibited"><c>true</c> to extract "prohibited" terms, too
		/// </param>
		/// <returns> an array of the terms used in a query, plus their weights.
		/// </returns>
		public static WeightedTerm[] GetTerms(Query query, bool prohibited)
		{
			return GetTerms(query, prohibited, null);
		}
		
		//fieldname MUST be interned prior to this call
        private static void GetTerms(Query query, HashSet<WeightedTerm> terms, bool prohibited, string fieldName)
        {
            try
            {
                if (query is BooleanQuery)
                    GetTermsFromBooleanQuery((BooleanQuery) query, terms, prohibited, fieldName);
                else if (query is FilteredQuery)
                    GetTermsFromFilteredQuery((FilteredQuery) query, terms, prohibited, fieldName);
                else
                {
                    var nonWeightedTerms = Support.Compatibility.SetFactory.CreateHashSet<Term>();
                    query.ExtractTerms(nonWeightedTerms);
                    foreach (var term in nonWeightedTerms)
                    {
                        if ((fieldName == null) || (term.Field == fieldName))
                        {
                            terms.Add(new WeightedTerm(query.Boost, term.Text));
                        }
                    }
                }
            }
            catch (System.NotSupportedException ignore)
            {
                //this is non-fatal for our purposes
            }
        }

	    /// <summary> extractTerms is currently the only query-independent means of introspecting queries but it only reveals
		/// a list of terms for that query - not the boosts each individual term in that query may or may not have.
		/// "Container" queries such as BooleanQuery should be unwrapped to get at the boost info held
		/// in each child element. 
		/// Some discussion around this topic here:
		/// http://www.gossamer-threads.com/lists/lucene/java-dev/34208?search_string=introspection;#34208
		/// Unfortunately there seemed to be limited interest in requiring all Query objects to implement
		/// something common which would allow access to child queries so what follows here are query-specific
		/// implementations for accessing embedded query elements. 
		/// </summary>
		private static void  GetTermsFromBooleanQuery(BooleanQuery query, HashSet<WeightedTerm> terms, bool prohibited, string fieldName)
		{
            BooleanClause[] queryClauses = query.GetClauses();
            for (int i = 0; i < queryClauses.Length; i++)
            {
                if (prohibited || queryClauses[i].Occur != Occur.MUST_NOT)
                    GetTerms(queryClauses[i].Query, terms, prohibited, fieldName);
            }
		}
        private static void GetTermsFromFilteredQuery(FilteredQuery query, HashSet<WeightedTerm> terms, bool prohibited, string fieldName)
		{
			GetTerms(query.Query, terms, prohibited, fieldName);
		}
	}
}
