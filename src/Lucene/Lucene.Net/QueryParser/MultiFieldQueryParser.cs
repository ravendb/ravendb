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
using Lucene.Net.Search;
using Analyzer = Lucene.Net.Analysis.Analyzer;
using BooleanClause = Lucene.Net.Search.BooleanClause;
using BooleanQuery = Lucene.Net.Search.BooleanQuery;
using MultiPhraseQuery = Lucene.Net.Search.MultiPhraseQuery;
using PhraseQuery = Lucene.Net.Search.PhraseQuery;
using Query = Lucene.Net.Search.Query;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.QueryParsers
{

    /// <summary> A QueryParser which constructs queries to search multiple fields.
    /// 
    /// </summary>
    /// <version>  $Revision: 829231 $
    /// </version>
    public class MultiFieldQueryParser : QueryParser
    {
        protected internal string[] fields;
        protected internal IDictionary<string, float> boosts;

        /// <summary> Creates a MultiFieldQueryParser. Allows passing of a map with term to
        /// Boost, and the boost to apply to each term.
        /// 
        /// <p/>
        /// It will, when parse(String query) is called, construct a query like this
        /// (assuming the query consists of two terms and you specify the two fields
        /// <c>title</c> and <c>body</c>):
        /// <p/>
        /// 
        /// <code>
        /// (title:term1 body:term1) (title:term2 body:term2)
        /// </code>
        /// 
        /// <p/>
        /// When setDefaultOperator(AND_OPERATOR) is set, the result will be:
        /// <p/>
        /// 
        /// <code>
        /// +(title:term1 body:term1) +(title:term2 body:term2)
        /// </code>
        /// 
        /// <p/>
        /// When you pass a boost (title=>5 body=>10) you can get
        /// <p/>
        /// 
        /// <code>
        /// +(title:term1^5.0 body:term1^10.0) +(title:term2^5.0 body:term2^10.0)
        /// </code>
        /// 
        /// <p/>
        /// In other words, all the query's terms must appear, but it doesn't matter
        /// in what fields they appear.
        /// <p/>
        /// </summary>
        public MultiFieldQueryParser(Version matchVersion, string[] fields, Analyzer analyzer, IDictionary<string, float> boosts)
            : this(matchVersion, fields, analyzer)
        {
            this.boosts = boosts;
        }

        /// <summary> Creates a MultiFieldQueryParser.
        /// 
        /// <p/>
        /// It will, when parse(String query) is called, construct a query like this
        /// (assuming the query consists of two terms and you specify the two fields
        /// <c>title</c> and <c>body</c>):
        /// <p/>
        /// 
        /// <code>
        /// (title:term1 body:term1) (title:term2 body:term2)
        /// </code>
        /// 
        /// <p/>
        /// When setDefaultOperator(AND_OPERATOR) is set, the result will be:
        /// <p/>
        /// 
        /// <code>
        /// +(title:term1 body:term1) +(title:term2 body:term2)
        /// </code>
        /// 
        /// <p/>
        /// In other words, all the query's terms must appear, but it doesn't matter
        /// in what fields they appear.
        /// <p/>
        /// </summary>
        public MultiFieldQueryParser(Version matchVersion, System.String[] fields, Analyzer analyzer)
            : base(matchVersion, null, analyzer)
        {
            this.fields = fields;
        }

        protected internal override Query GetFieldQuery(string field, string queryText, int slop)
        {
            if (field == null)
            {
                IList<BooleanClause> clauses = new List<BooleanClause>();
                for (int i = 0; i < fields.Length; i++)
                {
                    Query q = base.GetFieldQuery(fields[i], queryText);
                    if (q != null)
                    {
                        //If the user passes a map of boosts
                        if (boosts != null)
                        {
                            //Get the boost from the map and apply them
                            Single boost = boosts[fields[i]];
                            q.Boost = boost;
                        }
                        ApplySlop(q, slop);
                        clauses.Add(new BooleanClause(q, Occur.SHOULD));
                    }
                }
                if (clauses.Count == 0)
                    // happens for stopwords
                    return null;
                return GetBooleanQuery(clauses, true);
            }
            Query q2 = base.GetFieldQuery(field, queryText);
            ApplySlop(q2, slop);
            return q2;
        }

        private void ApplySlop(Query q, int slop)
        {
            if (q is PhraseQuery)
            {
                ((PhraseQuery)q).Slop = slop;
            }
            else if (q is MultiPhraseQuery)
            {
                ((MultiPhraseQuery)q).Slop = slop;
            }
        }


        protected internal override Query GetFieldQuery(System.String field, System.String queryText)
        {
            return GetFieldQuery(field, queryText, 0);
        }


        protected internal override Query GetFuzzyQuery(System.String field, System.String termStr, float minSimilarity)
        {
            if (field == null)
            {
                IList<BooleanClause> clauses = new List<BooleanClause>();
                for (int i = 0; i < fields.Length; i++)
                {
                    clauses.Add(new BooleanClause(GetFuzzyQuery(fields[i], termStr, minSimilarity), Occur.SHOULD));
                }
                return GetBooleanQuery(clauses, true);
            }
            return base.GetFuzzyQuery(field, termStr, minSimilarity);
        }

        protected internal override Query GetPrefixQuery(System.String field, System.String termStr)
        {
            if (field == null)
            {
                IList<BooleanClause> clauses = new List<BooleanClause>();
                for (int i = 0; i < fields.Length; i++)
                {
                    clauses.Add(new BooleanClause(GetPrefixQuery(fields[i], termStr), Occur.SHOULD));
                }
                return GetBooleanQuery(clauses, true);
            }
            return base.GetPrefixQuery(field, termStr);
        }

        protected internal override Query GetWildcardQuery(System.String field, System.String termStr)
        {
            if (field == null)
            {
                IList<BooleanClause> clauses = new List<BooleanClause>();
                for (int i = 0; i < fields.Length; i++)
                {
                    clauses.Add(new BooleanClause(GetWildcardQuery(fields[i], termStr), Occur.SHOULD));
                }
                return GetBooleanQuery(clauses, true);
            }
            return base.GetWildcardQuery(field, termStr);
        }


        protected internal override Query GetRangeQuery(System.String field, System.String part1, System.String part2, bool inclusive)
        {
            if (field == null)
            {
                IList<BooleanClause> clauses = new List<BooleanClause>();
                for (int i = 0; i < fields.Length; i++)
                {
                    clauses.Add(new BooleanClause(GetRangeQuery(fields[i], part1, part2, inclusive), Occur.SHOULD));
                }
                return GetBooleanQuery(clauses, true);
            }
            return base.GetRangeQuery(field, part1, part2, inclusive);
        }

        /// <summary> Parses a query which searches on the fields specified.
        /// <p/>
        /// If x fields are specified, this effectively constructs:
        /// 
        /// <code>
        /// (field1:query1) (field2:query2) (field3:query3)...(fieldx:queryx)
        /// </code>
        /// 
        /// </summary>
        /// <param name="matchVersion">Lucene version to match; this is passed through to
        /// QueryParser.
        /// </param>
        /// <param name="queries">Queries strings to parse
        /// </param>
        /// <param name="fields">Fields to search on
        /// </param>
        /// <param name="analyzer">Analyzer to use
        /// </param>
        /// <throws>  ParseException </throws>
        /// <summary>             if query parsing fails
        /// </summary>
        /// <throws>  IllegalArgumentException </throws>
        /// <summary>             if the length of the queries array differs from the length of
        /// the fields array
        /// </summary>
        public static Query Parse(Version matchVersion, System.String[] queries, System.String[] fields, Analyzer analyzer)
        {
            if (queries.Length != fields.Length)
                throw new System.ArgumentException("queries.length != fields.length");
            BooleanQuery bQuery = new BooleanQuery();
            for (int i = 0; i < fields.Length; i++)
            {
                QueryParser qp = new QueryParser(matchVersion, fields[i], analyzer);
                Query q = qp.Parse(queries[i]);
                if (q != null && (!(q is BooleanQuery) || ((BooleanQuery)q).GetClauses().Length > 0))
                {
                    bQuery.Add(q, Occur.SHOULD);
                }
            }
            return bQuery;
        }

        /// <summary> Parses a query, searching on the fields specified. Use this if you need
        /// to specify certain fields as required, and others as prohibited.
        /// <p/>
        /// Uasge:
        /// <code>
        /// String[] fields = {&quot;filename&quot;, &quot;contents&quot;, &quot;description&quot;};
        /// BooleanClause.Occur[] flags = {BooleanClause.Occur.SHOULD,
        /// BooleanClause.Occur.MUST,
        /// BooleanClause.Occur.MUST_NOT};
        /// MultiFieldQueryParser.parse(&quot;query&quot;, fields, flags, analyzer);
        /// </code>
        /// <p/>
        /// The code above would construct a query:
        /// 
        /// <code>
        /// (filename:query) +(contents:query) -(description:query)
        /// </code>
        /// 
        /// </summary>
        /// <param name="matchVersion">Lucene version to match; this is passed through to
        /// QueryParser.
        /// </param>
        /// <param name="query">Query string to parse
        /// </param>
        /// <param name="fields">Fields to search on
        /// </param>
        /// <param name="flags">Flags describing the fields
        /// </param>
        /// <param name="analyzer">Analyzer to use
        /// </param>
        /// <throws>  ParseException </throws>
        /// <summary>             if query parsing fails
        /// </summary>
        /// <throws>  IllegalArgumentException </throws>
        /// <summary>             if the length of the fields array differs from the length of
        /// the flags array
        /// </summary>
        public static Query Parse(Version matchVersion, System.String query, System.String[] fields, Occur[] flags, Analyzer analyzer)
        {
            if (fields.Length != flags.Length)
                throw new System.ArgumentException("fields.length != flags.length");
            BooleanQuery bQuery = new BooleanQuery();
            for (int i = 0; i < fields.Length; i++)
            {
                QueryParser qp = new QueryParser(matchVersion, fields[i], analyzer);
                Query q = qp.Parse(query);
                if (q != null && (!(q is BooleanQuery) || ((BooleanQuery)q).GetClauses().Length > 0))
                {
                    bQuery.Add(q, flags[i]);
                }
            }
            return bQuery;
        }

        /// <summary> Parses a query, searching on the fields specified. Use this if you need
        /// to specify certain fields as required, and others as prohibited.
        /// <p/>
        /// Usage:
        /// <code>
        /// String[] query = {&quot;query1&quot;, &quot;query2&quot;, &quot;query3&quot;};
        /// String[] fields = {&quot;filename&quot;, &quot;contents&quot;, &quot;description&quot;};
        /// BooleanClause.Occur[] flags = {BooleanClause.Occur.SHOULD,
        /// BooleanClause.Occur.MUST,
        /// BooleanClause.Occur.MUST_NOT};
        /// MultiFieldQueryParser.parse(query, fields, flags, analyzer);
        /// </code>
        /// <p/>
        /// The code above would construct a query:
        /// 
        /// <code>
        /// (filename:query1) +(contents:query2) -(description:query3)
        /// </code>
        /// 
        /// </summary>
        /// <param name="matchVersion">Lucene version to match; this is passed through to
        /// QueryParser.
        /// </param>
        /// <param name="queries">Queries string to parse
        /// </param>
        /// <param name="fields">Fields to search on
        /// </param>
        /// <param name="flags">Flags describing the fields
        /// </param>
        /// <param name="analyzer">Analyzer to use
        /// </param>
        /// <throws>  ParseException </throws>
        /// <summary>             if query parsing fails
        /// </summary>
        /// <throws>  IllegalArgumentException </throws>
        /// <summary>             if the length of the queries, fields, and flags array differ
        /// </summary>
        public static Query Parse(Version matchVersion, System.String[] queries, System.String[] fields, Occur[] flags, Analyzer analyzer)
        {
            if (!(queries.Length == fields.Length && queries.Length == flags.Length))
                throw new System.ArgumentException("queries, fields, and flags array have have different length");
            BooleanQuery bQuery = new BooleanQuery();
            for (int i = 0; i < fields.Length; i++)
            {
                QueryParser qp = new QueryParser(matchVersion, fields[i], analyzer);
                Query q = qp.Parse(queries[i]);
                if (q != null && (!(q is BooleanQuery) || ((BooleanQuery)q).GetClauses().Length > 0))
                {
                    bQuery.Add(q, flags[i]);
                }
            }
            return bQuery;
        }
    }
}