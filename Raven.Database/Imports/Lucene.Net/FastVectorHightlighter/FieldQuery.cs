/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Text;

using Lucene.Net.Search;
using Lucene.Net.Index;
using Lucene.Net.Support.Compatibility;
using TermInfo = Lucene.Net.Search.Vectorhighlight.FieldTermStack.TermInfo;

namespace Lucene.Net.Search.Vectorhighlight
{
    public class FieldQuery
    {
        bool fieldMatch;

        // fieldMatch==true,  Map<fieldName,QueryPhraseMap>
        // fieldMatch==false, Map<null,QueryPhraseMap>
        public HashMap<String, QueryPhraseMap> rootMaps = new HashMap<String, QueryPhraseMap>();

        // fieldMatch==true,  Map<fieldName,setOfTermsInQueries>
        // fieldMatch==false, Map<null,setOfTermsInQueries>
        public HashMap<String, List<String>> termSetMap = new HashMap<String, List<String>>();

        int termOrPhraseNumber; // used for colored tag support

        public FieldQuery(Query query, bool phraseHighlight, bool fieldMatch)
        {
            this.fieldMatch = fieldMatch;
            Dictionary<Query, Query> flatQueries = new Dictionary<Query, Query>();
            flatten(query, flatQueries);
            SaveTerms(flatQueries);
            Dictionary<Query, Query> expandQueries = expand(flatQueries);

            foreach (Query flatQuery in expandQueries.Keys)
            {
                QueryPhraseMap rootMap = getRootMap(flatQuery);
                rootMap.Add(flatQuery);
                if (!phraseHighlight && flatQuery is PhraseQuery)
                {
                    PhraseQuery pq = (PhraseQuery)flatQuery;
                    if (pq.GetTerms().Length > 1)
                    {
                        foreach (Term term in pq.GetTerms())
                            rootMap.AddTerm(term.Text, flatQuery.Boost);
                    }
                }
            }
        }

        public void flatten(Query sourceQuery, Dictionary<Query, Query> flatQueries)
        {
            if (sourceQuery is BooleanQuery)
            {
                BooleanQuery bq = (BooleanQuery)sourceQuery;
                foreach (BooleanClause clause in bq.GetClauses())
                {
                    if (!clause.IsProhibited)
                        flatten(clause.Query, flatQueries);
                }
            }
            else if (sourceQuery is PrefixQuery)
            {
                if (!flatQueries.ContainsKey(sourceQuery))
                    flatQueries.Add(sourceQuery, sourceQuery);
            }
            else if (sourceQuery is DisjunctionMaxQuery)
            {
                DisjunctionMaxQuery dmq = (DisjunctionMaxQuery)sourceQuery;
                foreach (Query query in dmq)
                {
                    flatten(query, flatQueries);
                }
            }
            else if (sourceQuery is TermQuery)
            {
                if (!flatQueries.ContainsKey(sourceQuery))
                    flatQueries.Add(sourceQuery, sourceQuery);
            }
            else if (sourceQuery is PhraseQuery)
            {
                if (!flatQueries.ContainsKey(sourceQuery))
                {
                    PhraseQuery pq = (PhraseQuery)sourceQuery;
                    if (pq.GetTerms().Length > 1)
                        flatQueries.Add(pq, pq);
                    else if (pq.GetTerms().Length == 1)
                    {
                        Query q = new TermQuery(pq.GetTerms()[0]);
                        flatQueries.Add(q, q);
                    }
                }
            }
            else
            {
                // Fallback to using extracted terms
                ISet<Term> terms = SetFactory.CreateHashSet<Term>();
                try
                {
                    sourceQuery.ExtractTerms(terms);
                }
                catch (NotSupportedException)
                { // thrown by default impl
                    return; // ignore error and discard query
                }

                foreach (var term in terms)
                {
                    flatten(new TermQuery(term), flatQueries);
                }
            }
        }

        /*
         * Create expandQueries from flatQueries.
         * 
         * expandQueries := flatQueries + overlapped phrase queries
         * 
         * ex1) flatQueries={a,b,c}
         *      => expandQueries={a,b,c}
         * ex2) flatQueries={a,"b c","c d"}
         *      => expandQueries={a,"b c","c d","b c d"}
         */
        public Dictionary<Query, Query> expand(Dictionary<Query, Query> flatQueries)
        {
            Dictionary<Query, Query> expandQueries = new Dictionary<Query, Query>();
            foreach (Query query in new Dictionary<Query, Query>(flatQueries).Keys)
            {
                //Query query = i.next();
                flatQueries.Remove(query);
                expandQueries.Add(query, query);
                if (!(query is PhraseQuery)) continue;
                foreach (Query qj in flatQueries.Keys)
                {
                    if (!(qj is PhraseQuery)) continue;
                    CheckOverlap(expandQueries, (PhraseQuery)query, (PhraseQuery)qj);
                }
            }
            return expandQueries;
        }

        /*
         * Check if PhraseQuery A and B have overlapped part.
         * 
         * ex1) A="a b", B="b c" => overlap; expandQueries={"a b c"}
         * ex2) A="b c", B="a b" => overlap; expandQueries={"a b c"}
         * ex3) A="a b", B="c d" => no overlap; expandQueries={}
         */
        private void CheckOverlap(Dictionary<Query, Query> expandQueries, PhraseQuery a, PhraseQuery b)
        {
            if (a.Slop != b.Slop) return;
            Term[] ats = a.GetTerms();
            Term[] bts = b.GetTerms();
            if (fieldMatch && !ats[0].Field.Equals(bts[0].Field)) return;
            CheckOverlap(expandQueries, ats, bts, a.Slop, a.Boost);
            CheckOverlap(expandQueries, bts, ats, b.Slop, b.Boost);
        }

        /*
         * Check if src and dest have overlapped part and if it is, create PhraseQueries and add expandQueries.
         * 
         * ex1) src="a b", dest="c d"       => no overlap
         * ex2) src="a b", dest="a b c"     => no overlap
         * ex3) src="a b", dest="b c"       => overlap; expandQueries={"a b c"}
         * ex4) src="a b c", dest="b c d"   => overlap; expandQueries={"a b c d"}
         * ex5) src="a b c", dest="b c"     => no overlap
         * ex6) src="a b c", dest="b"       => no overlap
         * ex7) src="a a a a", dest="a a a" => overlap;
         *                                     expandQueries={"a a a a a","a a a a a a"}
         * ex8) src="a b c d", dest="b c"   => no overlap
         */
        private void CheckOverlap(Dictionary<Query, Query> expandQueries, Term[] src, Term[] dest, int slop, float boost)
        {
            // beginning from 1 (not 0) is safe because that the PhraseQuery has multiple terms
            // is guaranteed in flatten() method (if PhraseQuery has only one term, flatten()
            // converts PhraseQuery to TermQuery)
            for (int i = 1; i < src.Length; i++)
            {
                bool overlap = true;
                for (int j = i; j < src.Length; j++)
                {
                    if ((j - i) < dest.Length && !src[j].Text.Equals(dest[j - i].Text))
                    {
                        overlap = false;
                        break;
                    }
                }
                if (overlap && src.Length - i < dest.Length)
                {
                    PhraseQuery pq = new PhraseQuery();
                    foreach (Term srcTerm in src)
                        pq.Add(srcTerm);
                    for (int k = src.Length - i; k < dest.Length; k++)
                    {
                        pq.Add(new Term(src[0].Field, dest[k].Text));
                    }
                    pq.Slop = slop;
                    pq.Boost = boost;
                    if (!expandQueries.ContainsKey(pq))
                        expandQueries.Add(pq, pq);
                }
            }
        }

        public QueryPhraseMap getRootMap(Query query)
        {
            String key = GetKey(query);
            QueryPhraseMap map = rootMaps.Get(key);
            if (map == null)
            {
                map = new QueryPhraseMap(this);
                rootMaps.Put(key, map);
            }
            return map;
        }

        /*
         * Return 'key' string. 'key' is the field name of the Query.
         * If not fieldMatch, 'key' will be null.
         */
        private String GetKey(Query query)
        {
            if (!fieldMatch) return null;
            if (query is TermQuery)
                return ((TermQuery)query).Term.Field;

            if (query is PrefixQuery)
                return ((PrefixQuery)query).Prefix.Field;

            if (query is PhraseQuery)
            {
                PhraseQuery pq = (PhraseQuery)query;
                Term[] terms = pq.GetTerms();
                return terms[0].Field;
            }

            throw new ApplicationException("query \"" + query + "\" must be flatten first.");
        }

        /*
         * Save the set of terms in the queries to termSetMap.
         * 
         * ex1) q=name:john
         *      - fieldMatch==true
         *          termSetMap=Map<"name",Set<"john">>
         *      - fieldMatch==false
         *          termSetMap=Map<null,Set<"john">>
         *          
         * ex2) q=name:john title:manager
         *      - fieldMatch==true
         *          termSetMap=Map<"name",Set<"john">,
         *                         "title",Set<"manager">>
         *      - fieldMatch==false
         *          termSetMap=Map<null,Set<"john","manager">>
         *          
         * ex3) q=name:"john lennon"
         *      - fieldMatch==true
         *          termSetMap=Map<"name",Set<"john","lennon">>
         *      - fieldMatch==false
         *          termSetMap=Map<null,Set<"john","lennon">>
         */
        void SaveTerms(Dictionary<Query, Query> flatQueries)
        {
            foreach (Query query in flatQueries.Keys)
            {
                List<String> termSet = GetTermSet(query);
                if (query is TermQuery)
                    termSet.Add(((TermQuery)query).Term.Text);
                else if (query is PrefixQuery)
                    termSet.Add(((PrefixQuery)query).Prefix.Text + "*");
                else if (query is PhraseQuery)
                {
                    foreach (Term term in ((PhraseQuery)query).GetTerms())
                        termSet.Add(term.Text);
                }
                else
                    throw new System.ApplicationException("query \"" + query.ToString() + "\" must be flatten first.");
            }
        }

        private List<String> GetTermSet(Query query)
        {
            String key = GetKey(query);
            List<String> set = termSetMap.Get(key);
            if (set == null)
            {
                set = new List<String>();
                termSetMap.Put(key, set);
            }
            return set;
        }

        public List<String> getTermSet(String field)
        {
            return termSetMap.Get(fieldMatch ? field : null);
        }

        /*
         * 
         * <param name="fieldName"></param>
         * <param name="term"></param>
         * <returns>QueryPhraseMap</returns>
         */
        public QueryPhraseMap   GetFieldTermMap(String fieldName, String term)
        {
            QueryPhraseMap rootMap = GetRootMap(fieldName);
            return rootMap == null ? null : RetrieveQueryFromSubMap(rootMap, term);
        }

        public QueryPhraseMap RetrieveQueryFromSubMap(QueryPhraseMap rootMap, String term)
        {
            foreach (var kvp in rootMap.subMap)
            {
                if (StringUtils.TermStringMatch(kvp.Key, term))
                    return kvp.Value;
            }
            return null;
        }

        /*
         * 
         * <param name="fieldName"></param>
         * <param name="phraseCandidate"></param>
         * <returns>QueryPhraseMap</returns>
         */
        public QueryPhraseMap SearchPhrase(String fieldName, List<TermInfo> phraseCandidate)
        {
            QueryPhraseMap root = GetRootMap(fieldName);
            if (root == null) return null;
            return root.SearchPhrase(phraseCandidate);
        }

        private QueryPhraseMap GetRootMap(String fieldName)
        {
            return rootMaps.Get(fieldMatch ? fieldName : null);
        }

        int NextTermOrPhraseNumber()
        {
            return termOrPhraseNumber++;
        }

        public class QueryPhraseMap
        {

            public bool terminal;
            int slop;   // valid if terminal == true and phraseHighlight == true
            public float boost;  // valid if terminal == true
            int termOrPhraseNumber;   // valid if terminal == true
            FieldQuery fieldQuery;
            public HashMap<String, QueryPhraseMap> subMap = new HashMap<String, QueryPhraseMap>();

            public QueryPhraseMap(FieldQuery fieldQuery)
            {
                this.fieldQuery = fieldQuery;
            }

            public void AddTerm(String termText, float boost)
            {
                QueryPhraseMap map = GetOrNewMap(subMap, termText);
                map.MarkTerminal(boost);
            }

            private QueryPhraseMap GetOrNewMap(HashMap<String, QueryPhraseMap> subMap, String term)
            {
                QueryPhraseMap map = subMap.Get(term);
                if (map == null)
                {
                    map = new QueryPhraseMap(fieldQuery);
                    subMap.Put(term, map);
                }
                return map;
            }

            public void Add(Query query)
            {
                if (query is TermQuery)
                {
                    AddTerm(((TermQuery)query).Term.Text, query.Boost);
                }
                else if (query is PrefixQuery)
                {
                    AddTerm(((PrefixQuery)query).Prefix.Text + "*", query.Boost);
                }
                else if (query is PhraseQuery)
                {
                    PhraseQuery pq = (PhraseQuery)query;
                    Term[] terms = pq.GetTerms();
                    HashMap<String, QueryPhraseMap> map = subMap;
                    QueryPhraseMap qpm = null;
                    foreach (Term term in terms)
                    {
                        qpm = GetOrNewMap(map, term.Text);
                        map = qpm.subMap;
                    }
                    qpm.MarkTerminal(pq.Slop, pq.Boost);
                }
                else
                    throw new ApplicationException("query \"" + query.ToString() + "\" must be flatten first.");
            }

            public QueryPhraseMap GetTermMap(String term)
            {
                return subMap.Get(term);
            }

            private void MarkTerminal(float boost)
            {
                MarkTerminal(0, boost);
            }

            private void MarkTerminal(int slop, float boost)
            {
                this.terminal = true;
                this.slop = slop;
                this.boost = boost;
                this.termOrPhraseNumber = fieldQuery.NextTermOrPhraseNumber();
            }

            public bool IsTerminal
            {
                get { return terminal; }
            }

            public int Slop
            {
                get { return slop; }
            }

            public float Boost
            {
                get { return boost; }
            }

            public int TermOrPhraseNumber
            {
                get { return termOrPhraseNumber; }
            }

            public QueryPhraseMap SearchPhrase(List<TermInfo> phraseCandidate)
            {
                QueryPhraseMap currMap = this;
                foreach (TermInfo ti in phraseCandidate)
                {
                    currMap = currMap.subMap[ti.Text];
                    if (currMap == null) return null;
                }
                return currMap.IsValidTermOrPhrase(phraseCandidate) ? currMap : null;
            }

            public bool IsValidTermOrPhrase(List<TermInfo> phraseCandidate)
            {
                // check terminal
                if (!terminal) return false;

                // if the candidate is a term, it is valid
                if (phraseCandidate.Count == 1) return true;

                // else check whether the candidate is valid phrase
                // compare position-gaps between terms to slop

                int pos = phraseCandidate[0].Position;
                for (int i = 1; i < phraseCandidate.Count; i++)
                {
                    int nextPos = phraseCandidate[i].Position;
                    if (Math.Abs(nextPos - pos - 1) > slop) return false;
                    pos = nextPos;
                }
                return true;
            }
        }
    }
}
