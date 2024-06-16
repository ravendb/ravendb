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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Lucene.Net.Search;
using Lucene.Net.Index;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Support;
using Lucene.Net.Util;

namespace Lucene.Net.Search
{
    /// <summary>
    /// Fuzzifies ALL terms provided as strings and then picks the best n differentiating terms.
    /// In effect this mixes the behaviour of FuzzyQuery and MoreLikeThis but with special consideration
    /// of fuzzy scoring factors.
    /// This generally produces good results for queries where users may provide details in a number of 
    /// fields and have no knowledge of boolean query syntax and also want a degree of fuzzy matching and
    /// a fast query.
    /// 
    /// For each source term the fuzzy variants are held in a BooleanQuery with no coord factor (because
    /// we are not looking for matches on multiple variants in any one doc). Additionally, a specialized
    /// TermQuery is used for variants and does not use that variant term's IDF because this would favour rarer 
    /// terms eg misspellings. Instead, all variants use the same IDF ranking (the one for the source query 
    /// term) and this is factored into the variant's boost. If the source query term does not exist in the
    /// index the average IDF of the variants is used.
    /// </summary>
    public class FuzzyLikeThisQuery : Query
    {
        static Similarity sim = new DefaultSimilarity();
        Query rewrittenQuery = null;
        EquatableList<FieldVals> fieldVals = new EquatableList<FieldVals>();
        Analyzer analyzer;

        ScoreTermQueue q;
        int MAX_VARIANTS_PER_TERM = 50;
        bool ignoreTF = false;
        private int maxNumTerms;

        public override int GetHashCode()
        {
            int prime = 31;
            int result = 1;
            result = prime * result + ((analyzer == null) ? 0 : analyzer.GetHashCode());
            result = prime * result
                + ((fieldVals == null) ? 0 : fieldVals.GetHashCode());
            result = prime * result + (ignoreTF ? 1231 : 1237);
            result = prime * result + maxNumTerms;
            return result;
        }

        public override bool Equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (GetType() != obj.GetType())
                return false;
            FuzzyLikeThisQuery other = (FuzzyLikeThisQuery)obj;
            if (analyzer == null)
            {
                if (other.analyzer != null)
                    return false;
            }
            else if (!analyzer.Equals(other.analyzer))
                return false;
            if (fieldVals == null)
            {
                if (other.fieldVals != null)
                    return false;
            }
            else if (!fieldVals.Equals(other.fieldVals))
                return false;
            if (ignoreTF != other.ignoreTF)
                return false;
            if (maxNumTerms != other.maxNumTerms)
                return false;
            return true;
        }


        /*
         * 
         * <param name="maxNumTerms">The total number of terms clauses that will appear once rewritten as a BooleanQuery</param>
         * <param name="analyzer"></param>
         */
        public FuzzyLikeThisQuery(int maxNumTerms, Analyzer analyzer)
        {
            q = new ScoreTermQueue(maxNumTerms);
            this.analyzer = analyzer;
            this.maxNumTerms = maxNumTerms;
        }

        class FieldVals
        {
            internal String queryString;
            internal String fieldName;
            internal float minSimilarity;
            internal int prefixLength;
            public FieldVals(String name, float similarity, int length, String queryString)
            {
                fieldName = name;
                minSimilarity = similarity;
                prefixLength = length;
                this.queryString = queryString;
            }

            public override int GetHashCode()
            {
                int prime = 31;
                int result = 1;
                result = prime * result
                    + ((fieldName == null) ? 0 : fieldName.GetHashCode());
                result = prime * result + BitConverter.ToInt32(BitConverter.GetBytes(minSimilarity),0);
                result = prime * result + prefixLength;
                result = prime * result
                    + ((queryString == null) ? 0 : queryString.GetHashCode());
                return result;
            }

            public override bool Equals(Object obj)
            {
                if (this == obj)
                    return true;
                if (obj == null)
                    return false;
                if (GetType() != obj.GetType())
                    return false;
                FieldVals other = (FieldVals)obj;
                if (fieldName == null)
                {
                    if (other.fieldName != null)
                        return false;
                }
                else if (!fieldName.Equals(other.fieldName))
                    return false;
                if (BitConverter.ToInt32(BitConverter.GetBytes(minSimilarity), 0) != BitConverter.ToInt32(BitConverter.GetBytes(other.minSimilarity), 0))
                //if (Float.floatToIntBits(minSimilarity) != Float.floatToIntBits(other.minSimilarity))
                    return false;
                if (prefixLength != other.prefixLength)
                    return false;
                if (queryString == null)
                {
                    if (other.queryString != null)
                        return false;
                }
                else if (!queryString.Equals(other.queryString))
                    return false;
                return true;
            }



        }

        /*
         * <summary>Adds user input for "fuzzification" </summary>
         * <param name="queryString">The string which will be parsed by the analyzer and for which fuzzy variants will be parsed</param>
         * <param name="fieldName"></param>
         * <param name="minSimilarity">The minimum similarity of the term variants (see FuzzyTermEnum)</param>
         * <param name="prefixLength">Length of required common prefix on variant terms (see FuzzyTermEnum)</param>
         */
        public void AddTerms(String queryString, String fieldName, float minSimilarity, int prefixLength)
        {
            fieldVals.Add(new FieldVals(fieldName, minSimilarity, prefixLength, queryString));
        }


        private void AddTerms(IndexReader reader, FieldVals f)
        {
            if (f.queryString == null) return;
            TokenStream ts = analyzer.TokenStream(f.fieldName, new System.IO.StringReader(f.queryString));
            ITermAttribute termAtt = ts.AddAttribute<ITermAttribute>();

            int corpusNumDocs = reader.NumDocs();
            Term internSavingTemplateTerm = new Term(f.fieldName); //optimization to avoid constructing new Term() objects
            HashSet<string> processedTerms = new HashSet<string>();
            while (ts.IncrementToken())
            {
                String term = termAtt.Term;
                if (!processedTerms.Contains(term))
                {
                    processedTerms.Add(term);
                    ScoreTermQueue variantsQ = new ScoreTermQueue(MAX_VARIANTS_PER_TERM); //maxNum variants considered for any one term
                    float minScore = 0;
                    Term startTerm = internSavingTemplateTerm.CreateTerm(term);
                    FuzzyTermEnum fe = new FuzzyTermEnum(reader, startTerm, f.minSimilarity, f.prefixLength);
                    TermEnum origEnum = reader.Terms(startTerm);
                    int df = 0;
                    if (startTerm.Equals(origEnum.Term))
                    {
                        df = origEnum.DocFreq(); //store the df so all variants use same idf
                    }
                    int numVariants = 0;
                    int totalVariantDocFreqs = 0;
                    do
                    {
                        Term possibleMatch = fe.Term;
                        if (possibleMatch != null)
                        {
                            numVariants++;
                            totalVariantDocFreqs += fe.DocFreq();
                            float score = fe.Difference();
                            if (variantsQ.Size() < MAX_VARIANTS_PER_TERM || score > minScore)
                            {
                                ScoreTerm st = new ScoreTerm(possibleMatch, score, startTerm);
                                variantsQ.InsertWithOverflow(st);
                                minScore = variantsQ.Top().Score; // maintain minScore
                            }
                        }
                    }
                    while (fe.Next());
                    if (numVariants > 0)
                    {
                        int avgDf = totalVariantDocFreqs / numVariants;
                        if (df == 0)//no direct match we can use as df for all variants 
                        {
                            df = avgDf; //use avg df of all variants
                        }

                        // take the top variants (scored by edit distance) and reset the score
                        // to include an IDF factor then add to the global queue for ranking 
                        // overall top query terms
                        int size = variantsQ.Size();
                        for (int i = 0; i < size; i++)
                        {
                            ScoreTerm st = variantsQ.Pop();
                            st.Score = (st.Score * st.Score) * sim.Idf(df, corpusNumDocs);
                            q.InsertWithOverflow(st);
                        }
                    }
                }
            }
        }

        public override Query Rewrite(IndexReader reader)
        {
            if (rewrittenQuery != null)
            {
                return rewrittenQuery;
            }
            //load up the list of possible terms
            foreach (FieldVals f in fieldVals)
            {
                AddTerms(reader, f);
            }
            //clear the list of fields
            fieldVals.Clear();

            BooleanQuery bq = new BooleanQuery();


            //create BooleanQueries to hold the variants for each token/field pair and ensure it
            // has no coord factor
            //Step 1: sort the termqueries by term/field
            HashMap<Term, List<ScoreTerm>> variantQueries = new HashMap<Term, List<ScoreTerm>>();
            int size = q.Size();
            for (int i = 0; i < size; i++)
            {
                ScoreTerm st = q.Pop();
                var l = variantQueries[st.fuzziedSourceTerm];
                if (l == null)
                {
                    l = new List<ScoreTerm>();
                    variantQueries.Add(st.fuzziedSourceTerm, l);
                }
                l.Add(st);
            }
            //Step 2: Organize the sorted termqueries into zero-coord scoring boolean queries
            foreach(var variants in variantQueries.Values)
            {
                if (variants.Count == 1)
                {
                    //optimize where only one selected variant
                    ScoreTerm st = variants[0];
                    TermQuery tq = new FuzzyTermQuery(st.Term, ignoreTF);
                    tq.Boost = st.Score; // set the boost to a mix of IDF and score
                    bq.Add(tq, Occur.SHOULD);
                }
                else
                {
                    BooleanQuery termVariants = new BooleanQuery(true); //disable coord and IDF for these term variants
                    foreach(ScoreTerm st in variants)
                    {
                        TermQuery tq = new FuzzyTermQuery(st.Term, ignoreTF);      // found a match
                        tq.Boost = st.Score; // set the boost using the ScoreTerm's score
                        termVariants.Add(tq, Occur.SHOULD);          // add to query                    
                    }
                    bq.Add(termVariants, Occur.SHOULD);          // add to query
                }
            }
            //TODO possible alternative step 3 - organize above booleans into a new layer of field-based
            // booleans with a minimum-should-match of NumFields-1?
            bq.Boost = Boost;
            this.rewrittenQuery = bq;
            return bq;
        }

        //Holds info for a fuzzy term variant - initially score is set to edit distance (for ranking best
        // term variants) then is reset with IDF for use in ranking against all other
        // terms/fields
        private class ScoreTerm
        {
            public Term Term { get; set; }
            public float Score { get; set; }

            internal Term fuzziedSourceTerm;

            public ScoreTerm(Term term, float score, Term fuzziedSourceTerm)
            {
                this.Term = term;
                this.Score = score;
                this.fuzziedSourceTerm = fuzziedSourceTerm;
            }
        }

        private class ScoreTermQueue : PriorityQueue<ScoreTerm>
        {
            public ScoreTermQueue(int size)
            {
                Initialize(size);
            }

            /* (non-Javadoc)
             * <see cref="org.apache.lucene.util.PriorityQueue.lessThan(java.lang.Object, java.lang.Object)"/>
             */
            public override bool LessThan(ScoreTerm termA, ScoreTerm termB)
            {
                if (termA.Score == termB.Score)
                    return termA.Term.CompareTo(termB.Term) > 0;
                else
                    return termA.Score < termB.Score;
            }

        }

        //overrides basic TermQuery to negate effects of IDF (idf is factored into boost of containing BooleanQuery)
        private class FuzzyTermQuery : TermQuery
        {
            bool ignoreTF;
            
            public FuzzyTermQuery(Term t, bool ignoreTF): base(t)
            {
                this.ignoreTF = ignoreTF;
            }

            public override Similarity GetSimilarity(Searcher searcher)
            {
                Similarity result = base.GetSimilarity(searcher);
                result = new AnonymousSimilarityDelegator(this,result);
                return result;
            }

            class AnonymousSimilarityDelegator : SimilarityDelegator
            {
                FuzzyTermQuery parent = null;
                public AnonymousSimilarityDelegator(FuzzyTermQuery parent,Similarity result) : base(result)
                {
                    this.parent = parent;
                }

                public override float Tf(float freq)
                {
                    if (parent.ignoreTF)
                    {
                        return 1; //ignore tf
                    }
                    return base.Tf(freq);
                }

                public override float Idf(int docFreq, int numDocs)
                {
                    //IDF is already factored into individual term boosts
                    return 1;
                }
            }
        }


        /* (non-Javadoc)
         * <see cref="org.apache.lucene.search.Query.toString(java.lang.String)"/>
         */
        public override String ToString(String field)
        {
            return null;
        }


        public bool IsIgnoreTF()
        {
            return ignoreTF;
        }


        public void SetIgnoreTF(bool ignoreTF)
        {
            this.ignoreTF = ignoreTF;
        }

    }
}
