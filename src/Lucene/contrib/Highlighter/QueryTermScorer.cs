/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Support;

namespace Lucene.Net.Search.Highlight
{
    /*
 * {@link Scorer} implementation which scores text fragments by the number of
 * unique query terms found. This class uses the {@link QueryTermExtractor}
 * class to process determine the query terms and their boosts to be used.
 */
    // TODO: provide option to boost score of fragments near beginning of document
    // based on fragment.getFragNum()
    public class QueryTermScorer : IScorer
    {
        private TextFragment currentTextFragment = null;
        private HashSet<String> uniqueTermsInFragment;

        private float totalScore = 0;
        private float maxTermWeight = 0;
        private HashMap<String, WeightedTerm> termsToFind;

        private ITermAttribute termAtt;

        /*
         * 
         * @param query a Lucene query (ideally rewritten using query.rewrite before
         *        being passed to this class and the searcher)
         */

        public QueryTermScorer(Query query)
            : this(QueryTermExtractor.GetTerms(query))
        {
        }

        /*
         * 
         * @param query a Lucene query (ideally rewritten using query.rewrite before
         *        being passed to this class and the searcher)
         * @param fieldName the Field name which is used to match Query terms
         */

        public QueryTermScorer(Query query, String fieldName)
            : this(QueryTermExtractor.GetTerms(query, false, fieldName))
        {
        }

        /*
         * 
         * @param query a Lucene query (ideally rewritten using query.rewrite before
         *        being passed to this class and the searcher)
         * @param reader used to compute IDF which can be used to a) score selected
         *        fragments better b) use graded highlights eg set font color
         *        intensity
         * @param fieldName the field on which Inverse Document Frequency (IDF)
         *        calculations are based
         */

        public QueryTermScorer(Query query, IndexReader reader, String fieldName)
            : this(QueryTermExtractor.GetIdfWeightedTerms(query, reader, fieldName))
        {
        }

        public QueryTermScorer(WeightedTerm[] weightedTerms)
        {
            termsToFind = new HashMap<String, WeightedTerm>();
            for (int i = 0; i < weightedTerms.Length; i++)
            {
                WeightedTerm existingTerm = termsToFind[weightedTerms[i].Term];
                if ((existingTerm == null)
                    || (existingTerm.Weight < weightedTerms[i].Weight))
                {
                    // if a term is defined more than once, always use the highest scoring
                    // Weight
                    termsToFind[weightedTerms[i].Term] = weightedTerms[i];
                    maxTermWeight = Math.Max(maxTermWeight, weightedTerms[i].Weight);
                }
            }
        }

        /* (non-Javadoc)
         * @see org.apache.lucene.search.highlight.Scorer#init(org.apache.lucene.analysis.TokenStream)
         */

        public TokenStream Init(TokenStream tokenStream)
        {
            termAtt = tokenStream.AddAttribute<ITermAttribute>();
            return null;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.lucene.search.highlight.FragmentScorer#startFragment(org.apache
         * .lucene.search.highlight.TextFragment)
         */

        public void StartFragment(TextFragment newFragment)
        {
            uniqueTermsInFragment = new HashSet<String>();
            currentTextFragment = newFragment;
            totalScore = 0;

        }


        /* (non-Javadoc)
         * @see org.apache.lucene.search.highlight.Scorer#getTokenScore()
         */

        public float GetTokenScore()
        {
            String termText = termAtt.Term;

            WeightedTerm queryTerm = termsToFind[termText];
            if (queryTerm == null)
            {
                // not a query term - return
                return 0;
            }
            // found a query term - is it unique in this doc?
            if (!uniqueTermsInFragment.Contains(termText))
            {
                totalScore += queryTerm.Weight;
                uniqueTermsInFragment.Add(termText);
            }
            return queryTerm.Weight;
        }


        /* (non-Javadoc)
         * @see org.apache.lucene.search.highlight.Scorer#getFragmentScore()
         */

        public float FragmentScore
        {
            get { return totalScore; }
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.lucene.search.highlight.FragmentScorer#allFragmentsProcessed()
         */

        public void AllFragmentsProcessed()
        {
            // this class has no special operations to perform at end of processing
        }

        /*
         * 
         * @return The highest weighted term (useful for passing to GradientFormatter
         *         to set top end of coloring scale.
         */

        public float MaxTermWeight
        {
            get { return maxTermWeight; }
        }
    }
}
