/**
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
using System.Linq;
using System.Text;

using Lucene.Net.Search;
using Lucene.Net.Analysis;
using Lucene.Net.Index;

namespace Lucene.Net.Search.Similar
{
    /**
 * A simple wrapper for MoreLikeThis for use in scenarios where a Query object is required eg
 * in custom QueryParser extensions. At query.rewrite() time the reader is used to construct the
 * actual MoreLikeThis object and obtain the real Query object.
 */
    public class MoreLikeThisQuery : Query
    {


        private String likeText;
        private String[] moreLikeFields;
        private Analyzer analyzer;
        float percentTermsToMatch = 0.3f;
        int minTermFrequency = 1;
        int maxQueryTerms = 5;
        System.Collections.Hashtable stopWords = null;
        int minDocFreq = -1;


        /**
         * @param moreLikeFields
         */
        public MoreLikeThisQuery(String likeText, String[] moreLikeFields, Analyzer analyzer)
        {
            this.likeText = likeText;
            this.moreLikeFields = moreLikeFields;
            this.analyzer = analyzer;
        }

        public override Query Rewrite(IndexReader reader)
        {
            MoreLikeThis mlt = new MoreLikeThis(reader);

            mlt.SetFieldNames(moreLikeFields);
            mlt.SetAnalyzer(analyzer);
            mlt.SetMinTermFreq(minTermFrequency);
            if (minDocFreq >= 0)
            {
                mlt.SetMinDocFreq(minDocFreq);
            }
            mlt.SetMaxQueryTerms(maxQueryTerms);
            mlt.SetStopWords(stopWords);
            BooleanQuery bq = (BooleanQuery)mlt.Like( new System.IO.StringReader(likeText));
            BooleanClause[] clauses = bq.GetClauses();
            //make at least half the terms match
            bq.SetMinimumNumberShouldMatch((int)(clauses.Length * percentTermsToMatch));
            return bq;
        }
        /* (non-Javadoc)
         * @see org.apache.lucene.search.Query#toString(java.lang.String)
         */
        public override String ToString(String field)
        {
            return "like:" + likeText;
        }

        public float GetPercentTermsToMatch()
        {
            return percentTermsToMatch;
        }
        public void SetPercentTermsToMatch(float percentTermsToMatch)
        {
            this.percentTermsToMatch = percentTermsToMatch;
        }

        public  Analyzer GetAnalyzer()
        {
            return analyzer;
        }

        public void SetAnalyzer(Analyzer analyzer)
        {
            this.analyzer = analyzer;
        }

        public String GetLikeText()
        {
            return likeText;
        }

        public void SetLikeText(String likeText)
        {
            this.likeText = likeText;
        }

        public int GetMaxQueryTerms()
        {
            return maxQueryTerms;
        }

        public void SetMaxQueryTerms(int maxQueryTerms)
        {
            this.maxQueryTerms = maxQueryTerms;
        }

        public int GetMinTermFrequency()
        {
            return minTermFrequency;
        }

        public void SetMinTermFrequency(int minTermFrequency)
        {
            this.minTermFrequency = minTermFrequency;
        }

        public String[] GetMoreLikeFields()
        {
            return moreLikeFields;
        }

        public void SetMoreLikeFields(String[] moreLikeFields)
        {
            this.moreLikeFields = moreLikeFields;
        }
        public System.Collections.Hashtable GetStopWords()
        {
            return stopWords;
        }
        public void SetStopWords(System.Collections.Hashtable stopWords)
        {
            this.stopWords = stopWords;
        }

        public int GetMinDocFreq()
        {
            return minDocFreq;
        }

        public void SetMinDocFreq(int minDocFreq)
        {
            this.minDocFreq = minDocFreq;
        }
    }
}
