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
using System.Linq;
using System.Text;

using Lucene.Net.Search;
using Lucene.Net.Analysis;
using Lucene.Net.Index;

namespace Lucene.Net.Search.Similar
{
    /*<summary>
 * A simple wrapper for MoreLikeThis for use in scenarios where a Query object is required eg
 * in custom QueryParser extensions. At query.rewrite() time the reader is used to construct the
 * actual MoreLikeThis object and obtain the real Query object.
     * </summary>
 */
    public class MoreLikeThisQuery : Query
    {
        private String likeText;
        private String[] moreLikeFields;
        private Analyzer analyzer;
        float percentTermsToMatch = 0.3f;
        int minTermFrequency = 1;
        int maxQueryTerms = 5;
        ISet<string> stopWords = null;
        int minDocFreq = -1;


        /*<summary></summary>
         * <param name="moreLikeFields"></param>
         * <param name="likeText"></param>
         * <param name="analyzer"></param>
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
            mlt.Analyzer = analyzer;
            mlt.MinTermFreq = minTermFrequency;
            if (minDocFreq >= 0)
            {
                mlt.MinDocFreq = minDocFreq;
            }
            mlt.MaxQueryTerms = maxQueryTerms;
            mlt.SetStopWords(stopWords);
            BooleanQuery bq = (BooleanQuery)mlt.Like( new System.IO.StringReader(likeText));
            BooleanClause[] clauses = bq.GetClauses();
            //make at least half the terms match
            bq.MinimumNumberShouldMatch = (int)(clauses.Length * percentTermsToMatch);
            return bq;
        }
        /* (non-Javadoc)
         * <see cref="org.apache.lucene.search.Query.toString(java.lang.String)"/>
         */
        public override String ToString(String field)
        {
            return "like:" + likeText;
        }

        public float PercentTermsToMatch
        {
            get { return percentTermsToMatch; }
            set { this.percentTermsToMatch = value; }
        }

        public Analyzer Analyzer
        {
            get { return analyzer; }
            set { this.analyzer = value; }
        }

        public string LikeText
        {
            get { return likeText; }
            set { this.likeText = value; }
        }

        public int MaxQueryTerms
        {
            get { return maxQueryTerms; }
            set { this.maxQueryTerms = value; }
        }

        public int MinTermFrequency
        {
            get { return minTermFrequency; }
            set { this.minTermFrequency = value; }
        }

        public String[] GetMoreLikeFields()
        {
            return moreLikeFields;
        }

        public void SetMoreLikeFields(String[] moreLikeFields)
        {
            this.moreLikeFields = moreLikeFields;
        }
        public ISet<string> GetStopWords()
        {
            return stopWords;
        }
        public void SetStopWords(ISet<string> stopWords)
        {
            this.stopWords = stopWords;
        }

        public int MinDocFreq
        {
            get { return minDocFreq; }
            set { this.minDocFreq = value; }
        }
    }
}
