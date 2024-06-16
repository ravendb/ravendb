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
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Index.Memory;
using Lucene.Net.Search.Spans;
using Lucene.Net.Support;
using Lucene.Net.Util;

namespace Lucene.Net.Search.Highlight
{

    ///<summary>
    /// <see cref="IScorer"/> implementation which scores text fragments by the number of
    /// unique query terms found. This class converts appropriate <see cref="Query"/>s to
    /// <see cref="SpanQuery"/>s and attempts to score only those terms that participated in
    /// generating the 'hit' on the document.
    /// </summary>
    public class QueryScorer : IScorer
    {
        private float totalScore;
        private ISet<String> foundTerms;
        private IDictionary<String, WeightedSpanTerm> fieldWeightedSpanTerms;
        private float maxTermWeight;
        private int position = -1;
        private String defaultField;
        private ITermAttribute termAtt;
        private IPositionIncrementAttribute posIncAtt;
        private bool expandMultiTermQuery = true;
        private Query query;
        private String field;
        private IndexReader reader;
        private bool skipInitExtractor;
        private bool wrapToCaching = true;

        /// <summary>
        /// Constructs a new QueryScorer instance
        /// </summary>
        /// <param name="query">Query to use for highlighting</param>
        public QueryScorer(Query query)
        {
            Init(query, null, null, true);
        }

        /// <summary>
        /// Constructs a new QueryScorer instance
        /// </summary>
        /// <param name="query">Query to use for highlighting</param>
        /// <param name="field">Field to highlight - pass null to ignore fields</param>
        public QueryScorer(Query query, String field)
        {
            Init(query, field, null, true);
        }

        /// <summary>
        /// Constructs a new QueryScorer instance
        /// </summary>
        /// <param name="query">Query to use for highlighting</param>
        /// <param name="reader"><see cref="IndexReader"/> to use for quasi tf/idf scoring</param>
        /// <param name="field">Field to highlight - pass null to ignore fields</param>
        public QueryScorer(Query query, IndexReader reader, String field)
        {
            Init(query, field, reader, true);
        }

        /// <summary>
        /// Constructs a new QueryScorer instance
        /// </summary>
        /// <param name="query">Query to use for highlighting</param>
        /// <param name="reader"><see cref="IndexReader"/> to use for quasi tf/idf scoring</param>
        /// <param name="field">Field to highlight - pass null to ignore fields</param>
        /// <param name="defaultField">The default field for queries with the field name unspecified</param>
        public QueryScorer(Query query, IndexReader reader, String field, String defaultField)
        {
            this.defaultField = StringHelper.Intern(defaultField);
            Init(query, field, reader, true);
        }


        /// <summary>
        /// Constructs a new QueryScorer instance
        /// </summary>
        /// <param name="query">Query to use for highlighting</param>
        /// <param name="field">Field to highlight - pass null to ignore fields</param>
        /// <param name="defaultField">The default field for queries with the field name unspecified</param>
        public QueryScorer(Query query, String field, String defaultField)
        {
            this.defaultField = StringHelper.Intern(defaultField);
            Init(query, field, null, true);
        }

        /// <summary>
        /// Constructs a new QueryScorer instance
        /// </summary>
        /// <param name="weightedTerms">an array of pre-created <see cref="WeightedSpanTerm"/>s</param>
        public QueryScorer(WeightedSpanTerm[] weightedTerms)
        {
            this.fieldWeightedSpanTerms = new HashMap<String, WeightedSpanTerm>(weightedTerms.Length);

            foreach (WeightedSpanTerm t in weightedTerms)
            {
                WeightedSpanTerm existingTerm = fieldWeightedSpanTerms[t.Term];

                if ((existingTerm == null) ||
                    (existingTerm.Weight < t.Weight))
                {
                    // if a term is defined more than once, always use the highest
                    // scoring Weight
                    fieldWeightedSpanTerms[t.Term] = t;
                    maxTermWeight = Math.Max(maxTermWeight, t.Weight);
                }
            }
            skipInitExtractor = true;
        }

        /// <seealso cref="IScorer.FragmentScore"/>
        public float FragmentScore
        {
            get { return totalScore; }
        }

        /// <summary>
        /// The highest weighted term (useful for passing to GradientFormatter to set top end of coloring scale).
        /// </summary>
        public float MaxTermWeight
        {
            get { return maxTermWeight; }
        }

        /// <seealso cref="IScorer.GetTokenScore"/>
        public float GetTokenScore()
        {
            position += posIncAtt.PositionIncrement;
            String termText = termAtt.Term;

            WeightedSpanTerm weightedSpanTerm;

            if ((weightedSpanTerm = fieldWeightedSpanTerms[termText]) == null)
            {
                return 0;
            }

            if (weightedSpanTerm.IsPositionSensitive() &&
                !weightedSpanTerm.CheckPosition(position))
            {
                return 0;
            }

            float score = weightedSpanTerm.Weight;

            // found a query term - is it unique in this doc?
            if (!foundTerms.Contains(termText))
            {
                totalScore += score;
                foundTerms.Add(termText);
            }

            return score;
        }

        /// <seealso cref="IScorer.Init"/>
        public TokenStream Init(TokenStream tokenStream)
        {
            position = -1;
            termAtt = tokenStream.AddAttribute<ITermAttribute>();
            posIncAtt = tokenStream.AddAttribute<IPositionIncrementAttribute>();
            if (!skipInitExtractor)
            {
                if (fieldWeightedSpanTerms != null)
                {
                    fieldWeightedSpanTerms.Clear();
                }
                return InitExtractor(tokenStream);
            }
            return null;
        }

        /// <summary>
        /// Retrieve the <see cref="WeightedSpanTerm"/> for the specified token. Useful for passing
        /// Span information to a <see cref="IFragmenter"/>.
        /// </summary>
        /// <param name="token">token to get {@link WeightedSpanTerm} for</param>
        /// <returns>WeightedSpanTerm for token</returns>
        public WeightedSpanTerm GetWeightedSpanTerm(String token)
        {
            return fieldWeightedSpanTerms[token];
        }
        
        private void Init(Query query, String field, IndexReader reader, bool expandMultiTermQuery)
        {
            this.reader = reader;
            this.expandMultiTermQuery = expandMultiTermQuery;
            this.query = query;
            this.field = field;
        }

        private TokenStream InitExtractor(TokenStream tokenStream)
        {
            WeightedSpanTermExtractor qse = defaultField == null
                                                ? new WeightedSpanTermExtractor()
                                                : new WeightedSpanTermExtractor(defaultField);

            qse.ExpandMultiTermQuery = expandMultiTermQuery;
            qse.SetWrapIfNotCachingTokenFilter(wrapToCaching);
            if (reader == null)
            {
                this.fieldWeightedSpanTerms = qse.GetWeightedSpanTerms(query,
                                                                       tokenStream, field);
            }
            else
            {
                this.fieldWeightedSpanTerms = qse.GetWeightedSpanTermsWithScores(query,
                                                                                 tokenStream, field, reader);
            }
            if (qse.IsCachedTokenStream)
            {
                return qse.TokenStream;
            }

            return null;
        }

        /// <seealso cref="IScorer.StartFragment"/>
        public void StartFragment(TextFragment newFragment)
        {
            foundTerms = Support.Compatibility.SetFactory.CreateHashSet<string>();
            totalScore = 0;
        }

        /// <summary>
        /// Controls whether or not multi-term queries are expanded
        /// against a <see cref="MemoryIndex"/> <see cref="IndexReader"/>.
        /// </summary>
        public bool IsExpandMultiTermQuery
        {
            get { return expandMultiTermQuery; }
            set { this.expandMultiTermQuery = value; }
        }

        /// <summary>
        /// By default, <see cref="TokenStream"/>s that are not of the type
        /// <see cref="CachingTokenFilter"/> are wrapped in a <see cref="CachingTokenFilter"/> to
        /// ensure an efficient reset - if you are already using a different caching
        /// <see cref="TokenStream"/> impl and you don't want it to be wrapped, set this to
        /// false.
        /// </summary>
        public void SetWrapIfNotCachingTokenFilter(bool wrap)
        {
            this.wrapToCaching = wrap;
        }
    }
}