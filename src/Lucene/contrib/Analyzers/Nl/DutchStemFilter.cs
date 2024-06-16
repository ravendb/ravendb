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
using System.IO;
using System.Collections;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Support;

namespace Lucene.Net.Analysis.Nl
{
    /*
 * A {@link TokenFilter} that stems Dutch words. 
 * <p>
 * It supports a table of words that should
 * not be stemmed at all. The stemmer used can be changed at runtime after the
 * filter object is created (as long as it is a {@link DutchStemmer}).
 * </p>
 * NOTE: This stemmer does not implement the Snowball algorithm correctly,
 * specifically doubled consonants. It is recommended that you consider using
 * the "Dutch" stemmer in the snowball package instead. This stemmer will likely
 * be deprecated in a future release.
 */
    public sealed class DutchStemFilter : TokenFilter
    {
        /*
         * The actual token in the input stream.
         */
        private DutchStemmer stemmer = null;
        private ISet<string> exclusions = null;

        private ITermAttribute termAtt;

        public DutchStemFilter(TokenStream _in)
            : base(_in)
        {
            stemmer = new DutchStemmer();
            termAtt = AddAttribute<ITermAttribute>();
        }

        /*
         * Builds a DutchStemFilter that uses an exclusion table.
         */
        public DutchStemFilter(TokenStream _in, ISet<string> exclusiontable)
            : this(_in)
        {
            exclusions = exclusiontable;
        }

        /*
         * @param stemdictionary Dictionary of word stem pairs, that overrule the algorithm
         */
        public DutchStemFilter(TokenStream _in, ISet<string> exclusiontable, IDictionary<string, string> stemdictionary)
            : this(_in, exclusiontable)
        {
            stemmer.SetStemDictionary(stemdictionary);
        }

        /*
         * Returns the next token in the stream, or null at EOS
         */
        public override bool IncrementToken()
        {
            if (input.IncrementToken())
            {
                String term = termAtt.Term;

                // Check the exclusion table.
                if (exclusions == null || !exclusions.Contains(term))
                {
                    String s = stemmer.Stem(term);
                    // If not stemmed, don't waste the time adjusting the token.
                    if ((s != null) && !s.Equals(term))
                        termAtt.SetTermBuffer(s);
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        /*
         * Set a alternative/custom {@link DutchStemmer} for this filter.
         */
        public void SetStemmer(DutchStemmer stemmer)
        {
            if (stemmer != null)
            {
                this.stemmer = stemmer;
            }
        }

        /*
         * Set an alternative exclusion list for this filter.
         */
        public void SetExclusionTable(ISet<string> exclusiontable)
        {
            exclusions = exclusiontable;
        }

        /*
         * Set dictionary for stemming, this dictionary overrules the algorithm,
         * so you can correct for a particular unwanted word-stem pair.
         */
        public void SetStemDictionary(IDictionary<string, string> dict)
        {
            if (stemmer != null)
                stemmer.SetStemDictionary(dict);
        }
    }
}