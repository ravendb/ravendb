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
using System.Text;
using System.Collections;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Analysis.Fr
{
    /*
 * A {@link TokenFilter} that stems french words. 
 * <p>
 * It supports a table of words that should
 * not be stemmed at all. The used stemmer can be changed at runtime after the
 * filter object is created (as long as it is a {@link FrenchStemmer}).
 * </p>
 * NOTE: This stemmer does not implement the Snowball algorithm correctly,
 * especially involving case problems. It is recommended that you consider using
 * the "French" stemmer in the snowball package instead. This stemmer will likely
 * be deprecated in a future release.
 */
    public sealed class FrenchStemFilter : TokenFilter
    {

        /*
         * The actual token in the input stream.
         */
        private FrenchStemmer stemmer = null;
        private ISet<string> exclusions = null;

        private ITermAttribute termAtt;

        public FrenchStemFilter(TokenStream _in)
            : base(_in)
        {

            stemmer = new FrenchStemmer();
            termAtt = AddAttribute<ITermAttribute>();
        }


        public FrenchStemFilter(TokenStream _in, ISet<string> exclusiontable)
            : this(_in)
        {
            exclusions = exclusiontable;
        }

        /*
         * @return  Returns true for the next token in the stream, or false at EOS
         */
        public override bool IncrementToken()
        {
            if (input.IncrementToken())
            {
                String term = termAtt.Term;

                // Check the exclusion table
                if (exclusions == null || !exclusions.Contains(term))
                {
                    String s = stemmer.Stem(term);
                    // If not stemmed, don't waste the time  adjusting the token.
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
         * Set a alternative/custom {@link FrenchStemmer} for this filter.
         */
        public void SetStemmer(FrenchStemmer stemmer)
        {
            if (stemmer != null)
            {
                this.stemmer = stemmer;
            }
        }
        /*
         * Set an alternative exclusion list for this filter.
         */
        public void SetExclusionTable(IDictionary<string, string> exclusiontable)
        {
            exclusions = Support.Compatibility.SetFactory.CreateHashSet(exclusiontable.Keys);
        }
    }
}
