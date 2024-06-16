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

using System.Collections.Generic;
using Lucene.Net.Analysis;
using System.Collections;
using Lucene.Net.Analysis.Tokenattributes;
using Version = Lucene.Net.Util.Version;


/*
 * Based on GermanStemFilter
 *
 */
namespace Lucene.Net.Analysis.BR
{

    public sealed class BrazilianStemFilter : TokenFilter
    {

        /*
         * The actual token in the input stream.
         */
        private BrazilianStemmer stemmer = null;
        private ISet<string> exclusions = null;
        private ITermAttribute termAtt;

        public BrazilianStemFilter(TokenStream input)
            : base(input)
        {
            stemmer = new BrazilianStemmer();
            termAtt = AddAttribute<ITermAttribute>();
        }

        public BrazilianStemFilter(TokenStream input, ISet<string> exclusiontable)
            : this(input)
        {
            this.exclusions = exclusiontable;
        }

        /*
         * <returns>Returns the next token in the stream, or null at EOS.</returns>
         */
        public override bool IncrementToken()
        {
            if (input.IncrementToken())
            {
                string term = termAtt.Term;
                // Check the exclusion table.
                if (exclusions == null || !exclusions.Contains(term))
                {
                    string s = stemmer.Stem(term);
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
    }
}
