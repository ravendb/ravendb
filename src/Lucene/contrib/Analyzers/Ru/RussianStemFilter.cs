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
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Analysis.Ru
{
    /*
    * A {@link TokenFilter} that stems Russian words. 
    * <p>
    * The implementation was inspired by GermanStemFilter.
    * The input should be filtered by {@link LowerCaseFilter} before passing it to RussianStemFilter ,
    * because RussianStemFilter only works with lowercase characters.
    * </p>
    */
    public sealed class RussianStemFilter : TokenFilter
    {
        /*
         * The actual token in the input stream.
         */
        private RussianStemmer stemmer = null;

        private ITermAttribute termAtt;

        public RussianStemFilter(TokenStream _in)
            : base(_in)
        {
            stemmer = new RussianStemmer();
            termAtt = AddAttribute<ITermAttribute>();
        }
        /*
         * Returns the next token in the stream, or null at EOS
         */
        public sealed override bool IncrementToken()
        {
            if (input.IncrementToken())
            {
                String term = termAtt.Term;
                String s = stemmer.Stem(term);
                if (s != null && !s.Equals(term))
                    termAtt.SetTermBuffer(s);
                return true;
            }
            else
            {
                return false;
            }
        }


        // I don't get the point of this.  All methods in java are private, so they can't be
        // overridden...You can't really subclass any of its behavior.  I've commented it out,
        // as it doesn't compile as is. - cc
        ////*
        // * Set a alternative/custom {@link RussianStemmer} for this filter.
        // */
        //public void SetStemmer(RussianStemmer stemmer)
        //{
        //    if (stemmer != null)
        //    {
        //        this.stemmer = stemmer;
        //    }
        //}
    }
}