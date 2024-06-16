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
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Analysis.Fr
{
    /*
     * Removes elisions from a {@link TokenStream}. For example, "l'avion" (the plane) will be
     * tokenized as "avion" (plane).
     * <p>
     * Note that {@link StandardTokenizer} sees " ' " as a space, and cuts it out.
     * 
     * @see <a href="http://fr.wikipedia.org/wiki/%C3%89lision">Elision in Wikipedia</a>
     */
    public sealed class ElisionFilter : TokenFilter
    {
        private CharArraySet articles = null;
        private ITermAttribute termAtt;

        private static char[] apostrophes = { '\'', '’' };

        public void SetArticles(ISet<string> articles)
        {
            if (articles is CharArraySet)
                this.articles = (CharArraySet)articles;
            else
                this.articles = new CharArraySet(articles, true);
        }

        /*
         * Constructs an elision filter with standard stop words
         */
        internal ElisionFilter(TokenStream input)
            : this(input, new[] { "l", "m", "t", "qu", "n", "s", "j" })
        { }

        /*
         * Constructs an elision filter with a Set of stop words
         */
        public ElisionFilter(TokenStream input, ISet<string> articles)
            : base(input)
        {
            SetArticles(articles);
            termAtt = AddAttribute<ITermAttribute>();
        }

        /*
         * Constructs an elision filter with an array of stop words
         */
        public ElisionFilter(TokenStream input, IEnumerable<string> articles)
            : base(input)
        {
            this.articles = new CharArraySet(articles, true);
            termAtt = AddAttribute<ITermAttribute>();
        }

        /*
         * Increments the {@link TokenStream} with a {@link TermAttribute} without elisioned start
         */
        public override sealed bool IncrementToken()
        {
            if (input.IncrementToken())
            {
                char[] termBuffer = termAtt.TermBuffer();
                int termLength = termAtt.TermLength();

                int minPoz = int.MaxValue;
                for (int i = 0; i < apostrophes.Length; i++)
                {
                    char apos = apostrophes[i];
                    // The equivalent of String.indexOf(ch)
                    for (int poz = 0; poz < termLength; poz++)
                    {
                        if (termBuffer[poz] == apos)
                        {
                            minPoz = Math.Min(poz, minPoz);
                            break;
                        }
                    }
                }

                // An apostrophe has been found. If the prefix is an article strip it off.
                if (minPoz != int.MaxValue
                    && articles.Contains(termAtt.TermBuffer(), 0, minPoz))
                {
                    termAtt.SetTermBuffer(termAtt.TermBuffer(), minPoz + 1, termAtt.TermLength() - (minPoz + 1));
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
