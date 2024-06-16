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
using System.Linq;
using Lucene.Net.Analysis.Standard;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis.El
{
    /*
     * {@link Analyzer} for the Greek language. 
     * <p>
     * Supports an external list of stopwords (words
     * that will not be indexed at all).
     * A default set of stopwords is used unless an alternative list is specified.
     * </p>
     *
     * <p><b>NOTE</b>: This class uses the same {@link Version}
     * dependent settings as {@link StandardAnalyzer}.</p>
     */
    public sealed class GreekAnalyzer : Analyzer
    {
        /*
         * List of typical Greek stopwords.
         */

        private static readonly String[] GREEK_STOP_WORDS = {
                                                                "ο", "η", "το", "οι", "τα", "του", "τησ", "των", "τον",
                                                                "την", "και",
                                                                "κι", "κ", "ειμαι", "εισαι", "ειναι", "ειμαστε", "ειστε"
                                                                , "στο", "στον",
                                                                "στη", "στην", "μα", "αλλα", "απο", "για", "προσ", "με",
                                                                "σε", "ωσ",
                                                                "παρα", "αντι", "κατα", "μετα", "θα", "να", "δε", "δεν",
                                                                "μη", "μην",
                                                                "επι", "ενω", "εαν", "αν", "τοτε", "που", "πωσ", "ποιοσ"
                                                                , "ποια", "ποιο",
                                                                "ποιοι", "ποιεσ", "ποιων", "ποιουσ", "αυτοσ", "αυτη",
                                                                "αυτο", "αυτοι",
                                                                "αυτων", "αυτουσ", "αυτεσ", "αυτα", "εκεινοσ", "εκεινη",
                                                                "εκεινο",
                                                                "εκεινοι", "εκεινεσ", "εκεινα", "εκεινων", "εκεινουσ",
                                                                "οπωσ", "ομωσ",
                                                                "ισωσ", "οσο", "οτι"
                                                            };

        /*
         * Returns a set of default Greek-stopwords 
         * @return a set of default Greek-stopwords 
         */
        public static ISet<string> GetDefaultStopSet()
        {
            return DefaultSetHolder.DEFAULT_SET;
        }

        private static class DefaultSetHolder
        {
            internal static ISet<string> DEFAULT_SET = CharArraySet.UnmodifiableSet(new CharArraySet((IEnumerable<string>)GREEK_STOP_WORDS, false));
        }

        /*
         * Contains the stopwords used with the {@link StopFilter}.
         */
        private readonly ISet<string> stopSet;

        private readonly Version matchVersion;

        public GreekAnalyzer(Version matchVersion)
            : this(matchVersion, DefaultSetHolder.DEFAULT_SET)
        {
        }

        /*
         * Builds an analyzer with the given stop words 
         * 
         * @param matchVersion
         *          lucene compatibility version
         * @param stopwords
         *          a stopword set
         */
        public GreekAnalyzer(Version matchVersion, ISet<string> stopwords)
        {
            stopSet = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stopwords));
            this.matchVersion = matchVersion;
        }

        /*
         * Builds an analyzer with the given stop words.
         * @param stopwords Array of stopwords to use.
         * @deprecated use {@link #GreekAnalyzer(Version, Set)} instead
         */
        public GreekAnalyzer(Version matchVersion, params string[] stopwords)
            : this(matchVersion, StopFilter.MakeStopSet(stopwords))
        {
        }

        /*
         * Builds an analyzer with the given stop words.
         * @deprecated use {@link #GreekAnalyzer(Version, Set)} instead
         */
        public GreekAnalyzer(Version matchVersion, IDictionary<string, string> stopwords)
            : this(matchVersion, stopwords.Keys.ToArray())
        {
        }

        /*
         * Creates a {@link TokenStream} which tokenizes all the text in the provided {@link Reader}.
         *
         * @return  A {@link TokenStream} built from a {@link StandardTokenizer} filtered with
         *                  {@link GreekLowerCaseFilter} and {@link StopFilter}
         */
        public override TokenStream TokenStream(String fieldName, TextReader reader)
        {
            TokenStream result = new StandardTokenizer(matchVersion, reader);
            result = new GreekLowerCaseFilter(result);
            result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                    result, stopSet);
            return result;
        }

        private class SavedStreams
        {
            protected internal Tokenizer source;
            protected internal TokenStream result;
        };

        /*
         * Returns a (possibly reused) {@link TokenStream} which tokenizes all the text 
         * in the provided {@link Reader}.
         *
         * @return  A {@link TokenStream} built from a {@link StandardTokenizer} filtered with
         *                  {@link GreekLowerCaseFilter} and {@link StopFilter}
         */
        public override TokenStream ReusableTokenStream(String fieldName, TextReader reader)
        {
            SavedStreams streams = (SavedStreams)PreviousTokenStream;
            if (streams == null)
            {
                streams = new SavedStreams();
                streams.source = new StandardTokenizer(matchVersion, reader);
                streams.result = new GreekLowerCaseFilter(streams.source);
                streams.result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                                streams.result, stopSet);
                PreviousTokenStream = streams;
            }
            else
            {
                streams.source.Reset(reader);
            }
            return streams.result;
        }
    }
}
