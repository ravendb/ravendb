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
using System.IO;
using System.Collections;
using Lucene.Net.Analysis;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis.Ru
{
    /// <summary>
    /// Analyzer for Russian language. Supports an external list of stopwords (words that
    /// will not be indexed at all).
    /// A default set of stopwords is used unless an alternative list is specified.
    /// </summary>
    public sealed class RussianAnalyzer : Analyzer
    {
        /// <summary>
        /// List of typical Russian stopwords.
        /// </summary>
        private static readonly String[] RUSSIAN_STOP_WORDS = {
                                                                  "а", "без", "более", "бы", "был", "была", "были",
                                                                  "было", "быть", "в",
                                                                  "вам", "вас", "весь", "во", "вот", "все", "всего",
                                                                  "всех", "вы", "где",
                                                                  "да", "даже", "для", "до", "его", "ее", "ей", "ею",
                                                                  "если", "есть",
                                                                  "еще", "же", "за", "здесь", "и", "из", "или", "им",
                                                                  "их", "к", "как",
                                                                  "ко", "когда", "кто", "ли", "либо", "мне", "может",
                                                                  "мы", "на", "надо",
                                                                  "наш", "не", "него", "нее", "нет", "ни", "них", "но",
                                                                  "ну", "о", "об",
                                                                  "однако", "он", "она", "они", "оно", "от", "очень",
                                                                  "по", "под", "при",
                                                                  "с", "со", "так", "также", "такой", "там", "те", "тем"
                                                                  , "то", "того",
                                                                  "тоже", "той", "только", "том", "ты", "у", "уже",
                                                                  "хотя", "чего", "чей",
                                                                  "чем", "что", "чтобы", "чье", "чья", "эта", "эти",
                                                                  "это", "я"
                                                              };

        private static class DefaultSetHolder
        {
            internal static readonly ISet<string> DEFAULT_STOP_SET = CharArraySet.UnmodifiableSet(new CharArraySet((IEnumerable<string>)RUSSIAN_STOP_WORDS, false));
        }

        /// <summary>
        /// Contains the stopwords used with the StopFilter.
        /// </summary>
        private readonly ISet<string> stopSet;

        private readonly Version matchVersion;


        public RussianAnalyzer(Version matchVersion)
            : this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET)
        {
        }

        /*
         * Builds an analyzer with the given stop words.
         * @deprecated use {@link #RussianAnalyzer(Version, Set)} instead
         */
        public RussianAnalyzer(Version matchVersion, params string[] stopwords)
            : this(matchVersion, StopFilter.MakeStopSet(stopwords))
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
        public RussianAnalyzer(Version matchVersion, ISet<string> stopwords)
        {
            stopSet = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stopwords));
            this.matchVersion = matchVersion;
        }

        /*
         * Builds an analyzer with the given stop words.
         * TODO: create a Set version of this ctor
         * @deprecated use {@link #RussianAnalyzer(Version, Set)} instead
         */
        public RussianAnalyzer(Version matchVersion, IDictionary<string, string> stopwords)
            : this(matchVersion, stopwords.Keys.ToArray())
        {
        }

        /*
         * Creates a {@link TokenStream} which tokenizes all the text in the 
         * provided {@link Reader}.
         *
         * @return  A {@link TokenStream} built from a 
         *   {@link RussianLetterTokenizer} filtered with 
         *   {@link RussianLowerCaseFilter}, {@link StopFilter}, 
         *   and {@link RussianStemFilter}
         */
        public override TokenStream TokenStream(String fieldName, TextReader reader)
        {
            TokenStream result = new RussianLetterTokenizer(reader);
            result = new LowerCaseFilter(result);
            result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                    result, stopSet);
            result = new RussianStemFilter(result);
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
         * @return  A {@link TokenStream} built from a 
         *   {@link RussianLetterTokenizer} filtered with 
         *   {@link RussianLowerCaseFilter}, {@link StopFilter}, 
         *   and {@link RussianStemFilter}
         */
        public override TokenStream ReusableTokenStream(String fieldName, TextReader reader)
        {
            SavedStreams streams = (SavedStreams)PreviousTokenStream;
            if (streams == null)
            {
                streams = new SavedStreams();
                streams.source = new RussianLetterTokenizer(reader);
                streams.result = new LowerCaseFilter(streams.source);
                streams.result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                                streams.result, stopSet);
                streams.result = new RussianStemFilter(streams.result);
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