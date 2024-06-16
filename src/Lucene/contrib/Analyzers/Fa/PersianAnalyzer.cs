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
using Lucene.Net.Analysis.AR;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis.Fa
{
    /*
     * {@link Analyzer} for Persian.
     * <p>
     * This Analyzer uses {@link ArabicLetterTokenizer} which implies tokenizing around
     * zero-width non-joiner in addition to whitespace. Some persian-specific variant forms (such as farsi
     * yeh and keheh) are standardized. "Stemming" is accomplished via stopwords.
     * </p>
     */
    public sealed class PersianAnalyzer : Analyzer
    {

        /*
         * File containing default Persian stopwords.
         * 
         * Default stopword list is from
         * http://members.unine.ch/jacques.savoy/clef/index.html The stopword list is
         * BSD-Licensed.
         * 
         */
        public readonly static String DEFAULT_STOPWORD_FILE = "stopwords.txt";

        /*
         * Contains the stopwords used with the StopFilter.
         */
        private readonly ISet<string> stoptable;

        /*
         * The comment character in the stopwords file. All lines prefixed with this
         * will be ignored
         */
        public static readonly String STOPWORDS_COMMENT = "#";

        /*
         * Returns an unmodifiable instance of the default stop-words set.
         * @return an unmodifiable instance of the default stop-words set.
         */
        public static ISet<string> getDefaultStopSet()
        {
            return DefaultSetHolder.DEFAULT_STOP_SET;
        }

        /*
         * Atomically loads the DEFAULT_STOP_SET in a lazy fashion once the outer class 
         * accesses the static final set the first time.;
         */
        private static class DefaultSetHolder
        {
            internal static readonly ISet<string> DEFAULT_STOP_SET;

            static DefaultSetHolder()
            {
                try
                {
                    DEFAULT_STOP_SET = LoadDefaultStopWordSet();
                }
                catch (IOException ex)
                {
                    // default set should always be present as it is part of the
                    // distribution (JAR)
                    throw new Exception("Unable to load default stopword set");
                }
            }

            static ISet<String> LoadDefaultStopWordSet()
            {

                var stream = System.Reflection.Assembly.GetAssembly(typeof(PersianAnalyzer)).GetManifestResourceStream("Lucene.Net.Analyzers.Fa." + DEFAULT_STOPWORD_FILE);
                try
                {
                    StreamReader reader = new StreamReader(stream, System.Text.Encoding.UTF8);
                    // make sure it is unmodifiable as we expose it in the outer class
                    return CharArraySet.UnmodifiableSet(new CharArraySet(WordlistLoader.GetWordSet(reader, STOPWORDS_COMMENT), true));
                }
                finally
                {
                    stream.Close();
                }
            }
        }

        private readonly Version matchVersion;

        /*
         * Builds an analyzer with the default stop words:
         * {@link #DEFAULT_STOPWORD_FILE}.
         */
        public PersianAnalyzer(Version matchVersion)
            : this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET)
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
        public PersianAnalyzer(Version matchVersion, ISet<string> stopwords)
        {
            stoptable = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stopwords));
            this.matchVersion = matchVersion;
        }

        /*
         * Builds an analyzer with the given stop words.
         * @deprecated use {@link #PersianAnalyzer(Version, Set)} instead
         */
        public PersianAnalyzer(Version matchVersion, params string[] stopwords)
            : this(matchVersion, StopFilter.MakeStopSet(stopwords))
        {

        }

        /*
         * Builds an analyzer with the given stop words.
         * @deprecated use {@link #PersianAnalyzer(Version, Set)} instead
         */
        public PersianAnalyzer(Version matchVersion, IDictionary<string, string> stopwords)
            : this(matchVersion, stopwords.Keys.ToArray())
        {

        }

        /*
         * Builds an analyzer with the given stop words. Lines can be commented out
         * using {@link #STOPWORDS_COMMENT}
         * @deprecated use {@link #PersianAnalyzer(Version, Set)} instead
         */
        public PersianAnalyzer(Version matchVersion, FileInfo stopwords)
            : this(matchVersion, WordlistLoader.GetWordSet(stopwords, STOPWORDS_COMMENT))
        {

        }

        /*
         * Creates a {@link TokenStream} which tokenizes all the text in the provided
         * {@link Reader}.
         * 
         * @return A {@link TokenStream} built from a {@link ArabicLetterTokenizer}
         *         filtered with {@link LowerCaseFilter}, 
         *         {@link ArabicNormalizationFilter},
         *         {@link PersianNormalizationFilter} and Persian Stop words
         */
        public override TokenStream TokenStream(String fieldName, TextReader reader)
        {
            TokenStream result = new ArabicLetterTokenizer(reader);
            result = new LowerCaseFilter(result);
            result = new ArabicNormalizationFilter(result);
            /* additional persian-specific normalization */
            result = new PersianNormalizationFilter(result);
            /*
             * the order here is important: the stopword list is normalized with the
             * above!
             */
            result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                    result, stoptable);
            return result;
        }

        private class SavedStreams
        {
            protected internal Tokenizer source;
            protected internal TokenStream result;
        }

        /*
         * Returns a (possibly reused) {@link TokenStream} which tokenizes all the text 
         * in the provided {@link Reader}.
         * 
         * @return A {@link TokenStream} built from a {@link ArabicLetterTokenizer}
         *         filtered with {@link LowerCaseFilter}, 
         *         {@link ArabicNormalizationFilter},
         *         {@link PersianNormalizationFilter} and Persian Stop words
         */
        public override TokenStream ReusableTokenStream(String fieldName, TextReader reader)
        {
            SavedStreams streams = (SavedStreams)PreviousTokenStream;
            if (streams == null)
            {
                streams = new SavedStreams();
                streams.source = new ArabicLetterTokenizer(reader);
                streams.result = new LowerCaseFilter(streams.source);
                streams.result = new ArabicNormalizationFilter(streams.result);
                /* additional persian-specific normalization */
                streams.result = new PersianNormalizationFilter(streams.result);
                /*
                 * the order here is important: the stopword list is normalized with the
                 * above!
                 */
                streams.result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                                streams.result, stoptable);
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
