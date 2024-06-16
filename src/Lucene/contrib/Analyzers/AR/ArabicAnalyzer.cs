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
using System.IO;
using System.Collections;
using System.Linq;
using Lucene.Net.Analysis;
using Version = Lucene.Net.Util.Version;
using Lucene.Net.Support.Compatibility;

namespace Lucene.Net.Analysis.AR
{
    /*
     * <see cref="Analyzer"/> for Arabic. 
     * <p/>
     * This analyzer implements light-stemming as specified by:
     * <i>
     * Light Stemming for Arabic Information Retrieval
     * </i>    
     * http://www.mtholyoke.edu/~lballest/Pubs/arab_stem05.pdf
     * <p/>
     * The analysis package contains three primary components:
     * <ul>
     *  <li><see cref="ArabicNormalizationFilter"/>: Arabic orthographic normalization.</li>
     *  <li><see cref="ArabicStemFilter"/>: Arabic light stemming</li>
     *  <li>Arabic stop words file: a set of default Arabic stop words.</li>
     * </ul>
     * 
     */
    public class ArabicAnalyzer : Analyzer
    {

        /*
         * File containing default Arabic stopwords.
         * 
         * Default stopword list is from http://members.unine.ch/jacques.savoy/clef/index.html
         * The stopword list is BSD-Licensed.
         */
        public static string DEFAULT_STOPWORD_FILE = "ArabicStopWords.txt";

        /*
         * Contains the stopwords used with the StopFilter.
         */
        private readonly ISet<string> stoptable;
        /*<summary>
         * The comment character in the stopwords file.  All lines prefixed with this will be ignored  
         * </summary>
         */
        [Obsolete("Use WordListLoader.GetWordSet(FileInfo, string) directly")]
        public static string STOPWORDS_COMMENT = "#";

        /// <summary>
        /// Returns an unmodifiable instance of the default stop-words set
        /// </summary>
        /// <returns>Returns an unmodifiable instance of the default stop-words set</returns>
        public static ISet<string>  GetDefaultStopSet()
        {
            return DefaultSetHolder.DEFAULT_STOP_SET;
        }

        private static class DefaultSetHolder
        {
            internal static ISet<string> DEFAULT_STOP_SET;

            static DefaultSetHolder()
            {
                try
                {
                    DEFAULT_STOP_SET = LoadDefaultStopWordSet();
                }
                catch (System.IO.IOException)
                {
                    // default set should always be present as it is part of the
                    // distribution (JAR)
                    throw new Exception("Unable to load default stopword set");
                }
            }

            internal static ISet<string> LoadDefaultStopWordSet()
            {
                using (StreamReader reader = new StreamReader(System.Reflection.Assembly.GetAssembly(typeof(ArabicAnalyzer)).GetManifestResourceStream("Lucene.Net.Analysis.AR." + DEFAULT_STOPWORD_FILE)))
                {
                    return CharArraySet.UnmodifiableSet(CharArraySet.Copy(WordlistLoader.GetWordSet(reader, STOPWORDS_COMMENT)));
                }
            }
        }

        private Version matchVersion;

        /*
         * Builds an analyzer with the default stop words: <see cref="DEFAULT_STOPWORD_FILE"/>.
         */
        public ArabicAnalyzer(Version matchVersion)
            : this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET)
        {
        }

        /// <summary>
        /// Builds an analyzer with the given stop words.
        /// </summary>
        /// <param name="matchVersion">Lucene compatibility version</param>
        /// <param name="stopwords">a stopword set</param>
        public ArabicAnalyzer(Version matchVersion, ISet<string> stopwords)
        {
            stoptable = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stopwords));
            this.matchVersion = matchVersion;
        }

        /*
         * Builds an analyzer with the given stop words.
         */
        [Obsolete("Use ArabicAnalyzer(Version, Set) instead")]
        public ArabicAnalyzer(Version matchVersion, params string[] stopwords)
            : this(matchVersion, StopFilter.MakeStopSet(stopwords))
        {
        }

        /*
         * Builds an analyzer with the given stop words.
         */
        [Obsolete("Use ArabicAnalyzer(Version, Set) instead")]
        public ArabicAnalyzer(Version matchVersion, IDictionary<string, string> stopwords)
            : this(matchVersion, stopwords.Keys.ToArray())
        {
        }

        /*
         * Builds an analyzer with the given stop words.  Lines can be commented out using <see cref="STOPWORDS_COMMENT"/>
         */
        public ArabicAnalyzer(Version matchVersion, FileInfo stopwords)
            : this(matchVersion, WordlistLoader.GetWordSet(stopwords, STOPWORDS_COMMENT))
        {
        }


        /*
         * Creates a <see cref="TokenStream"/> which tokenizes all the text in the provided <see cref="TextReader"/>.
         *
         * <returns>A <see cref="TokenStream"/> built from an <see cref="ArabicLetterTokenizer"/> filtered with
         * 			<see cref="LowerCaseFilter"/>, <see cref="StopFilter"/>, <see cref="ArabicNormalizationFilter"/>
         *            and <see cref="ArabicStemFilter"/>.</returns>
         */
        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            TokenStream result = new ArabicLetterTokenizer(reader);
            result = new LowerCaseFilter(result);
            // the order here is important: the stopword list is not normalized!
            result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion), result, stoptable);
            result = new ArabicNormalizationFilter(result);
            result = new ArabicStemFilter(result);

            return result;
        }

        private class SavedStreams
        {
            internal Tokenizer Source;
            internal TokenStream Result;
        };

        /*
         * Returns a (possibly reused) <see cref="TokenStream"/> which tokenizes all the text 
         * in the provided <see cref="TextReader"/>.
         *
         * <returns>A <see cref="TokenStream"/> built from an <see cref="ArabicLetterTokenizer"/> filtered with
         *            <see cref="LowerCaseFilter"/>, <see cref="StopFilter"/>, <see cref="ArabicNormalizationFilter"/>
         *            and <see cref="ArabicStemFilter"/>.</returns>
         */
        public override TokenStream ReusableTokenStream(string fieldName, TextReader reader)
        {
            SavedStreams streams = (SavedStreams)PreviousTokenStream;
            if (streams == null)
            {
                streams = new SavedStreams();
                streams.Source = new ArabicLetterTokenizer(reader);
                streams.Result = new LowerCaseFilter(streams.Source);
                // the order here is important: the stopword list is not normalized!
                streams.Result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                                streams.Result, stoptable);
                streams.Result = new ArabicNormalizationFilter(streams.Result);
                streams.Result = new ArabicStemFilter(streams.Result);
                PreviousTokenStream = streams;
            }
            else
            {
                streams.Source.Reset(reader);
            }
            return streams.Result;
        }
    }
}