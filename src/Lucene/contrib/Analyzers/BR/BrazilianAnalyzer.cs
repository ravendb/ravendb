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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using System.IO;
using Version = Lucene.Net.Util.Version;

/*
 * Analyzer for Brazilian language. Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (word that will
 * not be stemmed, but indexed).
 *
 */
namespace Lucene.Net.Analysis.BR
{
    public sealed class BrazilianAnalyzer : Analyzer
    {
        /*
         * List of typical Brazilian stopwords.
         */
        //TODO: Make this private in 3.1
        public static string[] BRAZILIAN_STOP_WORDS = {
                                                          "a", "ainda", "alem", "ambas", "ambos", "antes",
                                                          "ao", "aonde", "aos", "apos", "aquele", "aqueles",
                                                          "as", "assim", "com", "como", "contra", "contudo",
                                                          "cuja", "cujas", "cujo", "cujos", "da", "das", "de",
                                                          "dela", "dele", "deles", "demais", "depois", "desde",
                                                          "desta", "deste", "dispoe", "dispoem", "diversa",
                                                          "diversas", "diversos", "do", "dos", "durante", "e",
                                                          "ela", "elas", "ele", "eles", "em", "entao", "entre",
                                                          "essa", "essas", "esse", "esses", "esta", "estas",
                                                          "este", "estes", "ha", "isso", "isto", "logo", "mais",
                                                          "mas", "mediante", "menos", "mesma", "mesmas", "mesmo",
                                                          "mesmos", "na", "nas", "nao", "nas", "nem", "nesse", "neste",
                                                          "nos", "o", "os", "ou", "outra", "outras", "outro", "outros",
                                                          "pelas", "pelas", "pelo", "pelos", "perante", "pois", "por",
                                                          "porque", "portanto", "proprio", "propios", "quais", "qual",
                                                          "qualquer", "quando", "quanto", "que", "quem", "quer", "se",
                                                          "seja", "sem", "sendo", "seu", "seus", "sob", "sobre", "sua",
                                                          "suas", "tal", "tambem", "teu", "teus", "toda", "todas",
                                                          "todo",
                                                          "todos", "tua", "tuas", "tudo", "um", "uma", "umas", "uns"
                                                      };

        /// <summary>
        /// Returns an unmodifiable instance of the default stop-words set.
        /// </summary>
        /// <returns>Returns an unmodifiable instance of the default stop-words set.</returns>
        public static ISet<string> GetDefaultStopSet()
        {
            return DefaultSetHolder.DEFAULT_STOP_SET;
        }

        private static class DefaultSetHolder
        {
            internal static ISet<string> DEFAULT_STOP_SET =
                CharArraySet.UnmodifiableSet(new CharArraySet((IEnumerable<string>)BRAZILIAN_STOP_WORDS, false));
        }

        /// <summary>
        /// Contains the stopwords used with the StopFilter.
        /// </summary>
        private ISet<string> stoptable = Support.Compatibility.SetFactory.CreateHashSet<string>();

        private readonly Version matchVersion;

        // TODO: make this private in 3.1
        /// <summary>
        /// Contains words that should be indexed but not stemmed.
        /// </summary>
        private ISet<string> excltable = Support.Compatibility.SetFactory.CreateHashSet<string>();

        public BrazilianAnalyzer(Version matchVersion)
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

        public BrazilianAnalyzer(Version matchVersion, ISet<string> stopwords)
        {
            stoptable = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stopwords));
            this.matchVersion = matchVersion;
        }

        /*
         * Builds an analyzer with the given stop words and stemming exclusion words
         * 
         * @param matchVersion
         *          lucene compatibility version
         * @param stopwords
         *          a stopword set
         */

        public BrazilianAnalyzer(Version matchVersion, ISet<string> stopwords,
                                 ISet<string> stemExclusionSet)
            : this(matchVersion, stopwords)
        {

            excltable = CharArraySet.UnmodifiableSet(CharArraySet
                                                         .Copy(stemExclusionSet));
        }

        /*
         * Builds an analyzer with the given stop words.
         * @deprecated use {@link #BrazilianAnalyzer(Version, Set)} instead
         */

        public BrazilianAnalyzer(Version matchVersion, params string[] stopwords)
            : this(matchVersion, StopFilter.MakeStopSet(stopwords))
        {

        }

        /*
   * Builds an analyzer with the given stop words. 
   * @deprecated use {@link #BrazilianAnalyzer(Version, Set)} instead
   */

        public BrazilianAnalyzer(Version matchVersion, IDictionary<string, string> stopwords)
            : this(matchVersion, stopwords.Keys.ToArray())
        {

        }

        /*
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #BrazilianAnalyzer(Version, Set)} instead
   */

        public BrazilianAnalyzer(Version matchVersion, FileInfo stopwords)
            : this(matchVersion, WordlistLoader.GetWordSet(stopwords))
        {
        }

        /*
         * Builds an exclusionlist from an array of Strings.
         * @deprecated use {@link #BrazilianAnalyzer(Version, Set, Set)} instead
         */

        public void SetStemExclusionTable(params string[] exclusionlist)
        {
            excltable = StopFilter.MakeStopSet(exclusionlist);
            PreviousTokenStream = null; // force a new stemmer to be created
        }

        /*
         * Builds an exclusionlist from a {@link Map}.
         * @deprecated use {@link #BrazilianAnalyzer(Version, Set, Set)} instead
         */

        public void SetStemExclusionTable(IDictionary<string, string> exclusionlist)
        {
            excltable = Support.Compatibility.SetFactory.CreateHashSet(exclusionlist.Keys);
            PreviousTokenStream = null; // force a new stemmer to be created
        }

        /*
         * Builds an exclusionlist from the words contained in the given file.
         * @deprecated use {@link #BrazilianAnalyzer(Version, Set, Set)} instead
         */

        public void SetStemExclusionTable(FileInfo exclusionlist)
        {
            excltable = WordlistLoader.GetWordSet(exclusionlist);
            PreviousTokenStream = null; // force a new stemmer to be created
        }

        /*
         * Creates a {@link TokenStream} which tokenizes all the text in the provided {@link Reader}.
         *
         * @return  A {@link TokenStream} built from a {@link StandardTokenizer} filtered with
         * 			{@link LowerCaseFilter}, {@link StandardFilter}, {@link StopFilter}, and 
         *          {@link BrazilianStemFilter}.
         */
        public override TokenStream TokenStream(String fieldName, TextReader reader)
        {
            TokenStream result = new StandardTokenizer(matchVersion, reader);
            result = new LowerCaseFilter(result);
            result = new StandardFilter(result);
            result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                    result, stoptable);
            result = new BrazilianStemFilter(result, excltable);
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
         *          {@link LowerCaseFilter}, {@link StandardFilter}, {@link StopFilter}, and 
         *          {@link BrazilianStemFilter}.
         */

        public override TokenStream ReusableTokenStream(String fieldName, TextReader reader)
        {
            SavedStreams streams = (SavedStreams) PreviousTokenStream;
            if (streams == null)
            {
                streams = new SavedStreams();
                streams.source = new StandardTokenizer(matchVersion, reader);
                streams.result = new LowerCaseFilter(streams.source);
                streams.result = new StandardFilter(streams.result);
                streams.result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                                streams.result, stoptable);
                streams.result = new BrazilianStemFilter(streams.result, excltable);
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
