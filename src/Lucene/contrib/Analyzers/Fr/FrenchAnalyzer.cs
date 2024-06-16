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
using Lucene.Net.Analysis.De;
using Lucene.Net.Analysis.Standard;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis.Fr
{
    /*
 * {@link Analyzer} for French language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (word that will
 * not be stemmed, but indexed).
 * A default set of stopwords is used unless an alternative list is specified, but the
 * exclusion list is empty by default.
 * </p>
 *
 * <a name="version"/>
 * <p>You must specify the required {@link Version}
 * compatibility when creating FrenchAnalyzer:
 * <ul>
 *   <li> As of 2.9, StopFilter preserves position
 *        increments
 * </ul>
 *
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
    public sealed class FrenchAnalyzer : Analyzer
    {

        /*
         * Extended list of typical French stopwords.
         * @deprecated use {@link #getDefaultStopSet()} instead
         */
        // TODO make this private in 3.1
        public readonly static String[] FRENCH_STOP_WORDS = {
    "a", "afin", "ai", "ainsi", "après", "attendu", "au", "aujourd", "auquel", "aussi",
    "autre", "autres", "aux", "auxquelles", "auxquels", "avait", "avant", "avec", "avoir",
    "c", "car", "ce", "ceci", "cela", "celle", "celles", "celui", "cependant", "certain",
    "certaine", "certaines", "certains", "ces", "cet", "cette", "ceux", "chez", "ci",
    "combien", "comme", "comment", "concernant", "contre", "d", "dans", "de", "debout",
    "dedans", "dehors", "delà", "depuis", "derrière", "des", "désormais", "desquelles",
    "desquels", "dessous", "dessus", "devant", "devers", "devra", "divers", "diverse",
    "diverses", "doit", "donc", "dont", "du", "duquel", "durant", "dès", "elle", "elles",
    "en", "entre", "environ", "est", "et", "etc", "etre", "eu", "eux", "excepté", "hormis",
    "hors", "hélas", "hui", "il", "ils", "j", "je", "jusqu", "jusque", "l", "la", "laquelle",
    "le", "lequel", "les", "lesquelles", "lesquels", "leur", "leurs", "lorsque", "lui", "là",
    "ma", "mais", "malgré", "me", "merci", "mes", "mien", "mienne", "miennes", "miens", "moi",
    "moins", "mon", "moyennant", "même", "mêmes", "n", "ne", "ni", "non", "nos", "notre",
    "nous", "néanmoins", "nôtre", "nôtres", "on", "ont", "ou", "outre", "où", "par", "parmi",
    "partant", "pas", "passé", "pendant", "plein", "plus", "plusieurs", "pour", "pourquoi",
    "proche", "près", "puisque", "qu", "quand", "que", "quel", "quelle", "quelles", "quels",
    "qui", "quoi", "quoique", "revoici", "revoilà", "s", "sa", "sans", "sauf", "se", "selon",
    "seront", "ses", "si", "sien", "sienne", "siennes", "siens", "sinon", "soi", "soit",
    "son", "sont", "sous", "suivant", "sur", "ta", "te", "tes", "tien", "tienne", "tiennes",
    "tiens", "toi", "ton", "tous", "tout", "toute", "toutes", "tu", "un", "une", "va", "vers",
    "voici", "voilà", "vos", "votre", "vous", "vu", "vôtre", "vôtres", "y", "à", "ça", "ès",
    "été", "être", "ô"
  };

        /*
         * Contains the stopwords used with the {@link StopFilter}.
         */
        private readonly ISet<string> stoptable;
        /*
         * Contains words that should be indexed but not stemmed.
         */
        //TODO make this final in 3.0
        private ISet<string> excltable = Support.Compatibility.SetFactory.CreateHashSet<string>();

        private readonly Version matchVersion;

        /*
         * Returns an unmodifiable instance of the default stop-words set.
         * @return an unmodifiable instance of the default stop-words set.
         */
        public static ISet<string> GetDefaultStopSet()
        {
            return DefaultSetHolder.DEFAULT_STOP_SET;
        }

        static class DefaultSetHolder
        {
            internal static ISet<string> DEFAULT_STOP_SET = CharArraySet.UnmodifiableSet(new CharArraySet((IEnumerable<string>)FRENCH_STOP_WORDS, false));
        }

        /*
         * Builds an analyzer with the default stop words ({@link #FRENCH_STOP_WORDS}).
         */
        public FrenchAnalyzer(Version matchVersion)
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
        public FrenchAnalyzer(Version matchVersion, ISet<string> stopwords)
            : this(matchVersion, stopwords, CharArraySet.EMPTY_SET)
        {
        }

        /*
         * Builds an analyzer with the given stop words
         * 
         * @param matchVersion
         *          lucene compatibility version
         * @param stopwords
         *          a stopword set
         * @param stemExclutionSet
         *          a stemming exclusion set
         */
        public FrenchAnalyzer(Version matchVersion, ISet<string> stopwords, ISet<string> stemExclutionSet)
        {
            this.matchVersion = matchVersion;
            this.stoptable = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stopwords));
            this.excltable = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stemExclutionSet));
        }


        /*
         * Builds an analyzer with the given stop words.
         * @deprecated use {@link #FrenchAnalyzer(Version, Set)} instead
         */
        public FrenchAnalyzer(Version matchVersion, params string[] stopwords)
            : this(matchVersion, StopFilter.MakeStopSet(stopwords))
        {

        }

        /*
         * Builds an analyzer with the given stop words.
         * @throws IOException
         * @deprecated use {@link #FrenchAnalyzer(Version, Set)} instead
         */
        public FrenchAnalyzer(Version matchVersion, FileInfo stopwords)
            : this(matchVersion, WordlistLoader.GetWordSet(stopwords))
        {
        }

        /*
         * Builds an exclusionlist from an array of Strings.
         * @deprecated use {@link #FrenchAnalyzer(Version, Set, Set)} instead
         */
        public void SetStemExclusionTable(params string[] exclusionlist)
        {
            excltable = StopFilter.MakeStopSet(exclusionlist);
            PreviousTokenStream = null; // force a new stemmer to be created
        }

        /*
         * Builds an exclusionlist from a Map.
         * @deprecated use {@link #FrenchAnalyzer(Version, Set, Set)} instead
         */
        public void SetStemExclusionTable(IDictionary<string, string> exclusionlist)
        {
            excltable = Support.Compatibility.SetFactory.CreateHashSet(exclusionlist.Keys);
            PreviousTokenStream = null; // force a new stemmer to be created
        }

        /*
         * Builds an exclusionlist from the words contained in the given file.
         * @throws IOException
         * @deprecated use {@link #FrenchAnalyzer(Version, Set, Set)} instead
         */
        public void SetStemExclusionTable(FileInfo exclusionlist)
        {
            excltable = WordlistLoader.GetWordSet(exclusionlist);
            PreviousTokenStream = null; // force a new stemmer to be created
        }

        /*
         * Creates a {@link TokenStream} which tokenizes all the text in the provided
         * {@link Reader}.
         *
         * @return A {@link TokenStream} built from a {@link StandardTokenizer} 
         *         filtered with {@link StandardFilter}, {@link StopFilter}, 
         *         {@link FrenchStemFilter} and {@link LowerCaseFilter}
         */
        public override sealed TokenStream TokenStream(String fieldName, TextReader reader)
        {
            TokenStream result = new StandardTokenizer(matchVersion, reader);
            result = new StandardFilter(result);
            result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                    result, stoptable);
            result = new FrenchStemFilter(result, excltable);
            // Convert to lowercase after stemming!
            result = new LowerCaseFilter(result);
            return result;
        }

        class SavedStreams
        {
            protected internal Tokenizer source;
            protected internal TokenStream result;
        };

        /*
         * Returns a (possibly reused) {@link TokenStream} which tokenizes all the 
         * text in the provided {@link Reader}.
         *
         * @return A {@link TokenStream} built from a {@link StandardTokenizer} 
         *         filtered with {@link StandardFilter}, {@link StopFilter}, 
         *         {@link FrenchStemFilter} and {@link LowerCaseFilter}
         */
        public override TokenStream ReusableTokenStream(String fieldName, TextReader reader)
        {
            SavedStreams streams = (SavedStreams)PreviousTokenStream;
            if (streams == null)
            {
                streams = new SavedStreams();
                streams.source = new StandardTokenizer(matchVersion, reader);
                streams.result = new StandardFilter(streams.source);
                streams.result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                                streams.result, stoptable);
                streams.result = new FrenchStemFilter(streams.result, excltable);
                // Convert to lowercase after stemming!
                streams.result = new LowerCaseFilter(streams.result);
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