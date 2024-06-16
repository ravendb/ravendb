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
using System.Collections;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Support;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis.Nl
{
    /*
 * {@link Analyzer} for Dutch language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all), an external list of exclusions (word that will
 * not be stemmed, but indexed) and an external list of word-stem pairs that overrule
 * the algorithm (dictionary stemming).
 * A default set of stopwords is used unless an alternative list is specified, but the
 * exclusion list is empty by default.
 * </p>
 *
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
    public class DutchAnalyzer : Analyzer
    {
        /*
         * List of typical Dutch stopwords.
         * @deprecated use {@link #getDefaultStopSet()} instead
         */
        public static readonly String[] DUTCH_STOP_WORDS =
      {
        "de", "en", "van", "ik", "te", "dat", "die", "in", "een",
        "hij", "het", "niet", "zijn", "is", "was", "op", "aan", "met", "als", "voor", "had",
        "er", "maar", "om", "hem", "dan", "zou", "of", "wat", "mijn", "men", "dit", "zo",
        "door", "over", "ze", "zich", "bij", "ook", "tot", "je", "mij", "uit", "der", "daar",
        "haar", "naar", "heb", "hoe", "heeft", "hebben", "deze", "u", "want", "nog", "zal",
        "me", "zij", "nu", "ge", "geen", "omdat", "iets", "worden", "toch", "al", "waren",
        "veel", "meer", "doen", "toen", "moet", "ben", "zonder", "kan", "hun", "dus",
        "alles", "onder", "ja", "eens", "hier", "wie", "werd", "altijd", "doch", "wordt",
        "wezen", "kunnen", "ons", "zelf", "tegen", "na", "reeds", "wil", "kon", "niets",
        "uw", "iemand", "geweest", "andere"
      };
        /*
         * Returns an unmodifiable instance of the default stop-words set.
         * @return an unmodifiable instance of the default stop-words set.
         */
        public static ISet<string> getDefaultStopSet()
        {
            return DefaultSetHolder.DEFAULT_STOP_SET;
        }

        static class DefaultSetHolder
        {
            internal static readonly ISet<string> DEFAULT_STOP_SET = CharArraySet
                .UnmodifiableSet(new CharArraySet((IEnumerable<string>)DUTCH_STOP_WORDS, false));
        }


        /*
         * Contains the stopwords used with the StopFilter.
         */
        private readonly ISet<string> stoptable;

        /*
         * Contains words that should be indexed but not stemmed.
         */
        private ISet<string> excltable = Support.Compatibility.SetFactory.CreateHashSet<string>();

        private IDictionary<String, String> stemdict = new HashMap<String, String>();
        private readonly Version matchVersion;

        /*
         * Builds an analyzer with the default stop words ({@link #DUTCH_STOP_WORDS}) 
         * and a few default entries for the stem exclusion table.
         * 
         */
        public DutchAnalyzer(Version matchVersion)
            : this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET)
        {
            stemdict.Add("fiets", "fiets"); //otherwise fiet
            stemdict.Add("bromfiets", "bromfiets"); //otherwise bromfiet
            stemdict.Add("ei", "eier");
            stemdict.Add("kind", "kinder");
        }

        public DutchAnalyzer(Version matchVersion, ISet<string> stopwords)
            : this(matchVersion, stopwords, CharArraySet.EMPTY_SET)
        {

        }

        public DutchAnalyzer(Version matchVersion, ISet<string> stopwords, ISet<string> stemExclusionTable)
        {
            stoptable = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stopwords));
            excltable = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stemExclusionTable));
            this.matchVersion = matchVersion;
            SetOverridesTokenStreamMethod<DutchAnalyzer>();
        }

        /*
         * Builds an analyzer with the given stop words.
         *
         * @param matchVersion
         * @param stopwords
         * @deprecated use {@link #DutchAnalyzer(Version, Set)} instead
         */
        public DutchAnalyzer(Version matchVersion, params string[] stopwords)
            : this(matchVersion, StopFilter.MakeStopSet(stopwords))
        {

        }

        /*
         * Builds an analyzer with the given stop words.
         *
         * @param stopwords
         * @deprecated use {@link #DutchAnalyzer(Version, Set)} instead
         */
        public DutchAnalyzer(Version matchVersion, HashSet<string> stopwords)
            : this(matchVersion, (ISet<string>)stopwords)
        {

        }

        /*
         * Builds an analyzer with the given stop words.
         *
         * @param stopwords
         * @deprecated use {@link #DutchAnalyzer(Version, Set)} instead
         */
        public DutchAnalyzer(Version matchVersion, FileInfo stopwords)
        {
            // this is completely broken!
            SetOverridesTokenStreamMethod<DutchAnalyzer>();
            try
            {
                stoptable = WordlistLoader.GetWordSet(stopwords);
            }
            catch (IOException e)
            {
                // TODO: throw IOException
                throw new Exception("", e);
            }
            this.matchVersion = matchVersion;
        }

        /*
         * Builds an exclusionlist from an array of Strings.
         *
         * @param exclusionlist
         * @deprecated use {@link #DutchAnalyzer(Version, Set, Set)} instead
         */
        public void SetStemExclusionTable(params string[] exclusionlist)
        {
            excltable = StopFilter.MakeStopSet(exclusionlist);
            PreviousTokenStream = null; // force a new stemmer to be created
        }

        /*
         * Builds an exclusionlist from a Hashtable.
         * @deprecated use {@link #DutchAnalyzer(Version, Set, Set)} instead
         */
        public void SetStemExclusionTable(ISet<string> exclusionlist)
        {
            excltable = exclusionlist;
            PreviousTokenStream = null; // force a new stemmer to be created
        }

        /*
         * Builds an exclusionlist from the words contained in the given file.
         * @deprecated use {@link #DutchAnalyzer(Version, Set, Set)} instead
         */
        public void SetStemExclusionTable(FileInfo exclusionlist)
        {
            try
            {
                excltable = WordlistLoader.GetWordSet(exclusionlist);
                PreviousTokenStream = null; // force a new stemmer to be created
            }
            catch (IOException e)
            {
                // TODO: throw IOException
                throw new Exception("", e);
            }
        }

        /*
         * Reads a stemdictionary file , that overrules the stemming algorithm
         * This is a textfile that contains per line
         * <tt>word<b>\t</b>stem</tt>, i.e: two tab seperated words
         */
        public void SetStemDictionary(FileInfo stemdictFile)
        {
            try
            {
                stemdict = WordlistLoader.GetStemDict(stemdictFile);
                PreviousTokenStream = null; // force a new stemmer to be created
            }
            catch (IOException e)
            {
                // TODO: throw IOException
                throw new Exception(string.Empty, e);
            }
        }

        /*
         * Creates a {@link TokenStream} which tokenizes all the text in the 
         * provided {@link Reader}.
         *
         * @return A {@link TokenStream} built from a {@link StandardTokenizer}
         *   filtered with {@link StandardFilter}, {@link StopFilter}, 
         *   and {@link DutchStemFilter}
         */
        public override TokenStream TokenStream(String fieldName, TextReader reader)
        {
            TokenStream result = new StandardTokenizer(matchVersion, reader);
            result = new StandardFilter(result);
            result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                    result, stoptable);
            result = new DutchStemFilter(result, excltable, stemdict);
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
         *   filtered with {@link StandardFilter}, {@link StopFilter}, 
         *   and {@link DutchStemFilter}
         */
        public override TokenStream ReusableTokenStream(String fieldName, TextReader reader)
        {
            if (overridesTokenStreamMethod)
            {
                // LUCENE-1678: force fallback to tokenStream() if we
                // have been subclassed and that subclass overrides
                // tokenStream but not reusableTokenStream
                return TokenStream(fieldName, reader);
            }

            SavedStreams streams = (SavedStreams)PreviousTokenStream;
            if (streams == null)
            {
                streams = new SavedStreams();
                streams.source = new StandardTokenizer(matchVersion, reader);
                streams.result = new StandardFilter(streams.source);
                streams.result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                                streams.result, stoptable);
                streams.result = new DutchStemFilter(streams.result, excltable, stemdict);
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