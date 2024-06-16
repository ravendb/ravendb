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
using System.Linq;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Analysis;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis.De
{
    /// <summary>
    /// Analyzer for German language. Supports an external list of stopwords (words that
    /// will not be indexed at all) and an external list of exclusions (word that will
    /// not be stemmed, but indexed).
    /// A default set of stopwords is used unless an alternative list is specified, the
    /// exclusion list is empty by default.
    /// </summary>
    public class GermanAnalyzer : Analyzer
    {
        /// <summary>
        /// List of typical german stopwords.
        /// </summary>
        //TODO: make this private in 3.1
        private static readonly String[] GERMAN_STOP_WORDS = 
		{
			"einer", "eine", "eines", "einem", "einen",
			"der", "die", "das", "dass", "daß",
			"du", "er", "sie", "es",
			"was", "wer", "wie", "wir",
			"und", "oder", "ohne", "mit",
			"am", "im", "in", "aus", "auf",
			"ist", "sein", "war", "wird",
			"ihr", "ihre", "ihres",
			"als", "für", "von",
			"dich", "dir", "mich", "mir",
			"mein", "kein",
			"durch", "wegen"
		};

        /// <summary>
        /// Returns a set of default German-stopwords 
        /// </summary>
        public static ISet<string> GetDefaultStopSet()
        {
            return DefaultSetHolder.DEFAULT_SET;
        }

        private static class DefaultSetHolder
        {
            internal static readonly ISet<string> DEFAULT_SET = CharArraySet.UnmodifiableSet(new CharArraySet(
                                                                                                 (IEnumerable<string>)GERMAN_STOP_WORDS,
                                                                                                 false));
        }

        /// <summary>
        /// Contains the stopwords used with the StopFilter. 
        /// </summary>
        //TODO: make this readonly in 3.1
        private ISet<string> stopSet;

        /// <summary>
        /// Contains words that should be indexed but not stemmed. 
        /// </summary>
        //TODO: make this readonly in 3.1
        private ISet<string> exclusionSet;

        private Version matchVersion;
        private readonly bool _normalizeDin2;

        /// <summary>
        /// Builds an analyzer with the default stop words:
        /// <see cref="GetDefaultStopSet"/>
        /// </summary>
        [Obsolete("Use GermanAnalyzer(Version) instead")]
        public GermanAnalyzer()
            : this(Version.LUCENE_CURRENT)
        {
        }

        /// <summary>
        /// Builds an analyzer with the default stop words:
        /// <see cref="GetDefaultStopSet"/>
        /// </summary>
        /// <param name="matchVersion">Lucene compatibility version</param>
        public GermanAnalyzer(Version matchVersion)
            : this(matchVersion, DefaultSetHolder.DEFAULT_SET)
        { }

        /// <summary>
        /// Builds an analyzer with the default stop words:
        /// <see cref="GetDefaultStopSet"/>
        ///  </summary>
        /// <param name="matchVersion">Lucene compatibility version</param>
        /// <param name="normalizeDin2">Specifies if the DIN-2007-2 style stemmer should be used in addition to DIN1.  This
        /// will cause words with 'ae', 'ue', or 'oe' in them (expanded umlauts) to be first converted to 'a', 'u', and 'o'
        /// respectively, before the DIN1 stemmer is invoked.</param>
        public GermanAnalyzer(Version matchVersion, bool normalizeDin2)
            : this(matchVersion, DefaultSetHolder.DEFAULT_SET, normalizeDin2)
        { }

        /// <summary>
        /// Builds an analyzer with the given stop words, using the default DIN-5007-1 stemmer
        /// </summary>
        /// <param name="matchVersion">Lucene compatibility version</param>
        /// <param name="stopwords">a stopword set</param>
        public GermanAnalyzer(Version matchVersion, ISet<string> stopwords)
            : this(matchVersion, stopwords, CharArraySet.EMPTY_SET)
        {
        }

        /// <summary>
        /// Builds an analyzer with the given stop words
        /// </summary>
        /// <param name="matchVersion">Lucene compatibility version</param>
        /// <param name="stopwords">a stopword set</param>
        /// <param name="normalizeDin2">Specifies if the DIN-2007-2 style stemmer should be used in addition to DIN1.  This
        /// will cause words with 'ae', 'ue', or 'oe' in them (expanded umlauts) to be first converted to 'a', 'u', and 'o'
        /// respectively, before the DIN1 stemmer is invoked.</param>
        public GermanAnalyzer(Version matchVersion, ISet<string> stopwords, bool normalizeDin2)
            : this(matchVersion, stopwords, CharArraySet.EMPTY_SET, normalizeDin2)
        {
        }

        /// <summary>
        /// Builds an analyzer with the given stop words, using the default DIN-5007-1 stemmer
        /// </summary>
        /// <param name="matchVersion">lucene compatibility version</param>
        /// <param name="stopwords">a stopword set</param>
        /// <param name="stemExclusionSet">a stemming exclusion set</param>
        public GermanAnalyzer(Version matchVersion, ISet<string> stopwords, ISet<string> stemExclusionSet)
            : this(matchVersion, stopwords, stemExclusionSet, false)
        { }


        /// <summary>
        /// Builds an analyzer with the given stop words
        /// </summary>
        /// <param name="matchVersion">lucene compatibility version</param>
        /// <param name="stopwords">a stopword set</param>
        /// <param name="stemExclusionSet">a stemming exclusion set</param>
        /// <param name="normalizeDin2">Specifies if the DIN-2007-2 style stemmer should be used in addition to DIN1.  This
        /// will cause words with 'ae', 'ue', or 'oe' in them (expanded umlauts) to be first converted to 'a', 'u', and 'o'
        /// respectively, before the DIN1 stemmer is invoked.</param>
        public GermanAnalyzer(Version matchVersion, ISet<string> stopwords, ISet<string> stemExclusionSet, bool normalizeDin2)
        {
            stopSet = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stopwords));
            exclusionSet = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stemExclusionSet));
            this.matchVersion = matchVersion;
            _normalizeDin2 = normalizeDin2;
            SetOverridesTokenStreamMethod<GermanAnalyzer>();
        }

        /// <summary>
        /// Builds an analyzer with the given stop words. 
        /// </summary>
        /// <param name="stopwords"></param>
        [Obsolete("use GermanAnalyzer(Version, Set) instead")]
        public GermanAnalyzer(Version matchVersion, params string[] stopwords)
            : this(matchVersion, StopFilter.MakeStopSet(stopwords))
        {
        }

        /// <summary>
        /// Builds an analyzer with the given stop words.
        /// </summary>
        [Obsolete("Use GermanAnalyzer(Version, ISet)")]
        public GermanAnalyzer(Version matchVersion, IDictionary<string, string> stopwords)
            : this(matchVersion, stopwords.Keys.ToArray())
        {

        }

        /// <summary>
        /// Builds an analyzer with the given stop words. 
        /// </summary>
        [Obsolete("Use GermanAnalyzer(Version, ISet)")]
        public GermanAnalyzer(Version matchVersion, FileInfo stopwords)
            : this(matchVersion, WordlistLoader.GetWordSet(stopwords))
        {
        }

        /// <summary>
        /// Builds an exclusionlist from an array of Strings. 
        /// </summary>
        [Obsolete("Use GermanAnalyzer(Version, ISet, ISet) instead")]
        public void SetStemExclusionTable(String[] exclusionlist)
        {
            exclusionSet = StopFilter.MakeStopSet(exclusionlist);
            PreviousTokenStream = null;
        }

        /// <summary>
        /// Builds an exclusionlist from a IDictionary. 
        /// </summary>
        [Obsolete("Use GermanAnalyzer(Version, ISet, ISet) instead")]
        public void SetStemExclusionTable(IDictionary<string, string> exclusionlist)
        {
            exclusionSet = Support.Compatibility.SetFactory.CreateHashSet(exclusionlist.Keys);
            PreviousTokenStream = null;
        }

        /// <summary>
        /// Builds an exclusionlist from the words contained in the given file. 
        /// </summary>
        [Obsolete("Use GermanAnalyzer(Version, ISet, ISet) instead")]
        public void SetStemExclusionTable(FileInfo exclusionlist)
        {
            exclusionSet = WordlistLoader.GetWordSet(exclusionlist);
            PreviousTokenStream = null;
        }

        /// <summary>
        /// Creates a TokenStream which tokenizes all the text in the provided TextReader. 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="reader"></param>
        /// <returns>A TokenStream build from a StandardTokenizer filtered with StandardFilter, StopFilter, GermanStemFilter</returns>
        public override TokenStream TokenStream(String fieldName, TextReader reader)
        {
            TokenStream result = new StandardTokenizer(matchVersion, reader);
            result = new StandardFilter(result);
            result = new LowerCaseFilter(result);
            result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion), result, stopSet);
            result = new GermanStemFilter(result, exclusionSet, _normalizeDin2);
            return result;
        }
    }
}