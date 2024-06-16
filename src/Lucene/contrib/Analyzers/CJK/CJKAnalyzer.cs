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
using Lucene.Net.Analysis;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis.CJK
{
    /// <summary>
    /// Filters CJKTokenizer with StopFilter.
    /// 
    /// <author>Che, Dong</author>
    /// </summary>
    public class CJKAnalyzer : Analyzer
    {
        //~ Static fields/initializers ---------------------------------------------

        /// <summary>
        /// An array containing some common English words that are not usually
        /// useful for searching. and some double-byte interpunctions.....
        /// </summary>
        // TODO make this final in 3.1 -
        // this might be revised and merged with StopFilter stop words too
        [Obsolete("use GetDefaultStopSet() instead")] public static String[] STOP_WORDS =
            {
                "a", "and", "are", "as", "at", "be",
                "but", "by", "for", "if", "in",
                "into", "is", "it", "no", "not",
                "of", "on", "or", "s", "such", "t",
                "that", "the", "their", "then",
                "there", "these", "they", "this",
                "to", "was", "will", "with", "",
                "www"
            };

        //~ Instance fields --------------------------------------------------------

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
                CharArraySet.UnmodifiableSet(new CharArraySet((IEnumerable<string>)STOP_WORDS, false));
        }

        /// <summary>
        /// stop word list
        /// </summary>
        private ISet<string> stopTable;

        private readonly Version matchVersion;

        //~ Constructors -----------------------------------------------------------

        public CJKAnalyzer(Version matchVersion)
            : this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET)
        {

        }

        public CJKAnalyzer(Version matchVersion, ISet<string> stopWords)
        {
            stopTable = CharArraySet.UnmodifiableSet(CharArraySet.Copy(stopWords));
            this.matchVersion = matchVersion;
        }

        /// <summary>
        /// Builds an analyzer which removes words in the provided array.
        /// </summary>
        /// <param name="stopWords">stop word array</param>
        public CJKAnalyzer(Version matchVersion, params string[] stopWords)
        {
            stopTable = StopFilter.MakeStopSet(stopWords);
            this.matchVersion = matchVersion;
        }

        //~ Methods ----------------------------------------------------------------

        /// <summary>
        /// get token stream from input
        /// </summary>
        /// <param name="fieldName">lucene field name</param>
        /// <param name="reader">input reader</param>
        /// <returns>Token Stream</returns>
        public override sealed TokenStream TokenStream(String fieldName, TextReader reader)
        {
            return new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                  new CJKTokenizer(reader), stopTable);
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
         * @param fieldName lucene field name
         * @param reader    Input {@link Reader}
         * @return A {@link TokenStream} built from {@link CJKTokenizer}, filtered with
         *    {@link StopFilter}
         */
        public override sealed TokenStream ReusableTokenStream(String fieldName, TextReader reader)
        {
            /* tokenStream() is final, no back compat issue */
            SavedStreams streams = (SavedStreams) PreviousTokenStream;
            if (streams == null)
            {
                streams = new SavedStreams();
                streams.source = new CJKTokenizer(reader);
                streams.result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                                streams.source, stopTable);
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
