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
using System.IO;
using System.Text;
using System.Collections;

using Lucene.Net.Analysis;

namespace Lucene.Net.Analysis.Cn
{
    /// <summary>
    /// An <see cref="Analyzer"/> that tokenizes text with <see cref="ChineseTokenizer"/> and
    /// filters with <see cref="ChineseFilter"/>
    /// </summary>
    public class ChineseAnalyzer : Analyzer
    {

        public ChineseAnalyzer()
        {
        }

        /// <summary>
        /// Creates a TokenStream which tokenizes all the text in the provided Reader.
        /// </summary>
        /// <returns>A TokenStream build from a ChineseTokenizer filtered with ChineseFilter.</returns>
        public override sealed TokenStream TokenStream(String fieldName, TextReader reader)
        {
            TokenStream result = new ChineseTokenizer(reader);
            result = new ChineseFilter(result);
            return result;
        }

        private class SavedStreams
        {
            protected internal Tokenizer source;
            protected internal TokenStream result;
        };

        /// <summary>
        /// Returns a (possibly reused) <see cref="TokenStream"/> which tokenizes all the text in the
        /// provided <see cref="TextReader"/>.
        /// </summary>
        /// <returns>
        ///   A <see cref="TokenStream"/> built from a <see cref="ChineseTokenizer"/> 
        ///   filtered with <see cref="ChineseFilter"/>.
        /// </returns>
        public override TokenStream ReusableTokenStream(String fieldName, TextReader reader)
        {
            /* tokenStream() is final, no back compat issue */
            SavedStreams streams = (SavedStreams) PreviousTokenStream;
            if (streams == null)
            {
                streams = new SavedStreams();
                streams.source = new ChineseTokenizer(reader);
                streams.result = new ChineseFilter(streams.source);
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
