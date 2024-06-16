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
using System.Text;
using Lucene.Net.Analysis.Standard;
using Version=Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis.Th
{
    /*
     * {@link Analyzer} for Thai language. It uses {@link java.text.BreakIterator} to break words.
     * @version 0.2
     *
     * <p><b>NOTE</b>: This class uses the same {@link Version}
     * dependent settings as {@link StandardAnalyzer}.</p>
     */
    public class ThaiAnalyzer : Analyzer
    {
        private readonly Version matchVersion;

        public ThaiAnalyzer(Version matchVersion)
        {
            SetOverridesTokenStreamMethod<ThaiAnalyzer>();
            this.matchVersion = matchVersion;
        }

        public override TokenStream TokenStream(String fieldName, TextReader reader)
        {
            TokenStream ts = new StandardTokenizer(matchVersion, reader);
            ts = new StandardFilter(ts);
            ts = new ThaiWordFilter(ts);
            ts = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                ts, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
            return ts;
        }

        private class SavedStreams
        {
            protected internal Tokenizer source;
            protected internal TokenStream result;
        };

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
                streams.result = new ThaiWordFilter(streams.result);
                streams.result = new StopFilter(StopFilter.GetEnablePositionIncrementsVersionDefault(matchVersion),
                                                streams.result, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
                PreviousTokenStream = streams;
            }
            else
            {
                streams.source.Reset(reader);
                streams.result.Reset(); // reset the ThaiWordFilter's state
            }
            return streams.result;
        }
    }
}
