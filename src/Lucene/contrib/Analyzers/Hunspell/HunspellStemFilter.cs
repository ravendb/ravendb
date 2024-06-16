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
using System.Linq;
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Analysis.Hunspell {
    /// <summary>
    ///   TokenFilter that uses hunspell affix rules and words to stem tokens.  Since hunspell supports a
    ///   word having multiple stems, this filter can emit multiple tokens for each consumed token.
    /// </summary>
    public class HunspellStemFilter : TokenFilter {
        private readonly ITermAttribute _termAtt;
        private readonly IPositionIncrementAttribute _posIncAtt;
        private readonly HunspellStemmer _stemmer;

        private readonly Queue<HunspellStem> _buffer = new Queue<HunspellStem>();
        private State _savedState;

        private readonly Boolean _dedup;

        /// <summary>
        ///   Creates a new HunspellStemFilter that will stem tokens from the given TokenStream using
        ///   affix rules in the provided HunspellDictionary.
        /// </summary>
        /// <param name="input">TokenStream whose tokens will be stemmed.</param>
        /// <param name="dictionary">HunspellDictionary containing the affix rules and words that will be used to stem the tokens.</param>
        /// <param name="dedup">true if only unique terms should be output.</param>
        public HunspellStemFilter(TokenStream input, HunspellDictionary dictionary, Boolean dedup = true)
            : base(input) {
            _posIncAtt = AddAttribute<IPositionIncrementAttribute>();
            _termAtt = AddAttribute<ITermAttribute>();

            _dedup = dedup;
            _stemmer = new HunspellStemmer(dictionary);
        }

        public override Boolean IncrementToken() {
            if (_buffer.Any()) {
                var nextStem = _buffer.Dequeue();

                RestoreState(_savedState);
                _posIncAtt.PositionIncrement = 0;
                _termAtt.SetTermBuffer(nextStem.Stem, 0, nextStem.StemLength);
                return true;
            }

            if (!input.IncrementToken())
                return false;

            var newTerms = _dedup
                               ? _stemmer.UniqueStems(_termAtt.Term)
                               : _stemmer.Stem(_termAtt.Term);
            foreach (var newTerm in newTerms)
                _buffer.Enqueue(newTerm);

            if (_buffer.Count == 0)
                // we do not know this word, return it unchanged
                return true;

            var stem = _buffer.Dequeue();
            _termAtt.SetTermBuffer(stem.Stem, 0, stem.StemLength);

            if (_buffer.Count > 0)
                _savedState = CaptureState();

            return true;
        }

        public override void Reset() {
            base.Reset();

            _buffer.Clear();
        }
    }
}
