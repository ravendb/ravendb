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

using System.IO;
using System.Collections;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;

namespace Lucene.Net.Analysis.NGram
{
    /*
     * Tokenizes the input into n-grams of the given size(s).
     */
    public sealed class NGramTokenFilter : TokenFilter
    {
        public static int DEFAULT_MIN_NGRAM_SIZE = 1;
        public static int DEFAULT_MAX_NGRAM_SIZE = 2;

        private int minGram, maxGram;

        private char[] curTermBuffer;
        private int curTermLength;
        private int curGramSize;
        private int curPos;
        private int tokStart;

        private ITermAttribute termAtt;
        private IOffsetAttribute offsetAtt;

        /*
         * Creates NGramTokenFilter with given min and max n-grams.
         * <param name="input"><see cref="TokenStream"/> holding the input to be tokenized</param>
         * <param name="minGram">the smallest n-gram to generate</param>
         * <param name="maxGram">the largest n-gram to generate</param>
         */
        public NGramTokenFilter(TokenStream input, int minGram, int maxGram)
            : base(input)
        {

            if (minGram < 1)
            {
                throw new System.ArgumentException("minGram must be greater than zero");
            }
            if (minGram > maxGram)
            {
                throw new System.ArgumentException("minGram must not be greater than maxGram");
            }
            this.minGram = minGram;
            this.maxGram = maxGram;

            this.termAtt = AddAttribute<ITermAttribute>();
            this.offsetAtt = AddAttribute<IOffsetAttribute>();
        }

        /*
         * Creates NGramTokenFilter with default min and max n-grams.
         * <param name="input"><see cref="TokenStream"/> holding the input to be tokenized</param>
         */
        public NGramTokenFilter(TokenStream input)
            : this(input, DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE)
        {

        }

        /* Returns the next token in the stream, or null at EOS. */
        public override bool IncrementToken()
        {
            while (true)
            {
                if (curTermBuffer == null)
                {
                    if (!input.IncrementToken())
                    {
                        return false;
                    }
                    else
                    {
                        curTermBuffer = (char[])termAtt.TermBuffer().Clone();
                        curTermLength = termAtt.TermLength();
                        curGramSize = minGram;
                        curPos = 0;
                        tokStart = offsetAtt.StartOffset;
                    }
                }
                while (curGramSize <= maxGram)
                {
                    while (curPos + curGramSize <= curTermLength)
                    {     // while there is input
                        ClearAttributes();
                        termAtt.SetTermBuffer(curTermBuffer, curPos, curGramSize);
                        offsetAtt.SetOffset(tokStart + curPos, tokStart + curPos + curGramSize);
                        curPos++;
                        return true;
                    }
                    curGramSize++;                         // increase n-gram size
                    curPos = 0;
                }
                curTermBuffer = null;
            }
        }

        public override void Reset()
        {
            base.Reset();
            curTermBuffer = null;
        }
    }
}