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
    public sealed class NGramTokenizer : Tokenizer
    {
        public static int DEFAULT_MIN_NGRAM_SIZE = 1;
        public static int DEFAULT_MAX_NGRAM_SIZE = 2;

        private int minGram, maxGram;
        private int gramSize;
        private int pos = 0;
        private int inLen;
        private string inStr;
        private bool started = false;

        private ITermAttribute termAtt;
        private IOffsetAttribute offsetAtt;

        /*
         * Creates NGramTokenizer with given min and max n-grams.
         * <param name="input"><see cref="TextReader"/> holding the input to be tokenized</param>
         * <param name="minGram">the smallest n-gram to generate</param>
         * <param name="maxGram">the largest n-gram to generate</param>
         */
        public NGramTokenizer(TextReader input, int minGram, int maxGram)
            : base(input)
        {
            init(minGram, maxGram);
        }

        /*
         * Creates NGramTokenizer with given min and max n-grams.
         * <param name="source"><see cref="AttributeSource"/> to use</param>
         * <param name="input"><see cref="TextReader"/> holding the input to be tokenized</param>
         * <param name="minGram">the smallest n-gram to generate</param>
         * <param name="maxGram">the largest n-gram to generate</param>
         */
        public NGramTokenizer(AttributeSource source, TextReader input, int minGram, int maxGram)
            : base(source, input)
        {
            init(minGram, maxGram);
        }

        /*
         * Creates NGramTokenizer with given min and max n-grams.
         * <param name="factory"><see cref="AttributeSource.AttributeFactory"/> to use</param>
         * <param name="input"><see cref="TextReader"/> holding the input to be tokenized</param>
         * <param name="minGram">the smallest n-gram to generate</param>
         * <param name="maxGram">the largest n-gram to generate</param>
         */
        public NGramTokenizer(AttributeFactory factory, TextReader input, int minGram, int maxGram)
            : base(factory, input)
        {
            init(minGram, maxGram);
        }

        /*
         * Creates NGramTokenizer with default min and max n-grams.
         * <param name="input"><see cref="TextReader"/> holding the input to be tokenized</param>
         */
        public NGramTokenizer(TextReader input)
            : this(input, DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE)
        {

        }

        private void init(int minGram, int maxGram)
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

        /* Returns the next token in the stream, or null at EOS. */
        public override bool IncrementToken()
        {
            ClearAttributes();
            if (!started)
            {
                started = true;
                gramSize = minGram;
                char[] chars = new char[1024];
                inStr = input.ReadToEnd();  // remove any trailing empty strings 
                inLen = inStr.Length;
            }

            if (pos + gramSize > inLen)
            {            // if we hit the end of the string
                pos = 0;                           // reset to beginning of string
                gramSize++;                        // increase n-gram size
                if (gramSize > maxGram)            // we are done
                    return false;
                if (pos + gramSize > inLen)
                    return false;
            }

            int oldPos = pos;
            pos++;
            termAtt.SetTermBuffer(inStr, oldPos, gramSize);
            offsetAtt.SetOffset(CorrectOffset(oldPos), CorrectOffset(oldPos + gramSize));
            return true;
        }

        public override void End()
        {
            // set offset
            int finalOffset = inLen;
            this.offsetAtt.SetOffset(finalOffset, finalOffset);
        }

        public override void Reset(TextReader input)
        {
            base.Reset(input);
            Reset();
        }

        public override void Reset()
        {
            base.Reset();
            started = false;
            pos = 0;
        }
    }
}