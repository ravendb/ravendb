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
using System.IO;
using System.Collections;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;

namespace Lucene.Net.Analysis.NGram
{
    public static class SideExtensions
    {
        public static string GetLabel(this Side theSide)
        {
            switch(theSide)
            {
                case Side.FRONT:
                    return "front";
                case Side.BACK:
                    return "back";
                default:
                    throw new ArgumentException(string.Format("{0} is not a valid value for EdgeNGramTokenFilter.Side", theSide));
            }
        }

        public static Side GetSide(string sideName)
        {
            if (Side.FRONT.GetLabel() == sideName)
            {
                return Side.FRONT;
            }

            if (Side.BACK.GetLabel() == sideName)
            {
                return Side.BACK;
            }

            return (Side)(-1); // TODO: returning null instead of null?  Should an exception be thrown instead?
        }
    }

    /// <summary>
    /// Specifies which side of the input the n-gram should be generated from
    /// </summary>
    public enum Side
    {
        FRONT,
        BACK
    }

    /*
     * Tokenizes the given token into n-grams of given size(s).
     * <p>
     * This <see cref="TokenFilter"/> create n-grams from the beginning edge or ending edge of a input token.
     * </p>
     */
    public sealed class EdgeNGramTokenFilter : TokenFilter
    {
        public static Side DEFAULT_SIDE = Side.FRONT;
        public static int DEFAULT_MAX_GRAM_SIZE = 1;
        public static int DEFAULT_MIN_GRAM_SIZE = 1;

        private int minGram;
        private int maxGram;
        private Side side;
        private char[] curTermBuffer;
        private int curTermLength;
        private int curGramSize;
        private int tokStart;

        private ITermAttribute termAtt;
        private IOffsetAttribute offsetAtt;


        protected EdgeNGramTokenFilter(TokenStream input) : base(input)
        {
            this.termAtt = AddAttribute<ITermAttribute>();
            this.offsetAtt = AddAttribute<IOffsetAttribute>();
        }

        /*
         * Creates EdgeNGramTokenFilter that can generate n-grams in the sizes of the given range
         *
         * <param name="input"><see cref="TokenStream"/> holding the input to be tokenized</param>
         * <param name="side">the <see cref="Side"/> from which to chop off an n-gram</param>
         * <param name="minGram">the smallest n-gram to generate</param>
         * <param name="maxGram">the largest n-gram to generate</param>
         */
        public EdgeNGramTokenFilter(TokenStream input, Side side, int minGram, int maxGram)
            : base(input)
        {


            if (side != Side.FRONT && side != Side.BACK)
            {
                throw new System.ArgumentException("sideLabel must be either front or back");
            }

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
            this.side = side;
            this.termAtt = AddAttribute<ITermAttribute>();
            this.offsetAtt = AddAttribute<IOffsetAttribute>();
        }

        /*
         * Creates EdgeNGramTokenFilter that can generate n-grams in the sizes of the given range
         *
         * <param name="input"><see cref="TokenStream"/> holding the input to be tokenized</param>
         * <param name="sideLabel">the name of the <see cref="Side"/> from which to chop off an n-gram</param>
         * <param name="minGram">the smallest n-gram to generate</param>
         * <param name="maxGram">the largest n-gram to generate</param>
         */
        public EdgeNGramTokenFilter(TokenStream input, string sideLabel, int minGram, int maxGram)
            : this(input, SideExtensions.GetSide(sideLabel), minGram, maxGram)
        {
        }

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
                        tokStart = offsetAtt.StartOffset;
                    }
                }
                if (curGramSize <= maxGram)
                {
                    if (!(curGramSize > curTermLength         // if the remaining input is too short, we can't generate any n-grams
                        || curGramSize > maxGram))
                    {       // if we have hit the end of our n-gram size range, quit
                        // grab gramSize chars from front or back
                        int start = side == Side.FRONT ? 0 : curTermLength - curGramSize;
                        int end = start + curGramSize;
                        ClearAttributes();
                        offsetAtt.SetOffset(tokStart + start, tokStart + end);
                        termAtt.SetTermBuffer(curTermBuffer, start, curGramSize);
                        curGramSize++;
                        return true;
                    }
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