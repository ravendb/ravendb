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
using System.Text;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;

namespace Lucene.Net.Analysis.Shingle
{
    /*
 * <p>A ShingleFilter constructs shingles (token n-grams) from a token stream.
 * In other words, it creates combinations of tokens as a single token.
 *
 * <p>For example, the sentence "please divide this sentence into shingles"
 * might be tokenized into shingles "please divide", "divide this",
 * "this sentence", "sentence into", and "into shingles".
 *
 * <p>This filter handles position increments > 1 by inserting filler tokens
 * (tokens with termtext "_"). It does not handle a position increment of 0.
 */
    public sealed class ShingleFilter : TokenFilter
    {

        private LinkedList<State> shingleBuf = new LinkedList<State>();
        private StringBuilder[] shingles;
        private String tokenType = "shingle";

        /*
         * filler token for when positionIncrement is more than 1
         */
        public static readonly char[] FILLER_TOKEN = { '_' };


        /*
         * default maximum shingle size is 2.
         */
        public const int DEFAULT_MAX_SHINGLE_SIZE = 2;

        /*
         * The string to use when joining adjacent tokens to form a shingle
         */
        public const String TOKEN_SEPARATOR = " ";

        /*
         * By default, we output unigrams (individual tokens) as well as shingles
         * (token n-grams).
         */
        private bool outputUnigrams = true;

        /*
         * maximum shingle size (number of tokens)
         */
        private int maxShingleSize;

        /*
         * Constructs a ShingleFilter with the specified single size from the
         * {@link TokenStream} <c>input</c>
         *
         * @param input input stream
         * @param maxShingleSize maximum shingle size produced by the filter.
         */
        public ShingleFilter(TokenStream input, int maxShingleSize)
            : base(input)
        {
            SetMaxShingleSize(maxShingleSize);
            this.termAtt = AddAttribute<ITermAttribute>(); ;
            this.offsetAtt = AddAttribute<IOffsetAttribute>(); ;
            this.posIncrAtt = AddAttribute<IPositionIncrementAttribute>(); ;
            this.typeAtt = AddAttribute<ITypeAttribute>(); ;
        }

        /*
         * Construct a ShingleFilter with default shingle size.
         *
         * @param input input stream
         */
        public ShingleFilter(TokenStream input)
            : this(input, DEFAULT_MAX_SHINGLE_SIZE)
        {
        }

        /*
         * Construct a ShingleFilter with the specified token type for shingle tokens.
         *
         * @param input input stream
         * @param tokenType token type for shingle tokens
         */
        public ShingleFilter(TokenStream input, String tokenType)
            : this(input, DEFAULT_MAX_SHINGLE_SIZE)
        {
            setTokenType(tokenType);
        }

        /*
         * Set the type of the shingle tokens produced by this filter.
         * (default: "shingle")
         *
         * @param tokenType token tokenType
         */
        public void setTokenType(String tokenType)
        {
            this.tokenType = tokenType;
        }

        /*
         * Shall the output stream contain the input tokens (unigrams) as well as
         * shingles? (default: true.)
         *
         * @param outputUnigrams Whether or not the output stream shall contain
         * the input tokens (unigrams)
         */
        public void SetOutputUnigrams(bool outputUnigrams)
        {
            this.outputUnigrams = outputUnigrams;
        }

        /*
         * Set the max shingle size (default: 2)
         *
         * @param maxShingleSize max size of output shingles
         */
        public void SetMaxShingleSize(int maxShingleSize)
        {
            if (maxShingleSize < 2)
            {
                throw new ArgumentException("Max shingle size must be >= 2");
            }
            shingles = new StringBuilder[maxShingleSize];
            for (int i = 0; i < shingles.Length; i++)
            {
                shingles[i] = new StringBuilder();
            }
            this.maxShingleSize = maxShingleSize;
        }

        /*
         * Clear the StringBuilders that are used for storing the output shingles.
         */
        private void ClearShingles()
        {
            for (int i = 0; i < shingles.Length; i++)
            {
                shingles[i].Length = 0;
            }
        }

        private AttributeSource.State nextToken;
        private int shingleBufferPosition;
        private int[] endOffsets;

        /* (non-Javadoc)
         * @see org.apache.lucene.analysis.TokenStream#next()
         */
        public sealed override bool IncrementToken()
        {
            while (true)
            {
                if (nextToken == null)
                {
                    if (!FillShingleBuffer())
                    {
                        return false;
                    }
                }

                nextToken = shingleBuf.First.Value;

                if (outputUnigrams)
                {
                    if (shingleBufferPosition == 0)
                    {
                        RestoreState(nextToken);
                        posIncrAtt.PositionIncrement = 1;
                        shingleBufferPosition++;
                        return true;
                    }
                }
                else if (shingleBufferPosition % this.maxShingleSize == 0)
                {
                    shingleBufferPosition++;
                }

                if (shingleBufferPosition < shingleBuf.Count)
                {
                    RestoreState(nextToken);
                    typeAtt.Type = tokenType;
                    offsetAtt.SetOffset(offsetAtt.StartOffset, endOffsets[shingleBufferPosition]);
                    StringBuilder buf = shingles[shingleBufferPosition];
                    int termLength = buf.Length;
                    char[] TermBuffer = termAtt.TermBuffer();
                    if (TermBuffer.Length < termLength)
                        TermBuffer = termAtt.ResizeTermBuffer(termLength);
                    buf.CopyTo(0, TermBuffer, 0, termLength);
                    termAtt.SetTermLength(termLength);
                    if ((!outputUnigrams) && shingleBufferPosition % this.maxShingleSize == 1)
                    {
                        posIncrAtt.PositionIncrement = 1;
                    }
                    else
                    {
                        posIncrAtt.PositionIncrement = 0;
                    }
                    shingleBufferPosition++;
                    if (shingleBufferPosition == shingleBuf.Count)
                    {
                        nextToken = null;
                        shingleBufferPosition = 0;
                    }
                    return true;
                }
                else
                {
                    nextToken = null;
                    shingleBufferPosition = 0;
                }
            }
        }

        private int numFillerTokensToInsert;
        private AttributeSource.State currentToken;
        private bool hasCurrentToken;

        private ITermAttribute termAtt;
        private IOffsetAttribute offsetAtt;
        private IPositionIncrementAttribute posIncrAtt;
        private ITypeAttribute typeAtt;

        /*
         * Get the next token from the input stream and push it on the token buffer.
         * If we encounter a token with position increment > 1, we put filler tokens
         * on the token buffer.
         * <p/>
         * Returns null when the end of the input stream is reached.
         * @return the next token, or null if at end of input stream
         * @throws IOException if the input stream has a problem
         */
        private bool GetNextToken()
        {

            while (true)
            {
                if (numFillerTokensToInsert > 0)
                {
                    if (currentToken == null)
                    {
                        currentToken = CaptureState();
                    }
                    else
                    {
                        RestoreState(currentToken);
                    }
                    numFillerTokensToInsert--;
                    // A filler token occupies no space
                    offsetAtt.SetOffset(offsetAtt.StartOffset, offsetAtt.StartOffset);
                    termAtt.SetTermBuffer(FILLER_TOKEN, 0, FILLER_TOKEN.Length);
                    return true;
                }

                if (hasCurrentToken)
                {
                    if (currentToken != null)
                    {
                        RestoreState(currentToken);
                        currentToken = null;
                    }
                    hasCurrentToken = false;
                    return true;
                }

                if (!input.IncrementToken()) return false;
                hasCurrentToken = true;

                if (posIncrAtt.PositionIncrement > 1)
                {
                    numFillerTokensToInsert = posIncrAtt.PositionIncrement - 1;
                }
            }
        }

        /*
         * Fill the output buffer with new shingles.
         *
         * @throws IOException if there's a problem getting the next token
         */
        private bool FillShingleBuffer()
        {
            bool addedToken = false;
            /*
             * Try to fill the shingle buffer.
             */
            do
            {
                if (GetNextToken())
                {
                    shingleBuf.AddLast(CaptureState());
                    if (shingleBuf.Count > maxShingleSize)
                    {
                        shingleBuf.RemoveFirst();
                    }
                    addedToken = true;
                }
                else
                {
                    break;
                }
            } while (shingleBuf.Count < maxShingleSize);

            if (shingleBuf.Count == 0)
            {
                return false;
            }

            /*
             * If no new token could be added to the shingle buffer, we have reached
             * the end of the input stream and have to discard the least recent token.
             */
            if (!addedToken)
            {
                shingleBuf.RemoveFirst();
            }

            if (shingleBuf.Count == 0)
            {
                return false;
            }

            ClearShingles();

            endOffsets = new int[shingleBuf.Count];
            // Set all offsets to 0
            endOffsets.Initialize();

            int i = 0;
            for (IEnumerator<State> it = shingleBuf.GetEnumerator(); it.MoveNext(); )
            {
                RestoreState(it.Current);
                for (int j = i; j < shingles.Length; j++)
                {
                    if (shingles[j].Length != 0)
                    {
                        shingles[j].Append(TOKEN_SEPARATOR);
                    }
                    shingles[j].Append(termAtt.TermBuffer().Take(termAtt.TermLength()).ToArray());
                }

                endOffsets[i] = offsetAtt.EndOffset;
                i++;
            }

            return true;
        }

        public override void Reset()
        {
            base.Reset();
            nextToken = null;
            shingleBufferPosition = 0;
            shingleBuf.Clear();
            numFillerTokensToInsert = 0;
            currentToken = null;
            hasCurrentToken = false;
        }
    }
}