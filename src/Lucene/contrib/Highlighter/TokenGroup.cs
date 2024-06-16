/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Search.Highlight
{
    /// <summary> One, or several overlapping tokens, along with the score(s) and the
    /// scope of the original text
    /// </summary>
    public class TokenGroup
    {
        private static readonly int MAX_NUM_TOKENS_PER_GROUP = 50;

        private Token[] tokens = new Token[MAX_NUM_TOKENS_PER_GROUP];
        private float[] scores = new float[MAX_NUM_TOKENS_PER_GROUP];
        private int startOffset = 0;
        private int endOffset = 0;
        private float tot;

        public int MatchStartOffset { get; private set; }
        public int MatchEndOffset { get; private set; }
        public int NumTokens { get; private set; }

        private IOffsetAttribute offsetAtt;
        private ITermAttribute termAtt;

        public TokenGroup(TokenStream tokenStream)
        {
            NumTokens = 0;
            offsetAtt = tokenStream.AddAttribute<IOffsetAttribute>();
            termAtt = tokenStream.AddAttribute<ITermAttribute>();
        }

        protected internal void AddToken(float score)
        {
            if (NumTokens < MAX_NUM_TOKENS_PER_GROUP)
            {
                int termStartOffset = offsetAtt.StartOffset;
                int termEndOffset = offsetAtt.EndOffset;
                if (NumTokens == 0)
                {
                    startOffset = MatchStartOffset = termStartOffset;
                    endOffset = MatchEndOffset = termEndOffset;
                    tot += score;
                }
                else
                {
                    startOffset = Math.Min(startOffset, termStartOffset);
                    endOffset = Math.Max(endOffset, termEndOffset);
                    if (score > 0)
                    {
                        if (tot == 0)
                        {
                            MatchStartOffset = offsetAtt.StartOffset;
                            MatchEndOffset = offsetAtt.EndOffset;
                        }
                        else
                        {
                            MatchStartOffset = Math.Min(MatchStartOffset, termStartOffset);
                            MatchEndOffset = Math.Max(MatchEndOffset, termEndOffset);
                        }
                        tot += score;
                    }
                }
                Token token = new Token(termStartOffset, termEndOffset);
                token.SetTermBuffer(termAtt.Term);
                tokens[NumTokens] = token;
                scores[NumTokens] = score;
                NumTokens++;
            }
        }

        protected internal bool IsDistinct()
        {
            return offsetAtt.StartOffset >= endOffset;
        }

        protected internal void Clear()
        {
            NumTokens = 0;
            tot = 0;
        }


        /// <summary>
        /// the "n"th token
        /// </summary>
        /// <param name="index">a value between 0 and numTokens -1</param>
        public Token GetToken(int index)
        {
            return tokens[index];
        }

        /// <summary>
        /// the "n"th score
        /// </summary>
        /// <param name="index">a value between 0 and numTokens -1</param>
        public float GetScore(int index)
        {
            return scores[index];
        }

        /// <summary>
        /// the end position in the original text
        /// </summary>
        public int EndOffset
        {
            get { return endOffset; }
        }
        
        /// <summary>
        /// The start position in the original text
        /// </summary>
        public int StartOffset
        {
            get { return startOffset; }
        }

        /// <summary>
        /// All tokens' scores summed up
        /// </summary>
        public float TotalScore
        {
            get { return tot; }
        }
    }
}
