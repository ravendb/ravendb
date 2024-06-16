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
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Analysis.Compound
{

    /*
     * Base class for decomposition token filters.
     */
    public abstract class CompoundWordTokenFilterBase : TokenFilter
    {
        /*
         * The default for minimal word length that gets decomposed
         */
        public static readonly int DEFAULT_MIN_WORD_SIZE = 5;

        /*
         * The default for minimal length of subwords that get propagated to the output of this filter
         */
        public static readonly int DEFAULT_MIN_SUBWORD_SIZE = 2;

        /*
         * The default for maximal length of subwords that get propagated to the output of this filter
         */
        public static readonly int DEFAULT_MAX_SUBWORD_SIZE = 15;

        protected readonly CharArraySet dictionary;
        protected readonly LinkedList<Token> tokens;
        protected readonly int minWordSize;
        protected readonly int minSubwordSize;
        protected readonly int maxSubwordSize;
        protected readonly bool onlyLongestMatch;

        private ITermAttribute termAtt;
        private IOffsetAttribute offsetAtt;
        private IFlagsAttribute flagsAtt;
        private IPositionIncrementAttribute posIncAtt;
        private ITypeAttribute typeAtt;
        private IPayloadAttribute payloadAtt;

        private readonly Token wrapper = new Token();

        protected CompoundWordTokenFilterBase(TokenStream input, String[] dictionary, int minWordSize, int minSubwordSize, int maxSubwordSize, bool onlyLongestMatch)
            : this(input, MakeDictionary(dictionary), minWordSize, minSubwordSize, maxSubwordSize, onlyLongestMatch)
        {

        }

        protected CompoundWordTokenFilterBase(TokenStream input, String[] dictionary, bool onlyLongestMatch)
            : this(input, MakeDictionary(dictionary), DEFAULT_MIN_WORD_SIZE, DEFAULT_MIN_SUBWORD_SIZE, DEFAULT_MAX_SUBWORD_SIZE, onlyLongestMatch)
        {

        }

        protected CompoundWordTokenFilterBase(TokenStream input, ISet<string> dictionary, bool onlyLongestMatch)
            : this(input, dictionary, DEFAULT_MIN_WORD_SIZE, DEFAULT_MIN_SUBWORD_SIZE, DEFAULT_MAX_SUBWORD_SIZE, onlyLongestMatch)
        {

        }

        protected CompoundWordTokenFilterBase(TokenStream input, String[] dictionary)
            : this(input, MakeDictionary(dictionary), DEFAULT_MIN_WORD_SIZE, DEFAULT_MIN_SUBWORD_SIZE, DEFAULT_MAX_SUBWORD_SIZE, false)
        {

        }

        protected CompoundWordTokenFilterBase(TokenStream input, ISet<string> dictionary)
            : this(input, dictionary, DEFAULT_MIN_WORD_SIZE, DEFAULT_MIN_SUBWORD_SIZE, DEFAULT_MAX_SUBWORD_SIZE, false)
        {

        }

        protected CompoundWordTokenFilterBase(TokenStream input, ISet<string> dictionary, int minWordSize, int minSubwordSize, int maxSubwordSize, bool onlyLongestMatch)
            : base(input)
        {
            this.tokens = new LinkedList<Token>();
            this.minWordSize = minWordSize;
            this.minSubwordSize = minSubwordSize;
            this.maxSubwordSize = maxSubwordSize;
            this.onlyLongestMatch = onlyLongestMatch;

            if (dictionary is CharArraySet)
            {
                this.dictionary = (CharArraySet)dictionary;
            }
            else
            {
                this.dictionary = new CharArraySet(dictionary.Count, false);
                AddAllLowerCase(this.dictionary, dictionary);
            }

            termAtt = AddAttribute<ITermAttribute>();
            offsetAtt = AddAttribute<IOffsetAttribute>();
            flagsAtt = AddAttribute<IFlagsAttribute>();
            posIncAtt = AddAttribute<IPositionIncrementAttribute>();
            typeAtt = AddAttribute<ITypeAttribute>();
            payloadAtt = AddAttribute<IPayloadAttribute>();
        }

        /*
         * Create a set of words from an array
         * The resulting Set does case insensitive matching
         * TODO We should look for a faster dictionary lookup approach.
         * @param dictionary 
         * @return {@link Set} of lowercased terms 
         */
        public static ISet<string> MakeDictionary(String[] dictionary)
        {
            // is the below really case insensitive? 
            CharArraySet dict = new CharArraySet(dictionary.Length, false);
            AddAllLowerCase(dict, dictionary);
            return dict;
        }

        private void setToken(Token token)
        {
            ClearAttributes();
            termAtt.SetTermBuffer(token.TermBuffer(), 0, token.TermLength());
            flagsAtt.Flags = token.Flags;
            typeAtt.Type = token.Type;
            offsetAtt.SetOffset(token.StartOffset, token.EndOffset);
            posIncAtt.PositionIncrement = token.PositionIncrement;
            payloadAtt.Payload = token.Payload;
        }

        public sealed override bool IncrementToken()
        {
            if (tokens.Count > 0)
            {
                setToken((Token)tokens.First.Value);
                tokens.RemoveFirst();
                return true;
            }

            if (input.IncrementToken() == false)
                return false;

            wrapper.SetTermBuffer(termAtt.TermBuffer(), 0, termAtt.TermLength());
            wrapper.StartOffset = offsetAtt.StartOffset;
            wrapper.EndOffset = offsetAtt.EndOffset;
            wrapper.Flags = flagsAtt.Flags;
            wrapper.Type = typeAtt.Type;
            wrapper.PositionIncrement = posIncAtt.PositionIncrement;
            wrapper.Payload = payloadAtt.Payload;

            Decompose(wrapper);

            if (tokens.Count > 0)
            {
                setToken(tokens.First.Value);
                tokens.RemoveFirst();
                return true;
            }
            else
            {
                return false;
            }
        }

        protected static void AddAllLowerCase(ISet<string> target, ICollection<string> col)
        {
            foreach (var str in col)
            {
                target.Add(str.ToLower(System.Globalization.CultureInfo.GetCultureInfo("en-US")));
            }
        }

        protected static char[] MakeLowerCaseCopy(char[] buffer)
        {
            char[] result = new char[buffer.Length];
            Array.Copy(buffer, 0, result, 0, buffer.Length);

            for (int i = 0; i < buffer.Length; ++i)
            {
                result[i] = char.ToLower(buffer[i]); // Is java invariant?
            }

            return result;
        }

        protected Token CreateToken(int offset, int length,
            Token prototype)
        {
            int newStart = prototype.StartOffset + offset;
            Token t = prototype.Clone(prototype.TermBuffer(), offset, length, newStart, newStart + length);
            t.PositionIncrement = 0;
            return t;
        }

        protected void Decompose(Token token)
        {
            // In any case we give the original token back
            tokens.AddLast((Token)token.Clone());

            // Only words longer than minWordSize get processed
            if (token.TermLength() < this.minWordSize)
            {
                return;
            }

            DecomposeInternal(token);
        }

        protected abstract void DecomposeInternal(Token token);

        public override void Reset()
        {
            base.Reset();
            tokens.Clear();
        }
    }
}