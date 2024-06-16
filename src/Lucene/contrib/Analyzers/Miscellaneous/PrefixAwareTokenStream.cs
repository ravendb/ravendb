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

using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;

namespace Lucene.Net.Analysis.Miscellaneous
{
    /// <summary>
    /// Joins two token streams and leaves the last token of the first stream available
    /// to be used when updating the token values in the second stream based on that token.
    /// 
    /// The default implementation adds last prefix token end offset to the suffix token start and end offsets.
    /// <p/>
    /// <b>NOTE:</b> This filter might not behave correctly if used with custom Attributes, i.e. Attributes other than
    /// the ones located in Lucene.Net.Analysis.TokenAttributes. 
    /// </summary>
    public class PrefixAwareTokenFilter : TokenStream
    {
        private readonly IFlagsAttribute _flagsAtt;
        private readonly IOffsetAttribute _offsetAtt;
        private readonly IFlagsAttribute _pFlagsAtt;

        private readonly IOffsetAttribute _pOffsetAtt;
        private readonly IPayloadAttribute _pPayloadAtt;
        private readonly IPositionIncrementAttribute _pPosIncrAtt;
        private readonly ITermAttribute _pTermAtt;
        private readonly ITypeAttribute _pTypeAtt;
        private readonly IPayloadAttribute _payloadAtt;
        private readonly IPositionIncrementAttribute _posIncrAtt;

        private readonly Token _previousPrefixToken = new Token();
        private readonly Token _reusableToken = new Token();
        private readonly ITermAttribute _termAtt;
        private readonly ITypeAttribute _typeAtt;

        private bool _prefixExhausted;

        public PrefixAwareTokenFilter(TokenStream prefix, TokenStream suffix) : base(suffix)
        {
            Suffix = suffix;
            Prefix = prefix;
            _prefixExhausted = false;

            // ReSharper disable DoNotCallOverridableMethodsInConstructor
            _termAtt = AddAttribute<ITermAttribute>();
            _posIncrAtt = AddAttribute<IPositionIncrementAttribute>();
            _payloadAtt = AddAttribute<IPayloadAttribute>();
            _offsetAtt = AddAttribute<IOffsetAttribute>();
            _typeAtt = AddAttribute<ITypeAttribute>();
            _flagsAtt = AddAttribute<IFlagsAttribute>();
            // ReSharper restore DoNotCallOverridableMethodsInConstructor

            _pTermAtt = prefix.AddAttribute<ITermAttribute>();
            _pPosIncrAtt = prefix.AddAttribute<IPositionIncrementAttribute>();
            _pPayloadAtt = prefix.AddAttribute<IPayloadAttribute>();
            _pOffsetAtt = prefix.AddAttribute<IOffsetAttribute>();
            _pTypeAtt = prefix.AddAttribute<ITypeAttribute>();
            _pFlagsAtt = prefix.AddAttribute<IFlagsAttribute>();
        }

        public TokenStream Prefix { get; set; }

        public TokenStream Suffix { get; set; }

        public override sealed bool IncrementToken()
        {
            if (!_prefixExhausted)
            {
                Token nextToken = GetNextPrefixInputToken(_reusableToken);
                if (nextToken == null)
                {
                    _prefixExhausted = true;
                }
                else
                {
                    _previousPrefixToken.Reinit(nextToken);
                    // Make it a deep copy
                    Payload p = _previousPrefixToken.Payload;
                    if (p != null)
                    {
                        _previousPrefixToken.Payload = (Payload) p.Clone();
                    }
                    SetCurrentToken(nextToken);
                    return true;
                }
            }

            Token nextSuffixToken = GetNextSuffixInputToken(_reusableToken);
            if (nextSuffixToken == null)
            {
                return false;
            }

            nextSuffixToken = UpdateSuffixToken(nextSuffixToken, _previousPrefixToken);
            SetCurrentToken(nextSuffixToken);
            return true;
        }

        private void SetCurrentToken(Token token)
        {
            if (token == null) return;
            ClearAttributes();
            _termAtt.SetTermBuffer(token.TermBuffer(), 0, token.TermLength());
            _posIncrAtt.PositionIncrement = token.PositionIncrement;
            _flagsAtt.Flags =token.Flags;
            _offsetAtt.SetOffset(token.StartOffset, token.EndOffset);
            _typeAtt.Type = token.Type;
            _payloadAtt.Payload = token.Payload;
        }

        private Token GetNextPrefixInputToken(Token token)
        {
            if (!Prefix.IncrementToken()) return null;
            token.SetTermBuffer(_pTermAtt.TermBuffer(), 0, _pTermAtt.TermLength());
            token.PositionIncrement = _pPosIncrAtt.PositionIncrement;
            token.Flags = _pFlagsAtt.Flags;
            token.SetOffset(_pOffsetAtt.StartOffset, _pOffsetAtt.EndOffset);
            token.Type = _pTypeAtt.Type;
            token.Payload = _pPayloadAtt.Payload;
            return token;
        }

        private Token GetNextSuffixInputToken(Token token)
        {
            if (!Suffix.IncrementToken()) return null;
            token.SetTermBuffer(_termAtt.TermBuffer(), 0, _termAtt.TermLength());
            token.PositionIncrement = _posIncrAtt.PositionIncrement;
            token.Flags = _flagsAtt.Flags;
            token.SetOffset(_offsetAtt.StartOffset, _offsetAtt.EndOffset);
            token.Type = _typeAtt.Type;
            token.Payload = _payloadAtt.Payload;
            return token;
        }

        /// <summary>
        /// The default implementation adds last prefix token end offset to the suffix token start and end offsets.
        /// </summary>
        /// <param name="suffixToken">a token from the suffix stream</param>
        /// <param name="lastPrefixToken">the last token from the prefix stream</param>
        /// <returns>consumer token</returns>
        public virtual Token UpdateSuffixToken(Token suffixToken, Token lastPrefixToken)
        {
            suffixToken.StartOffset = lastPrefixToken.EndOffset + suffixToken.StartOffset;
            suffixToken.EndOffset = lastPrefixToken.EndOffset + suffixToken.EndOffset;
            return suffixToken;
        }

        protected override void Dispose(bool disposing)
        {
            Prefix.Dispose();
            Suffix.Dispose();
        }

        public override void Reset()
        {
            base.Reset();

            if (Prefix != null)
            {
                _prefixExhausted = false;
                Prefix.Reset();
            }

            if (Suffix != null)
                Suffix.Reset();
        }
    }
}