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

using System.Diagnostics;
using Lucene.Net.Analysis.Tokenattributes;
using Attribute = Lucene.Net.Util.Attribute;

namespace Lucene.Net.Analysis.Miscellaneous
{
    /// <summary>
    /// A TokenStream containing a single token.
    /// </summary>
    public sealed class SingleTokenTokenStream : TokenStream
    {
        private bool _exhausted;

        // The token needs to be immutable, so work with clones!
        private Token _singleToken;
        private readonly Attribute _tokenAtt;

        public SingleTokenTokenStream(Token token)
            : base(Token.TOKEN_ATTRIBUTE_FACTORY)
        {
            Debug.Assert(token != null, "Token was null!");
            _singleToken = (Token) token.Clone();

            _tokenAtt = (Attribute)AddAttribute<ITermAttribute>();

            Debug.Assert(_tokenAtt is Token);
        }

        public override sealed bool IncrementToken()
        {
            if (_exhausted)
                return false;

            ClearAttributes();
            _singleToken.CopyTo(_tokenAtt);
            _exhausted = true;

            return true;
        }

        public override void Reset()
        {
            _exhausted = false;
        }

        protected override void Dispose(bool disposing)
        {
            // Do nothing
        }

        public Token GetToken()
        {
            return (Token) _singleToken.Clone();
        }

        public void SetToken(Token token)
        {
            _singleToken = (Token) token.Clone();
        }
    }
}