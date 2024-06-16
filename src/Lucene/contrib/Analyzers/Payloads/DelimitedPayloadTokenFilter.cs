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

namespace Lucene.Net.Analysis.Payloads
{
    /// <summary>
    /// Characters before the delimiter are the "token", those after are the payload.
    /// <p/>
    /// For example, if the delimiter is '|', then for the string "foo|bar", foo is the token
    /// and "bar" is a payload.
    /// <p/>
    /// Note, you can also include a {@link org.apache.lucene.analysis.payloads.PayloadEncoder} to convert the 
    /// payload in an appropriate way (from characters to bytes).
    /// <p/>
    /// Note make sure your Tokenizer doesn't split on the delimiter, or this won't work
    /// </summary>
    /// <seealso cref="PayloadEncoder"/>
    public sealed class DelimitedPayloadTokenFilter : TokenFilter
    {
        public static readonly char DEFAULT_DELIMITER = '|';
        internal char delimiter = DEFAULT_DELIMITER;
        internal ITermAttribute termAtt;
        internal IPayloadAttribute payAtt;
        internal PayloadEncoder encoder;

        /// <summary>
        /// Construct a token stream filtering the given input.
        /// </summary>
        internal DelimitedPayloadTokenFilter(TokenStream input)
            : this(input, DEFAULT_DELIMITER, new IdentityEncoder())
        {

        }


        public DelimitedPayloadTokenFilter(TokenStream input, char delimiter, PayloadEncoder encoder)
            : base(input)
        {
            termAtt = AddAttribute<ITermAttribute>();
            payAtt = AddAttribute<IPayloadAttribute>();
            this.delimiter = delimiter;
            this.encoder = encoder;
        }

        public override bool IncrementToken()
        {
            bool result = false;
            if (input.IncrementToken())
            {
                char[] buffer = termAtt.TermBuffer();
                int length = termAtt.TermLength();
                //look for the delimiter
                bool seen = false;
                for (int i = 0; i < length; i++)
                {
                    if (buffer[i] == delimiter)
                    {
                        termAtt.SetTermBuffer(buffer, 0, i);
                        payAtt.Payload = encoder.Encode(buffer, i + 1, (length - (i + 1)));
                        seen = true;
                        break;//at this point, we know the whole piece, so we can exit.  If we don't see the delimiter, then the termAtt is the same
                    }
                }
                if (seen == false)
                {
                    //no delimiter
                    payAtt.Payload = null;
                }
                result = true;
            }
            return result;
        }
    }
}
