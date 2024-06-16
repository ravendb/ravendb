/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

using System;
using System.Text;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;

namespace Lucene.Net.Analysis.Payloads
{
    /// <summary>
    /// Makes the Token.Type() a payload.
    /// Encodes the type using <see cref="System.Text.Encoding.UTF8"/> as the encoding
    /// </summary>
    public class TypeAsPayloadTokenFilter : TokenFilter
    {
        private IPayloadAttribute payloadAtt;
        private ITypeAttribute typeAtt;

        public TypeAsPayloadTokenFilter(TokenStream input)
            : base(input)
        {
            payloadAtt = AddAttribute<IPayloadAttribute>();
            typeAtt = AddAttribute<ITypeAttribute>();
        }

        public sealed override bool IncrementToken()
        {
            if (input.IncrementToken())
            {
                String type = typeAtt.Type;
                if (type != null && type.Equals("") == false)
                {
                    payloadAtt.Payload = new Payload(Encoding.UTF8.GetBytes(type));
                }
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
