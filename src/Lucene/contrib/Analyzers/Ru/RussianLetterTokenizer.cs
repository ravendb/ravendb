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
using System.IO;
using Lucene.Net.Analysis;
using Lucene.Net.Util;

namespace Lucene.Net.Analysis.Ru
{
    ///<summary>
    /// A RussianLetterTokenizer is a {@link Tokenizer} that extends {@link LetterTokenizer}
    /// by also allowing the basic latin digits 0-9. 
    ///</summary>
    public class RussianLetterTokenizer : CharTokenizer
    {
        public RussianLetterTokenizer(TextReader _in)
            : base(_in)
        {
        }

        public RussianLetterTokenizer(AttributeSource source, TextReader _in)
            : base(source, _in)
        {
        }

        public RussianLetterTokenizer(AttributeSource.AttributeFactory factory, TextReader __in)
            : base(factory, __in)
        {
        }

        /*
         * Collects only characters which satisfy
         * {@link Character#isLetter(char)}.
         */
        protected override bool IsTokenChar(char c)
        {
            if (char.IsLetter(c) || (c >= '0' && c <= '9'))
                return true;
            else
                return false;
        }
    }
}