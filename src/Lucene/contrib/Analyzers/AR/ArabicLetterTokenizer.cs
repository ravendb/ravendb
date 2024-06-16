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
using Lucene.Net.Util;

namespace Lucene.Net.Analysis.AR
{

    /*
     * Tokenizer that breaks text into runs of letters and diacritics.
     * <p>
     * The problem with the standard Letter tokenizer is that it fails on diacritics.
     * Handling similar to this is necessary for Indic Scripts, Hebrew, Thaana, etc.
     * </p>
     *
     */
    public class ArabicLetterTokenizer : LetterTokenizer
    {

        public ArabicLetterTokenizer(TextReader @in): base(@in)
        {
            
        }

        public ArabicLetterTokenizer(AttributeSource source, TextReader @in) : base(source, @in)
        {
            
        }

        public ArabicLetterTokenizer(AttributeFactory factory, TextReader @in) : base(factory, @in)
        {
            
        }

        /* 
         * Allows for Letter category or NonspacingMark category
         * <see cref="LetterTokenizer.IsTokenChar(char)"/>
         */
        protected override bool IsTokenChar(char c)
        {
            return base.IsTokenChar(c) || char.GetUnicodeCategory(c)==System.Globalization.UnicodeCategory.NonSpacingMark ;
        }

    }
}