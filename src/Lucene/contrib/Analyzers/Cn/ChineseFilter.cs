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
using System.Collections.Generic;
using System.IO;
using System.Collections;
using System.Globalization;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Analysis.Cn
{
    // TODO: convert this XML code to valid .NET
    /// <summary>
    /// A {@link TokenFilter} with a stop word table.  
    /// <ul>
    /// <li>Numeric tokens are removed.</li>
    /// <li>English tokens must be larger than 1 char.</li>
    /// <li>One Chinese char as one Chinese word.</li>
    /// </ul>
    /// TO DO:
    /// <ol>
    /// <li>Add Chinese stop words, such as \ue400</li>
    /// <li>Dictionary based Chinese word extraction</li>
    /// <li>Intelligent Chinese word extraction</li>
    /// </ol>
    /// </summary>
    public sealed class ChineseFilter : TokenFilter
    {
        // Only English now, Chinese to be added later.
        public static String[] STOP_WORDS =
            {
                "and", "are", "as", "at", "be", "but", "by",
                "for", "if", "in", "into", "is", "it",
                "no", "not", "of", "on", "or", "such",
                "that", "the", "their", "then", "there", "these",
                "they", "this", "to", "was", "will", "with"
            };

        private CharArraySet stopTable;
        private ITermAttribute termAtt;

        public ChineseFilter(TokenStream _in)
            : base(_in)
        {
            stopTable = new CharArraySet((IEnumerable<string>)STOP_WORDS, false);
            termAtt = AddAttribute<ITermAttribute>();
        }

        public override bool IncrementToken()
        {
            while (input.IncrementToken())
            {
                char[] text = termAtt.TermBuffer();
                int termLength = termAtt.TermLength();

                // why not key off token type here assuming ChineseTokenizer comes first?
                if (!stopTable.Contains(text, 0, termLength))
                {
                    switch (char.GetUnicodeCategory(text[0]))
                    {
                        case UnicodeCategory.LowercaseLetter:
                        case UnicodeCategory.UppercaseLetter:
                            // English word/token should larger than 1 char.
                            if (termLength > 1)
                            {
                                return true;
                            }
                            break;
                        case UnicodeCategory.OtherLetter:
                            // One Chinese char as one Chinese word.
                            // Chinese word extraction to be added later here.
                            return true;
                    }
                }
            }
            return false;
        }
    }
}
