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
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;


namespace Lucene.Net.Analysis.Ext
{
    /// <summary>
    /// This analyzer targets short fields where *word* like searches are required.
    /// [SomeUser@GMAIL.com 1234567890] will be tokenized as
    /// [s.o.m.e.u.s.e.r..g.m.a.i.l..com..1.2.3.4.5.6.7.8.9.0] (read .'s as blank)
    /// 
    /// Usage: 
    /// QueryParser p = new QueryParser(Lucene.Net.Util.Version.LUCENE_29, "fieldName", new SingleCharTokenAnalyzer());
    /// p.SetDefaultOperator(QueryParser.Operator.AND);
    /// p.SetEnablePositionIncrements(true);
    /// 
    /// TopDocs td = src.Search(p.Parse("678"), 10);
    /// or
    /// TopDocs td = src.Search(p.Parse("\"gmail.com 1234\""), 10);
    /// </summary>
    public class SingleCharTokenAnalyzer : Analyzer
    {
        /// <summary>
        /// </summary>
        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            TokenStream t = null;
            t = new LetterOrDigitTokenizer(reader);
            t = new LowerCaseFilter(t);
            t = new ASCIIFoldingFilter(t);
            t = new SingleCharTokenizer(t);

            return t;
        }
                
        class SingleCharTokenizer : Tokenizer
        {
            TokenStream _input = null;

            ITermAttribute _termAttribute = null;
            IOffsetAttribute _offsetAttribute = null;
            IPositionIncrementAttribute _positionIncrementAttribute = null;

            char[] _buffer = null;
            int _offset = -1;
            int _length = -1;
            int _offsetInStream = -1;

            public SingleCharTokenizer(TokenStream input): base(input)
            {
                _input = input;
                _termAttribute = AddAttribute<ITermAttribute>();
                _offsetAttribute = AddAttribute<IOffsetAttribute>();
                _positionIncrementAttribute = AddAttribute<IPositionIncrementAttribute>();
            }

            public override bool IncrementToken()
            {
                int positionIncrement = 0;
                if (_buffer == null || _offset >= _length)
                {
                    if (!_input.IncrementToken()) return false;

                    _offset = 0;
                    _buffer = _termAttribute.TermBuffer();
                    _length = _termAttribute.TermLength();
                    positionIncrement++;
                    _offsetInStream++;
                }

                _offsetAttribute.SetOffset(_offsetInStream, _offsetInStream + 1);
                _offsetInStream++;

                positionIncrement++;
                _positionIncrementAttribute.PositionIncrement = positionIncrement;

                _termAttribute.SetTermLength(1);
                _termAttribute.SetTermBuffer(_buffer[_offset++].ToString());

                return true;
            }

            public override void Reset()
            {
                _buffer = null;
                _offset = -1;
                _length = -1;
                _offsetInStream = -1;

                base.Reset();
            }

            protected override void Dispose(bool disposing)
            {
                _input.Close();
                base.Dispose(disposing);
            }
        }
    }

    /// <summary>
    /// Another Analyzer. Every char which is not a letter or digit is treated as a word separator.
    /// [Name.Surname@gmail.com 123.456 ğüşıöç%ĞÜŞİÖÇ$ΑΒΓΔΕΖ#АБВГДЕ SSß] will be tokenized as
    /// [name surname gmail com 123 456 gusioc gusioc αβγδεζ абвгде ssss]
    /// 
    /// No problem with searches like someuser@gmail or 123.456 since they are
    /// converted to phrase-query as "someuser gmail" or "123 456".
    /// </summary>
    public class UnaccentedWordAnalyzer : Analyzer
    {
        /// <summary>
        /// </summary>
        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            TokenStream t = null;
            t = new LetterOrDigitTokenizer(reader);
            t = new LowerCaseFilter(t);
            t = new ASCIIFoldingFilter(t);

            return t;
        }
    }

    /// <summary>
    /// if a char is not a letter or digit, it is a word separator
    /// </summary>
    public class LetterOrDigitTokenizer : CharTokenizer
    {
        /// <summary>
        /// </summary>
        public LetterOrDigitTokenizer(TextReader reader): base(reader)
        {
        }

        /// <summary>
        /// </summary>
        protected override bool IsTokenChar(char c)
        {
            return char.IsLetterOrDigit(c);
        }
    }
}
