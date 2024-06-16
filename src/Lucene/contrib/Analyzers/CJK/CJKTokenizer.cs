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
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;

namespace Lucene.Net.Analysis.CJK
{
    /// <summary>
    /// <p>
    /// CJKTokenizer was modified from StopTokenizer which does a decent job for
    /// most European languages. and it perferm other token method for double-byte
    /// chars: the token will return at each two charactors with overlap match.<br/>
    /// Example: "java C1C2C3C4" will be segment to: "java" "C1C2" "C2C3" "C3C4" it
    /// also need filter filter zero length token ""<br/>
    /// for Digit: digit, '+', '#' will token as letter<br/>
    /// for more info on Asia language(Chinese Japanese Korean) text segmentation:
    /// please search  <a
    /// href="http://www.google.com/search?q=word+chinese+segment">google</a>
    /// </p>
    /// 
    /// @author Che, Dong
    /// @version $Id: CJKTokenizer.java,v 1.3 2003/01/22 20:54:47 otis Exp $
    /// </summary>
    public sealed class CJKTokenizer : Tokenizer
    {
        //~ Static fields/initializers ---------------------------------------------
        /// <summary>
        /// Word token type
        /// </summary>
        internal static readonly int WORD_TYPE = 0;

        /// <summary>
        /// Single byte token type
        /// </summary>
        internal static readonly int SINGLE_TOKEN_TYPE = 1;

        /// <summary>
        /// Double byte token type
        /// </summary>
        internal static readonly int DOUBLE_TOKEN_TYPE = 2;

        /// <summary>
        /// Names for token types
        /// </summary>
        internal static readonly String[] TOKEN_TYPE_NAMES = { "word", "single", "double" };

        /// <summary>
        /// Max word length
        /// </summary>
        internal static readonly int MAX_WORD_LEN = 255;

        /// <summary>
        /// buffer size
        /// </summary>
        internal static readonly int IO_BUFFER_SIZE = 256;

        //~ Instance fields --------------------------------------------------------

        /// <summary>
        /// word offset, used to imply which character(in ) is parsed
        /// </summary>
        private int offset = 0;

        /// <summary>
        /// the index used only for ioBuffer
        /// </summary>
        private int bufferIndex = 0;

        /// <summary>
        /// data length
        /// </summary>
        private int dataLen = 0;

        /// <summary>
        /// character buffer, store the characters which are used to compose <br/>
        /// the returned Token
        /// </summary>
        private char[] buffer = new char[MAX_WORD_LEN];

        /// <summary>
        /// I/O buffer, used to store the content of the input(one of the <br/>
        /// members of Tokenizer)
        /// </summary>
        private char[] ioBuffer = new char[IO_BUFFER_SIZE];

        /// <summary>
        /// word type: single=>ASCII  double=>non-ASCII word=>default
        /// </summary>
        private int tokenType = WORD_TYPE;

        /// <summary>
        /// tag: previous character is a cached double-byte character  "C1C2C3C4"
        /// ----(set the C1 isTokened) C1C2 "C2C3C4" ----(set the C2 isTokened)
        /// C1C2 C2C3 "C3C4" ----(set the C3 isTokened) "C1C2 C2C3 C3C4"
        /// </summary>
        private bool preIsTokened = false;

        private ITermAttribute termAtt;
        private IOffsetAttribute offsetAtt;
        private ITypeAttribute typeAtt;

        //~ Constructors -----------------------------------------------------------

        /// <summary>
        /// Construct a token stream processing the given input.
        /// </summary>
        /// <param name="_in">I/O reader</param>
        public CJKTokenizer(TextReader _in)
            : base(_in)
        {
            Init();
        }

        public CJKTokenizer(AttributeSource source, TextReader _in)
            : base(source, _in)
        {
            Init();
        }

        public CJKTokenizer(AttributeFactory factory, TextReader _in)
            : base(factory, _in)
        {
            Init();
        }

        private void Init()
        {
            termAtt = AddAttribute<ITermAttribute>();
            offsetAtt = AddAttribute<IOffsetAttribute>();
            typeAtt = AddAttribute<ITypeAttribute>();
        }

        //~ Methods ----------------------------------------------------------------

        /*
         * Returns true for the next token in the stream, or false at EOS.
         * See http://java.sun.com/j2se/1.3/docs/api/java/lang/char.UnicodeBlock.html
         * for detail.
         *
         * @return false for end of stream, true otherwise
         *
         * @throws java.io.IOException - throw IOException when read error <br>
         *         happened in the InputStream
         *
         */

        Regex isBasicLatin = new Regex(@"\p{IsBasicLatin}", RegexOptions.Compiled);
        Regex isHalfWidthAndFullWidthForms = new Regex(@"\p{IsHalfwidthandFullwidthForms}", RegexOptions.Compiled);

        public override bool IncrementToken()
        {
            ClearAttributes();
            /* how many character(s) has been stored in buffer */

            while (true)
            {
                // loop until we find a non-empty token

                int length = 0;

                /* the position used to create Token */
                int start = offset;

                while (true)
                {
                    // loop until we've found a full token
                    /* current character */
                    char c;

                    offset++;

                    if (bufferIndex >= dataLen)
                    {
                        dataLen = input.Read(ioBuffer, 0, ioBuffer.Length);
                        bufferIndex = 0;
                    }

                    if (dataLen == 0) // input.Read returns 0 when its empty, not -1, as in java
                    {
                        if (length > 0)
                        {
                            if (preIsTokened == true)
                            {
                                length = 0;
                                preIsTokened = false;
                            }
                            else
                            {
                                offset--;
                            }

                            break;
                        }
                        else
                        {
                            offset--;
                            return false;
                        }
                    }
                    else
                    {
                        //get current character
                        c = ioBuffer[bufferIndex++];
                    }

                    //TODO: Using a Regex to determine the UnicodeCategory is probably slower than
                    //      If we just created a small class that would look it up for us, which 
                    //      would likely be trivial, however time-consuming.  I can't imagine a Regex
                    //      being fast for this, considering we have to pull a char from the buffer,
                    //      and convert it to a string before we run a regex on it. - cc
                    bool isHalfFullForm = isHalfWidthAndFullWidthForms.Match(c.ToString()).Success;
                    //if the current character is ASCII or Extend ASCII
                    if ((isBasicLatin.Match(c.ToString()).Success) || (isHalfFullForm))
                    {
                        if (isHalfFullForm)
                        {
                            int i = (int) c;
                            if (i >= 65281 && i <= 65374)
                            {
                                // convert certain HALFWIDTH_AND_FULLWIDTH_FORMS to BASIC_LATIN
                                i = i - 65248;
                                c = (char) i;
                            }
                        }

                        // if the current character is a letter or "_" "+" "#"
                        if (char.IsLetterOrDigit(c)
                            || ((c == '_') || (c == '+') || (c == '#'))
                            )
                        {
                            if (length == 0)
                            {
                                // "javaC1C2C3C4linux" <br>
                                //      ^--: the current character begin to token the ASCII
                                // letter
                                start = offset - 1;
                            }
                            else if (tokenType == DOUBLE_TOKEN_TYPE)
                            {
                                // "javaC1C2C3C4linux" <br>
                                //              ^--: the previous non-ASCII
                                // : the current character
                                offset--;
                                bufferIndex--;

                                if (preIsTokened == true)
                                {
                                    // there is only one non-ASCII has been stored
                                    length = 0;
                                    preIsTokened = false;
                                    break;
                                }
                                else
                                {
                                    break;
                                }
                            }

                            // store the LowerCase(c) in the buffer
                            buffer[length++] = char.ToLower(c); // TODO: is java invariant?  If so, this should be ToLowerInvariant()
                            tokenType = SINGLE_TOKEN_TYPE;

                            // break the procedure if buffer overflowed!
                            if (length == MAX_WORD_LEN)
                            {
                                break;
                            }
                        }
                        else if (length > 0)
                        {
                            if (preIsTokened)
                            {
                                length = 0;
                                preIsTokened = false;
                            }
                            else
                            {
                                break;
                            }
                        }
                    }
                    else
                    {
                        // non-ASCII letter, e.g."C1C2C3C4"
                        if (char.IsLetter(c))
                        {
                            if (length == 0)
                            {
                                start = offset - 1;
                                buffer[length++] = c;
                                tokenType = DOUBLE_TOKEN_TYPE;
                            }
                            else
                            {
                                if (tokenType == SINGLE_TOKEN_TYPE)
                                {
                                    offset--;
                                    bufferIndex--;

                                    //return the previous ASCII characters
                                    break;
                                }
                                else
                                {
                                    buffer[length++] = c;
                                    tokenType = DOUBLE_TOKEN_TYPE;

                                    if (length == 2)
                                    {
                                        offset--;
                                        bufferIndex--;
                                        preIsTokened = true;

                                        break;
                                    }
                                }
                            }
                        }
                        else if (length > 0)
                        {
                            if (preIsTokened == true)
                            {
                                // empty the buffer
                                length = 0;
                                preIsTokened = false;
                            }
                            else
                            {
                                break;
                            }
                        }
                    }
                }

                if (length > 0)
                {
                    termAtt.SetTermBuffer(buffer, 0, length);
                    offsetAtt.SetOffset(CorrectOffset(start), CorrectOffset(start + length));
                    typeAtt.Type = TOKEN_TYPE_NAMES[tokenType];
                    return true;
                }
                else if (dataLen == 0)
                {
                    offset--;
                    return false;
                }

                // Cycle back and try for the next token (don't
                // return an empty string)
            }
        }

        public override void End()
        {
            // set final offset
            int finalOffset = CorrectOffset(offset);
            this.offsetAtt.SetOffset(finalOffset, finalOffset);
        }

        public override void Reset()
        {
            base.Reset();
            offset = bufferIndex = dataLen = 0;
            preIsTokened = false;
            tokenType = WORD_TYPE;
        }

        public override void Reset(TextReader reader)
        {
            base.Reset(reader);
            Reset();
        }
    }
}
