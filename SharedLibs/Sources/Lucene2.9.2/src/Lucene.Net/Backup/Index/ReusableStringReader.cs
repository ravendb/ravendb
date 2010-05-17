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

namespace Lucene.Net.Index
{
	
	/// <summary>Used by DocumentsWriter to implemented a StringReader
	/// that can be reset to a new string; we use this when
	/// tokenizing the string value from a Field. 
	/// </summary>
    sealed class ReusableStringReader : System.IO.TextReader
    {
        internal int upto;
        internal int left;
        internal System.String s;
        internal void Init(System.String s)
        {
            this.s = s;
            left = s.Length;
            this.upto = 0;
        }
        public int Read(char[] c)
        {
            return Read(c, 0, c.Length);
        }
        public override int Read(System.Char[] c, int off, int len)
        {
            if (left > len)
            {
                SupportClass.TextSupport.GetCharsFromString(s, upto, upto + len, c, off);
                upto += len;
                left -= len;
                return len;
            }
            else if (0 == left)
            {
                return 0;
            }
            else
            {
                SupportClass.TextSupport.GetCharsFromString(s, upto, upto + left, c, off);
                int r = left;
                left = 0;
                upto = s.Length;
                return r;
            }
        }
        public override void Close()
        {
        }


        public override int Read()
        {
            throw new NotImplementedException("This method is not implemented");
        }

        public override int ReadBlock(char[] buffer, int index, int count)
        {
            throw new NotImplementedException("This method is not implemented");
        }

        public override string ReadLine()
        {
            throw new NotImplementedException("This method is not implemented");
        }

        public override int Peek()
        {
            throw new NotImplementedException("This method is not implemented");
        }

        public override string ReadToEnd()
        {
            left = 0;
            return s;
        }
    }
}