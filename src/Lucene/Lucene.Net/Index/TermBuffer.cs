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
using Lucene.Net.Store;
using Lucene.Net.Support;
using IndexInput = Lucene.Net.Store.IndexInput;
using UnicodeUtil = Lucene.Net.Util.UnicodeUtil;

namespace Lucene.Net.Index
{
	
	public sealed class TermBuffer : System.ICloneable
	{
		
		private System.String field;
		private Term term; // cached
		private bool preUTF8Strings; // true if strings are stored in modified UTF8 encoding (LUCENE-510)
		private bool dirty; // true if text was set externally (ie not read via UTF8 bytes)
		
		private UnicodeUtil.UTF16Result text = new UnicodeUtil.UTF16Result();
		private UnicodeUtil.UTF8Result bytes = new UnicodeUtil.UTF8Result();

        public Span<char> TextAsSpan => new Span<char>(text.result, 0, text.length);
        public string Field => field;

		public int CompareTo(TermBuffer other)
		{
			if ((System.Object) field == (System.Object) other.field)
			// fields are interned
				return CompareChars(text.result, text.length, other.text.result, other.text.length);
			else
				return String.CompareOrdinal(field, other.field);
		}
		
		private static int CompareChars(char[] chars1, int len1, char[] chars2, int len2)
		{
			int end = len1 < len2?len1:len2;
			for (int k = 0; k < end; k++)
			{
				char c1 = chars1[k];
				char c2 = chars2[k];
				if (c1 != c2)
				{
					return c1 - c2;
				}
			}
			return len1 - len2;
		}
		
		/// <summary>Call this if the IndexInput passed to <see cref="Read" />
		/// stores terms in the "modified UTF8" (pre LUCENE-510)
		/// format. 
		/// </summary>
		internal void  SetPreUTF8Strings()
		{
			preUTF8Strings = true;
		}
		
		public void  Read(IndexInput input, FieldInfos fieldInfos, IState state)
		{
            this.term = null; // invalidate cache
			int start = input.ReadVInt(state);
			int length = input.ReadVInt(state);
			int totalLength = start + length;
			if (preUTF8Strings)
			{
				text.SetLength(totalLength);
				input.ReadChars(text.result, start, length, state);
			}
			else
			{
				
				if (dirty)
				{
					// Fully convert all bytes since bytes is dirty
					UnicodeUtil.UTF16toUTF8(text.result, 0, text.length, bytes);
					bytes.SetLength(totalLength);
					input.ReadBytes(bytes.result, start, length, state);
					UnicodeUtil.UTF8toUTF16(bytes.result, 0, totalLength, text);
					dirty = false;
				}
				else
				{
					// Incrementally convert only the UTF8 bytes that are new:
					bytes.SetLength(totalLength);
					input.ReadBytes(bytes.result, start, length, state);
					UnicodeUtil.UTF8toUTF16(bytes.result, start, length, text);
				}
			}
			this.field = fieldInfos.FieldName(input.ReadVInt(state));
		}
		
		public void  Set(Term term)
		{
			if (term == null)
			{
				Reset();
				return ;
			}
			System.String termText = term.Text;
			int termLen = termText.Length;
			text.SetLength(termLen);
			TextSupport.GetCharsFromString(termText, 0, termLen, text.result, 0);
			dirty = true;
			field = term.Field;
			this.term = term;
		}
		
		public void  Set(TermBuffer other)
		{
			text.CopyText(other.text);
			dirty = true;
			field = other.field;
			term = other.term;
		}
		
		public void  Reset()
		{
			field = null;
			text.SetLength(0);
            term = null;
			dirty = true;
		}
		
		public Term ToTerm()
		{
			if (field == null)
			// unset
                return null;
			
			if (term == null)
				term = new Term(field, new System.String(text.result, 0, text.length), false);
			
			return term;
		}
		
		public System.Object Clone()
		{
			TermBuffer clone = null;
			try
			{
				clone = (TermBuffer) base.MemberwiseClone();
			}
			catch (System.Exception)
			{
			}
			
			clone.dirty = true;
			clone.bytes = new UnicodeUtil.UTF8Result();
			clone.text = new UnicodeUtil.UTF16Result();
			clone.text.CopyText(text);
			return clone;
		}
	}
}