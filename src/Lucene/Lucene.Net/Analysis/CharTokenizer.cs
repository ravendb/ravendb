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

using Lucene.Net.Analysis.Tokenattributes;
using AttributeSource = Lucene.Net.Util.AttributeSource;

namespace Lucene.Net.Analysis
{
	
	/// <summary>An abstract base class for simple, character-oriented tokenizers.</summary>
	public abstract class CharTokenizer:Tokenizer
	{
	    protected CharTokenizer(System.IO.TextReader input):base(input)
		{
			offsetAtt = AddAttribute<IOffsetAttribute>();
            termAtt = AddAttribute<ITermAttribute>();
		}

	    protected CharTokenizer(AttributeSource source, System.IO.TextReader input):base(source, input)
		{
            offsetAtt = AddAttribute<IOffsetAttribute>();
            termAtt = AddAttribute<ITermAttribute>();
		}

	    protected CharTokenizer(AttributeFactory factory, System.IO.TextReader input):base(factory, input)
		{
            offsetAtt = AddAttribute<IOffsetAttribute>();
            termAtt = AddAttribute<ITermAttribute>();
		}
		
		private int offset = 0, bufferIndex = 0, dataLen = 0;
		private const int MAX_WORD_LEN = 255;
		private const int IO_BUFFER_SIZE = 4096;
		private readonly char[] ioBuffer = new char[IO_BUFFER_SIZE];
		
		private readonly ITermAttribute termAtt;
		private readonly IOffsetAttribute offsetAtt;
		
		/// <summary>Returns true iff a character should be included in a token.  This
		/// tokenizer generates as tokens adjacent sequences of characters which
		/// satisfy this predicate.  Characters for which this is false are used to
		/// define token boundaries and are not included in tokens. 
		/// </summary>
		protected internal abstract bool IsTokenChar(char c);
		
		/// <summary>Called on each token character to normalize it before it is added to the
		/// token.  The default implementation does nothing.  Subclasses may use this
		/// to, e.g., lowercase tokens. 
		/// </summary>
		protected internal virtual char Normalize(char c)
		{
			return c;
		}
		
		public override bool IncrementToken()
		{
			ClearAttributes();
			int length = 0;
			int start = bufferIndex;
			char[] buffer = termAtt.TermBuffer();
			while (true)
			{
				
				if (bufferIndex >= dataLen)
				{
					offset += dataLen;
					dataLen = input.Read(ioBuffer, 0, ioBuffer.Length);
					if (dataLen <= 0)
					{
						dataLen = 0; // so next offset += dataLen won't decrement offset
						if (length > 0)
							break;
						return false;
					}
					bufferIndex = 0;
				}
				
				char c = ioBuffer[bufferIndex++];
				
				if (IsTokenChar(c))
				{
					// if it's a token char
					
					if (length == 0)
					// start of token
						start = offset + bufferIndex - 1;
					else if (length == buffer.Length)
						buffer = termAtt.ResizeTermBuffer(1 + length);
					
					buffer[length++] = Normalize(c); // buffer it, normalized
					
					if (length == MAX_WORD_LEN)
					// buffer overflow!
						break;
				}
				else if (length > 0)
				// at non-Letter w/ chars
					break; // return 'em
			}
			
			termAtt.SetTermLength(length);
			offsetAtt.SetOffset(CorrectOffset(start), CorrectOffset(start + length));
			return true;
		}
		
		public override void  End()
		{
			// set final offset
			int finalOffset = CorrectOffset(offset);
			offsetAtt.SetOffset(finalOffset, finalOffset);
		}
		
		public override void  Reset(System.IO.TextReader input)
		{
			base.Reset(input);
			bufferIndex = 0;
			offset = 0;
			dataLen = 0;
		}
	}
}