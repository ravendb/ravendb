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
using System.Runtime.CompilerServices;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Support;
using Lucene.Net.Util;
using Payload = Lucene.Net.Index.Payload;
using ArrayUtil = Lucene.Net.Util.ArrayUtil;
using Attribute = Lucene.Net.Util.Attribute;

namespace Lucene.Net.Analysis
{

    /// <summary>A Token is an occurrence of a term from the text of a field.  It consists of
    /// a term's text, the start and end offset of the term in the text of the field,
    /// and a type string.
    /// <p/>
    /// The start and end offsets permit applications to re-associate a token with
    /// its source text, e.g., to display highlighted query terms in a document
    /// browser, or to show matching text fragments in a <abbr
    /// title="KeyWord In Context">KWIC</abbr> display, etc.
    /// <p/>
    /// The type is a string, assigned by a lexical analyzer
    /// (a.k.a. tokenizer), naming the lexical or syntactic class that the token
    /// belongs to.  For example an end of sentence marker token might be implemented
    /// with type "eos".  The default token type is "word".  
    /// <p/>
    /// A Token can optionally have metadata (a.k.a. Payload) in the form of a variable
    /// length byte array. Use <see cref="TermPositions.PayloadLength" /> and 
    /// <see cref="TermPositions.GetPayload(byte[], int)" /> to retrieve the payloads from the index.
    /// </summary>
    /// <summary><br/><br/>
    /// </summary>
    /// <summary><p/><b>NOTE:</b> As of 2.9, Token implements all <see cref="IAttribute" /> interfaces
    /// that are part of core Lucene and can be found in the <see cref="Lucene.Net.Analysis.Tokenattributes"/> namespace.
    /// Even though it is not necessary to use Token anymore, with the new TokenStream API it can
    /// be used as convenience class that implements all <see cref="IAttribute" />s, which is especially useful
    /// to easily switch from the old to the new TokenStream API.
    /// <br/><br/>
    /// <p/>Tokenizers and TokenFilters should try to re-use a Token instance when
    /// possible for best performance, by implementing the
    /// <see cref="TokenStream.IncrementToken()" /> API.
    /// Failing that, to create a new Token you should first use
    /// one of the constructors that starts with null text.  To load
    /// the token from a char[] use <see cref="SetTermBuffer(char[], int, int)" />.
    /// To load from a String use <see cref="SetTermBuffer(String)" /> or <see cref="SetTermBuffer(String, int, int)" />.
    /// Alternatively you can get the Token's termBuffer by calling either <see cref="TermBuffer()" />,
    /// if you know that your text is shorter than the capacity of the termBuffer
    /// or <see cref="ResizeTermBuffer(int)" />, if there is any possibility
    /// that you may need to grow the buffer. Fill in the characters of your term into this
    /// buffer, with <see cref="string.ToCharArray(int, int)" /> if loading from a string,
    /// or with <see cref="Array.Copy(Array, long, Array, long, long)" />, and finally call <see cref="SetTermLength(int)" /> to
    /// set the length of the term text.  See <a target="_top"
    /// href="https://issues.apache.org/jira/browse/LUCENE-969">LUCENE-969</a>
    /// for details.<p/>
    /// <p/>Typical Token reuse patterns:
    /// <list type="bullet">
    /// <item> Copying text from a string (type is reset to <see cref="DEFAULT_TYPE" /> if not
    /// specified):<br/>
    /// <code>
    /// return reusableToken.reinit(string, startOffset, endOffset[, type]);
    /// </code>
    /// </item>
    /// <item> Copying some text from a string (type is reset to <see cref="DEFAULT_TYPE" />
    /// if not specified):<br/>
    /// <code>
    /// return reusableToken.reinit(string, 0, string.length(), startOffset, endOffset[, type]);
    /// </code>
    /// </item>
    /// <item> Copying text from char[] buffer (type is reset to <see cref="DEFAULT_TYPE" />
    /// if not specified):<br/>
    /// <code>
    /// return reusableToken.reinit(buffer, 0, buffer.length, startOffset, endOffset[, type]);
    /// </code>
    /// </item>
    /// <item> Copying some text from a char[] buffer (type is reset to
    /// <see cref="DEFAULT_TYPE" /> if not specified):<br/>
    /// <code>
    /// return reusableToken.reinit(buffer, start, end - start, startOffset, endOffset[, type]);
    /// </code>
    /// </item>
    /// <item> Copying from one one Token to another (type is reset to
    /// <see cref="DEFAULT_TYPE" /> if not specified):<br/>
    /// <code>
    /// return reusableToken.reinit(source.termBuffer(), 0, source.termLength(), source.startOffset(), source.endOffset()[, source.type()]);
    /// </code>
    /// </item>
    /// </list>
    /// A few things to note:
    /// <list type="bullet">
    /// <item>clear() initializes all of the fields to default values. This was changed in contrast to Lucene 2.4, but should affect no one.</item>
    /// <item>Because <c>TokenStreams</c> can be chained, one cannot assume that the <c>Token's</c> current type is correct.</item>
    /// <item>The startOffset and endOffset represent the start and offset in the
    /// source text, so be careful in adjusting them.</item>
    /// <item>When caching a reusable token, clone it. When injecting a cached token into a stream that can be reset, clone it again.</item>
    /// </list>
    /// <p/>
    /// </summary>
    /// <seealso cref="Lucene.Net.Index.Payload">
    /// </seealso>
    [Serializable]
	public class Token : Attribute, ITermAttribute, ITypeAttribute, IPositionIncrementAttribute, IFlagsAttribute, IOffsetAttribute, IPayloadAttribute
	{
		public const String DEFAULT_TYPE = "word";

		private const int MIN_BUFFER_SIZE = 10;

		private char[] termBuffer;
		private int termLength;
		private int startOffset, endOffset;
		private string type = DEFAULT_TYPE;
		private int flags;
		private Payload payload;
		private int positionIncrement = 1;
		
		/// <summary>Constructs a Token will null text. </summary>
		public Token()
		{
		}
		
		/// <summary>Constructs a Token with null text and start &amp; end
		/// offsets.
		/// </summary>
		/// <param name="start">start offset in the source text</param>
		/// <param name="end">end offset in the source text</param>
		public Token(int start, int end)
		{
			startOffset = start;
			endOffset = end;
		}
		
		/// <summary>Constructs a Token with null text and start &amp; end
		/// offsets plus the Token type.
		/// </summary>
		/// <param name="start">start offset in the source text</param>
		/// <param name="end">end offset in the source text</param>
		/// <param name="typ">the lexical type of this Token</param>
		public Token(int start, int end, String typ)
		{
			startOffset = start;
			endOffset = end;
			type = typ;
		}
		
		/// <summary> Constructs a Token with null text and start &amp; end
		/// offsets plus flags. NOTE: flags is EXPERIMENTAL.
		/// </summary>
		/// <param name="start">start offset in the source text</param>
		/// <param name="end">end offset in the source text</param>
		/// <param name="flags">The bits to set for this token</param>
		public Token(int start, int end, int flags)
		{
			startOffset = start;
			endOffset = end;
			this.flags = flags;
		}
		
		/// <summary>Constructs a Token with the given term text, and start
		/// &amp; end offsets.  The type defaults to "word."
		/// <b>NOTE:</b> for better indexing speed you should
		/// instead use the char[] termBuffer methods to set the
		/// term text.
		/// </summary>
		/// <param name="text">term text</param>
		/// <param name="start">start offset</param>
		/// <param name="end">end offset</param>
		public Token(String text, int start, int end)
		{
		    SetTermBuffer(text);
			startOffset = start;
			endOffset = end;
		}
		
		/// <summary>Constructs a Token with the given text, start and end
		/// offsets, &amp; type.  <b>NOTE:</b> for better indexing
		/// speed you should instead use the char[] termBuffer
		/// methods to set the term text.
		/// </summary>
		/// <param name="text">term text</param>
		/// <param name="start">start offset</param>
		/// <param name="end">end offset</param>
		/// <param name="typ">token type</param>
		public Token(System.String text, int start, int end, System.String typ)
		{
		    SetTermBuffer(text);
			startOffset = start;
			endOffset = end;
			type = typ;
		}
		
		/// <summary>  Constructs a Token with the given text, start and end
		/// offsets, &amp; type.  <b>NOTE:</b> for better indexing
		/// speed you should instead use the char[] termBuffer
		/// methods to set the term text.
		/// </summary>
		/// <param name="text"></param>
		/// <param name="start"></param>
		/// <param name="end"></param>
		/// <param name="flags">token type bits</param>
		public Token(System.String text, int start, int end, int flags)
		{
		    SetTermBuffer(text);
			startOffset = start;
			endOffset = end;
			this.flags = flags;
		}
		
		/// <summary>  Constructs a Token with the given term buffer (offset
		/// &amp; length), start and end
		/// offsets
		/// </summary>
		/// <param name="startTermBuffer"></param>
		/// <param name="termBufferOffset"></param>
		/// <param name="termBufferLength"></param>
		/// <param name="start"></param>
		/// <param name="end"></param>
		public Token(char[] startTermBuffer, int termBufferOffset, int termBufferLength, int start, int end)
		{
			SetTermBuffer(startTermBuffer, termBufferOffset, termBufferLength);
			startOffset = start;
			endOffset = end;
		}

	    /// <summary>Set the position increment.  This determines the position of this token
	    /// relative to the previous Token in a <see cref="TokenStream" />, used in phrase
	    /// searching.
	    /// 
	    /// <p/>The default value is one.
	    /// 
	    /// <p/>Some common uses for this are:<list>
	    /// 
	    /// <item>Set it to zero to put multiple terms in the same position.  This is
	    /// useful if, e.g., a word has multiple stems.  Searches for phrases
	    /// including either stem will match.  In this case, all but the first stem's
	    /// increment should be set to zero: the increment of the first instance
	    /// should be one.  Repeating a token with an increment of zero can also be
	    /// used to boost the scores of matches on that token.</item>
	    /// 
	    /// <item>Set it to values greater than one to inhibit exact phrase matches.
	    /// If, for example, one does not want phrases to match across removed stop
	    /// words, then one could build a stop word filter that removes stop words and
	    /// also sets the increment to the number of stop words removed before each
	    /// non-stop word.  Then exact phrase queries will only match when the terms
	    /// occur with no intervening stop words.</item>
	    /// 
	    /// </list>
	    /// </summary>
	    /// <value> the distance from the prior term </value>
	    /// <seealso cref="Lucene.Net.Index.TermPositions">
	    /// </seealso>
	    public virtual int PositionIncrement
	    {
	        set
	        {
	            if (value < 0)
	                throw new System.ArgumentException("Increment must be zero or greater: " + value);
	            this.positionIncrement = value;
	        }
	        get { return positionIncrement; }
	    }

	    /// <summary>Returns the Token's term text.
	    /// 
	    /// This method has a performance penalty
	    /// because the text is stored internally in a char[].  If
	    /// possible, use <see cref="TermBuffer()" /> and <see cref="TermLength()"/>
	    /// directly instead.  If you really need a
	    /// String, use this method, which is nothing more than
	    /// a convenience call to <b>new String(token.termBuffer(), 0, token.termLength())</b>
	    /// </summary>
	    public string Term
	    {
	        get
	        {
	            InitTermBuffer();
	            return new System.String(termBuffer, 0, termLength);
	        }
	    }

	    /// <summary>Copies the contents of buffer, starting at offset for
		/// length characters, into the termBuffer array.
		/// </summary>
		/// <param name="buffer">the buffer to copy</param>
		/// <param name="offset">the index in the buffer of the first character to copy</param>
		/// <param name="length">the number of characters to copy</param>
		public void  SetTermBuffer(char[] buffer, int offset, int length)
		{
			GrowTermBuffer(length);
			Array.Copy(buffer, offset, termBuffer, 0, length);
			termLength = length;
		}
		
		/// <summary>Copies the contents of buffer into the termBuffer array.</summary>
		/// <param name="buffer">the buffer to copy
		/// </param>
		public void  SetTermBuffer(System.String buffer)
		{
			int length = buffer.Length;
			GrowTermBuffer(length);
			TextSupport.GetCharsFromString(buffer, 0, length, termBuffer, 0);
			termLength = length;
		}
		
		/// <summary>Copies the contents of buffer, starting at offset and continuing
		/// for length characters, into the termBuffer array.
		/// </summary>
		/// <param name="buffer">the buffer to copy
		/// </param>
		/// <param name="offset">the index in the buffer of the first character to copy
		/// </param>
		/// <param name="length">the number of characters to copy
		/// </param>
		public void  SetTermBuffer(System.String buffer, int offset, int length)
		{
			System.Diagnostics.Debug.Assert(offset <= buffer.Length);
			System.Diagnostics.Debug.Assert(offset + length <= buffer.Length);
			GrowTermBuffer(length);
			TextSupport.GetCharsFromString(buffer, offset, offset + length, termBuffer, 0);
			termLength = length;
		}
		
		/// <summary>Returns the internal termBuffer character array which
		/// you can then directly alter.  If the array is too
		/// small for your token, use <see cref="ResizeTermBuffer(int)" />
		/// to increase it.  After
		/// altering the buffer be sure to call <see cref="SetTermLength" />
		/// to record the number of valid
		/// characters that were placed into the termBuffer. 
		/// </summary>
		public char[] TermBuffer()
		{
			InitTermBuffer();
			return termBuffer;
		}
		
		/// <summary>Grows the termBuffer to at least size newSize, preserving the
		/// existing content. Note: If the next operation is to change
		/// the contents of the term buffer use
		/// <see cref="SetTermBuffer(char[], int, int)" />,
		/// <see cref="SetTermBuffer(String)" />, or
		/// <see cref="SetTermBuffer(String, int, int)" />
		/// to optimally combine the resize with the setting of the termBuffer.
		/// </summary>
		/// <param name="newSize">minimum size of the new termBuffer
		/// </param>
		/// <returns> newly created termBuffer with length >= newSize
		/// </returns>
		public virtual char[] ResizeTermBuffer(int newSize)
		{
			if (termBuffer == null)
			{
                termBuffer = new char[ArrayUtil.GetNextSize(newSize < MIN_BUFFER_SIZE ? MIN_BUFFER_SIZE : newSize)];
			}
			else
			{
				if (termBuffer.Length < newSize)
				{
					// Not big enough; create a new array with slight
					// over allocation and preserve content
					var newCharBuffer = new char[ArrayUtil.GetNextSize(newSize)];
					Array.Copy(termBuffer, 0, newCharBuffer, 0, termBuffer.Length);
					termBuffer = newCharBuffer;
				}
			}
			return termBuffer;
		}

        /// <summary>Allocates a buffer char[] of at least newSize, without preserving the existing content.
        /// its always used in places that set the content 
        /// </summary>
        /// <param name="newSize">minimum size of the buffer
        /// </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void GrowTermBuffer(int newSize)
		{
			if (termBuffer == null)
			{
				// The buffer is always at least MIN_BUFFER_SIZE    
				termBuffer = new char[ArrayUtil.GetNextSize(newSize < MIN_BUFFER_SIZE?MIN_BUFFER_SIZE:newSize)];
			}
			else
			{
				if (termBuffer.Length < newSize)
				{
					// Not big enough; create a new array with slight
					// over allocation:
					termBuffer = new char[ArrayUtil.GetNextSize(newSize)];
				}
			}
		}
		
		private void  InitTermBuffer()
		{
			if (termBuffer == null)
			{
                termBuffer = new char[ArrayUtil.GetNextSize(MIN_BUFFER_SIZE)];
                termLength = 0;
			}
		}
		
		/// <summary>Return number of valid characters (length of the term)
		/// in the termBuffer array. 
		/// </summary>
		public int TermLength()
		{
			InitTermBuffer();
			return termLength;
		}
		
		/// <summary>Set number of valid characters (length of the term) in
		/// the termBuffer array. Use this to truncate the termBuffer
		/// or to synchronize with external manipulation of the termBuffer.
		/// Note: to grow the size of the array,
		/// use <see cref="ResizeTermBuffer(int)" /> first.
		/// </summary>
		/// <param name="length">the truncated length
		/// </param>
		public void  SetTermLength(int length)
		{
			InitTermBuffer();
			if (length > termBuffer.Length)
				throw new System.ArgumentException("length " + length + " exceeds the size of the termBuffer (" + termBuffer.Length + ")");
			termLength = length;
		}

	    /// <summary>Gets or sets this Token's starting offset, the position of the first character
	    /// corresponding to this token in the source text.
	    /// Note that the difference between endOffset() and startOffset() may not be
	    /// equal to <see cref="TermLength"/>, as the term text may have been altered by a
	    /// stemmer or some other filter. 
	    /// </summary>
	    public virtual int StartOffset
	    {
	        get { return startOffset; }
	        set { this.startOffset = value; }
	    }

	    /// <summary>Gets or sets this Token's ending offset, one greater than the position of the
	    /// last character corresponding to this token in the source text. The length
	    /// of the token in the source text is (endOffset - startOffset). 
	    /// </summary>
	    public virtual int EndOffset
	    {
	        get { return endOffset; }
	        set { this.endOffset = value; }
	    }

	    /// <summary>Set the starting and ending offset.
		/// See StartOffset() and EndOffset()
		/// </summary>
		public virtual void  SetOffset(int startOffset, int endOffset)
		{
			this.startOffset = startOffset;
			this.endOffset = endOffset;
		}

	    /// <summary>Returns this Token's lexical type.  Defaults to "word". </summary>
	    public string Type
	    {
	        get { return type; }
	        set { this.type = value; }
	    }

	    /// <summary> EXPERIMENTAL:  While we think this is here to stay, we may want to change it to be a long.
	    /// <p/>
	    /// 
	    /// Get the bitset for any bits that have been set.  This is completely distinct from <see cref="Type()" />, although they do share similar purposes.
	    /// The flags can be used to encode information about the token for use by other <see cref="TokenFilter"/>s.
	    /// 
	    /// 
	    /// </summary>
	    /// <value> The bits </value>
	    public virtual int Flags
	    {
	        get { return flags; }
	        set { flags = value; }
	    }

	    /// <summary> Returns this Token's payload.</summary>
	    public virtual Payload Payload
	    {
	        get { return payload; }
	        set { payload = value; }
	    }

	    public override String ToString()
		{
			var sb = new System.Text.StringBuilder();
			sb.Append('(');
			InitTermBuffer();
			if (termBuffer == null)
				sb.Append("null");
			else
				sb.Append(termBuffer, 0, termLength);
			sb.Append(',').Append(startOffset).Append(',').Append(endOffset);
			if (!type.Equals("word"))
				sb.Append(",type=").Append(type);
			if (positionIncrement != 1)
				sb.Append(",posIncr=").Append(positionIncrement);
			sb.Append(')');
			return sb.ToString();
		}
		
		/// <summary>Resets the term text, payload, flags, and positionIncrement,
		/// startOffset, endOffset and token type to default.
		/// </summary>
		public override void  Clear()
		{
			payload = null;
			// Leave termBuffer to allow re-use
			termLength = 0;
			positionIncrement = 1;
			flags = 0;
			startOffset = endOffset = 0;
			type = DEFAULT_TYPE;
		}
		
		public override System.Object Clone()
		{
			var t = (Token) base.Clone();
			// Do a deep clone
			if (termBuffer != null)
			{
				t.termBuffer = new char[termBuffer.Length];
				termBuffer.CopyTo(t.termBuffer, 0);
			}
			if (payload != null)
			{
				t.payload = (Payload) payload.Clone();
			}
			return t;
		}
		
		/// <summary>Makes a clone, but replaces the term buffer &amp;
		/// start/end offset in the process.  This is more
		/// efficient than doing a full clone (and then calling
		/// setTermBuffer) because it saves a wasted copy of the old
		/// termBuffer. 
		/// </summary>
		public virtual Token Clone(char[] newTermBuffer, int newTermOffset, int newTermLength, int newStartOffset, int newEndOffset)
		{
			var t = new Token(newTermBuffer, newTermOffset, newTermLength, newStartOffset, newEndOffset)
			        	{positionIncrement = positionIncrement, flags = flags, type = type};
			if (payload != null)
				t.payload = (Payload) payload.Clone();
			return t;
		}
		
		public  override bool Equals(Object obj)
		{
			if (obj == this)
				return true;

			var other = obj as Token;
			if (other == null)
				return false;
			
			InitTermBuffer();
			other.InitTermBuffer();

			if (termLength == other.termLength && startOffset == other.startOffset && endOffset == other.endOffset &&
			    flags == other.flags && positionIncrement == other.positionIncrement && SubEqual(type, other.type) &&
			    SubEqual(payload, other.payload))
			{
				for (int i = 0; i < termLength; i++)
					if (termBuffer[i] != other.termBuffer[i])
						return false;
				return true;
			}
			return false;
		}
		
		private bool SubEqual(System.Object o1, System.Object o2)
		{
			if (o1 == null)
				return o2 == null;
			return o1.Equals(o2);
		}

		public override int GetHashCode()
		{
			InitTermBuffer();
			int code = termLength;
			code = code * 31 + startOffset;
			code = code * 31 + endOffset;
			code = code * 31 + flags;
			code = code * 31 + positionIncrement;
			code = code * 31 + type.GetHashCode();
			code = (payload == null?code:code * 31 + payload.GetHashCode());
			code = code * 31 + ArrayUtil.HashCode(termBuffer, 0, termLength);
			return code;
		}
		
		// like clear() but doesn't clear termBuffer/text
		private void  ClearNoTermBuffer()
		{
			payload = null;
			positionIncrement = 1;
			flags = 0;
			startOffset = endOffset = 0;
			type = DEFAULT_TYPE;
		}
		
		/// <summary>Shorthand for calling <see cref="Clear" />,
		/// <see cref="SetTermBuffer(char[], int, int)" />,
		/// <see cref="StartOffset" />,
		/// <see cref="EndOffset" />,
		/// <see cref="Type" />
		/// </summary>
		/// <returns> this Token instance 
		/// </returns>
		public virtual Token Reinit(char[] newTermBuffer, int newTermOffset, int newTermLength, int newStartOffset, int newEndOffset, System.String newType)
		{
			ClearNoTermBuffer();
			payload = null;
			positionIncrement = 1;
			SetTermBuffer(newTermBuffer, newTermOffset, newTermLength);
			startOffset = newStartOffset;
			endOffset = newEndOffset;
			type = newType;
			return this;
		}
		
		/// <summary>Shorthand for calling <see cref="Clear" />,
		/// <see cref="SetTermBuffer(char[], int, int)" />,
		/// <see cref="StartOffset" />,
		/// <see cref="EndOffset" />
		/// <see cref="Type" /> on Token.DEFAULT_TYPE
		/// </summary>
		/// <returns> this Token instance 
		/// </returns>
		public virtual Token Reinit(char[] newTermBuffer, int newTermOffset, int newTermLength, int newStartOffset, int newEndOffset)
		{
			ClearNoTermBuffer();
			SetTermBuffer(newTermBuffer, newTermOffset, newTermLength);
			startOffset = newStartOffset;
			endOffset = newEndOffset;
			type = DEFAULT_TYPE;
			return this;
		}
		
		/// <summary>Shorthand for calling <see cref="Clear" />,
		/// <see cref="SetTermBuffer(String)" />,
		/// <see cref="StartOffset" />,
		/// <see cref="EndOffset" />
		/// <see cref="Type" />
		/// </summary>
		/// <returns> this Token instance 
		/// </returns>
		public virtual Token Reinit(System.String newTerm, int newStartOffset, int newEndOffset, System.String newType)
		{
			ClearNoTermBuffer();
			SetTermBuffer(newTerm);
			startOffset = newStartOffset;
			endOffset = newEndOffset;
			type = newType;
			return this;
		}
		
		/// <summary>Shorthand for calling <see cref="Clear" />,
		/// <see cref="SetTermBuffer(String, int, int)" />,
		/// <see cref="StartOffset" />,
		/// <see cref="EndOffset" />
		/// <see cref="Type" />
		/// </summary>
		/// <returns> this Token instance 
		/// </returns>
		public virtual Token Reinit(System.String newTerm, int newTermOffset, int newTermLength, int newStartOffset, int newEndOffset, System.String newType)
		{
			ClearNoTermBuffer();
			SetTermBuffer(newTerm, newTermOffset, newTermLength);
			startOffset = newStartOffset;
			endOffset = newEndOffset;
			type = newType;
			return this;
		}
		
		/// <summary>Shorthand for calling <see cref="Clear" />,
		/// <see cref="SetTermBuffer(String)" />,
		/// <see cref="StartOffset" />,
		/// <see cref="EndOffset" />
		/// <see cref="Type" /> on Token.DEFAULT_TYPE
		/// </summary>
		/// <returns> this Token instance 
		/// </returns>
		public virtual Token Reinit(System.String newTerm, int newStartOffset, int newEndOffset)
		{
			ClearNoTermBuffer();
			SetTermBuffer(newTerm);
			startOffset = newStartOffset;
			endOffset = newEndOffset;
			type = DEFAULT_TYPE;
			return this;
		}
		
		/// <summary>Shorthand for calling <see cref="Clear" />,
		/// <see cref="SetTermBuffer(String, int, int)" />,
		/// <see cref="StartOffset" />,
		/// <see cref="EndOffset" />
		/// <see cref="Type" /> on Token.DEFAULT_TYPE
		/// </summary>
		/// <returns> this Token instance 
		/// </returns>
		public virtual Token Reinit(System.String newTerm, int newTermOffset, int newTermLength, int newStartOffset, int newEndOffset)
		{
			ClearNoTermBuffer();
			SetTermBuffer(newTerm, newTermOffset, newTermLength);
			startOffset = newStartOffset;
			endOffset = newEndOffset;
			type = DEFAULT_TYPE;
			return this;
		}
		
		/// <summary> Copy the prototype token's fields into this one. Note: Payloads are shared.</summary>
		/// <param name="prototype">
		/// </param>
		public virtual void  Reinit(Token prototype)
		{
			prototype.InitTermBuffer();
			SetTermBuffer(prototype.termBuffer, 0, prototype.termLength);
			positionIncrement = prototype.positionIncrement;
			flags = prototype.flags;
			startOffset = prototype.startOffset;
			endOffset = prototype.endOffset;
			type = prototype.type;
			payload = prototype.payload;
		}
		
		/// <summary> Copy the prototype token's fields into this one, with a different term. Note: Payloads are shared.</summary>
		/// <param name="prototype">
		/// </param>
		/// <param name="newTerm">
		/// </param>
		public virtual void  Reinit(Token prototype, System.String newTerm)
		{
			SetTermBuffer(newTerm);
			positionIncrement = prototype.positionIncrement;
			flags = prototype.flags;
			startOffset = prototype.startOffset;
			endOffset = prototype.endOffset;
			type = prototype.type;
			payload = prototype.payload;
		}
		
		/// <summary> Copy the prototype token's fields into this one, with a different term. Note: Payloads are shared.</summary>
		/// <param name="prototype">
		/// </param>
		/// <param name="newTermBuffer">
		/// </param>
		/// <param name="offset">
		/// </param>
		/// <param name="length">
		/// </param>
		public virtual void  Reinit(Token prototype, char[] newTermBuffer, int offset, int length)
		{
			SetTermBuffer(newTermBuffer, offset, length);
			positionIncrement = prototype.positionIncrement;
			flags = prototype.flags;
			startOffset = prototype.startOffset;
			endOffset = prototype.endOffset;
			type = prototype.type;
			payload = prototype.payload;
		}
		
		public override void  CopyTo(Attribute target)
		{
			if (target is Token)
			{
				var to = (Token) target;
				to.Reinit(this);
				// reinit shares the payload, so clone it:
				if (payload != null)
				{
					to.payload = (Payload) payload.Clone();
				}
			}
			else
			{
				InitTermBuffer();
				((ITermAttribute) target).SetTermBuffer(termBuffer, 0, termLength);
				((IOffsetAttribute) target).SetOffset(startOffset, endOffset);
				((IPositionIncrementAttribute) target).PositionIncrement = positionIncrement;
				((IPayloadAttribute) target).Payload = (payload == null)?null:(Payload) payload.Clone();
				((IFlagsAttribute) target).Flags = flags;
				((ITypeAttribute) target).Type = type;
			}
		}
       
        ///<summary>
        /// Convenience factory that returns <c>Token</c> as implementation for the basic
        /// attributes and return the default impl (with &quot;Impl&quot; appended) for all other
        /// attributes.
        /// @since 3.0
        /// </summary>
	    public static AttributeSource.AttributeFactory TOKEN_ATTRIBUTE_FACTORY =
	        new TokenAttributeFactory(AttributeSource.AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY);
  
        /// <summary>
        /// <b>Expert</b>: Creates an AttributeFactory returning {@link Token} as instance for the basic attributes
        /// and for all other attributes calls the given delegate factory.
        /// </summary>
        public class TokenAttributeFactory : AttributeSource.AttributeFactory
        {

            private readonly AttributeSource.AttributeFactory _delegateFactory;

            /// <summary>
            /// <b>Expert</b>: Creates an AttributeFactory returning {@link Token} as instance for the basic attributes
            /// and for all other attributes calls the given delegate factory.
            /// </summary>
            public TokenAttributeFactory(AttributeSource.AttributeFactory delegateFactory)
            {
                this._delegateFactory = delegateFactory;
            }

            public override Attribute CreateAttributeInstance<T>()
            {
                return typeof(T).IsAssignableFrom(typeof(Token))
                           ? new Token()
                           : _delegateFactory.CreateAttributeInstance<T>();
            }

            public override bool Equals(Object other)
            {
                if (this == other) return true;

            	var af = other as TokenAttributeFactory;
            	return af != null && _delegateFactory.Equals(af._delegateFactory);
            }

            public override int GetHashCode()
            {
                return _delegateFactory.GetHashCode() ^ 0x0a45aa31;
            }
        }
    }
}