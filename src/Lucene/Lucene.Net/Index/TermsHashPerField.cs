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
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Documents;
using Lucene.Net.Support;
using Lucene.Net.Util;
using UnicodeUtil = Lucene.Net.Util.UnicodeUtil;

namespace Lucene.Net.Index
{	
	internal sealed class TermsHashPerField : InvertedDocConsumerPerField
	{
		private void InitBlock()
		{
			postingsHashHalfSize = postingsHashSize / 2;
			postingsHashMask = postingsHashSize - 1;
		    postingsHash = ArrayPool<RawPostingList>.Shared.Rent(postingsHashSize);
		}
		
		internal TermsHashConsumerPerField consumer;
		internal TermsHashPerField nextPerField;
		internal TermsHashPerThread perThread;
		internal DocumentsWriter.DocState docState;
		internal FieldInvertState fieldState;
		internal ITermAttribute termAtt;
		
		// Copied from our perThread
		internal CharBlockPool charPool;
		internal IntBlockPool intPool;
		internal ByteBlockPool bytePool;
		
		internal int streamCount;
		internal int numPostingInt;
		
		internal FieldInfo fieldInfo;
		
		internal bool postingsCompacted;
		internal int numPostings;
		private int postingsHashSize = 4;
		private int postingsHashHalfSize;
		private int postingsHashMask;
		private RawPostingList[] postingsHash;
		private RawPostingList p;

	    private readonly Sorter<RawPostingList, PostingComparer> _sorter;
		
		public TermsHashPerField(DocInverterPerField docInverterPerField, TermsHashPerThread perThread, TermsHashPerThread nextPerThread, FieldInfo fieldInfo)
		{
            InitBlock();
			this.perThread = perThread;

            intPool = perThread.intPool;
			charPool = perThread.charPool;
			bytePool = perThread.bytePool;
			docState = perThread.docState;
			fieldState = docInverterPerField.fieldState;
            
            // Sorter requires the char pool.
		    _sorter = new Sorter<RawPostingList, PostingComparer>(new PostingComparer(this));

            this.consumer = perThread.consumer.AddField(this, fieldInfo);
			streamCount = consumer.GetStreamCount();
			numPostingInt = 2 * streamCount;
			this.fieldInfo = fieldInfo;		    

            if (nextPerThread != null)
				nextPerField = (TermsHashPerField) nextPerThread.AddField(docInverterPerField, fieldInfo);
			else
				nextPerField = null;


		}
		
		internal void ShrinkHash(int targetSize)
		{
			System.Diagnostics.Debug.Assert(postingsCompacted || numPostings == 0);

            int newSize = 4;
			
			if (newSize != postingsHash.Length)
			{
			    ArrayPool<RawPostingList>.Shared.Return(postingsHash, clearArray: true);

			    postingsHash = ArrayPool<RawPostingList>.Shared.Rent(newSize);
				postingsHashSize = newSize;
				postingsHashHalfSize = newSize / 2;
				postingsHashMask = newSize - 1;
			}

		    System.Array.Clear(postingsHash, 0, postingsHash.Length);
		}
		
		public void Reset()
		{
			if (!postingsCompacted)
				CompactPostings();

			System.Diagnostics.Debug.Assert(numPostings <= postingsHash.Length);

			if (numPostings > 0)
			{
				perThread.termsHash.RecyclePostings(postingsHash, numPostings);
                Array.Clear(postingsHash, 0, numPostings);
				numPostings = 0;
			}
			postingsCompacted = false;
		    nextPerField?.Reset();
		}
		
		public override void Abort()
		{
			lock (this)
			{
				Reset();
			    nextPerField?.Abort();
			}
		}
		
		public void InitReader(ByteSliceReader reader, RawPostingList p, int stream)
		{
			System.Diagnostics.Debug.Assert(stream < streamCount);

			int[] ints = intPool.buffers[p.intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
			int upto = p.intStart & DocumentsWriter.INT_BLOCK_MASK;
			reader.Init(bytePool, p.byteStart + stream * ByteBlockPool.FIRST_LEVEL_SIZE, ints[upto + stream]);
		}
		
		private void CompactPostings()
		{
			lock (this)
			{
				int upto = 0;
				for (int i = 0; i < postingsHashSize; i++)
				{
					if (postingsHash[i] != null)
					{
						if (upto < i)
						{
							postingsHash[upto] = postingsHash[i];
							postingsHash[i] = null;
						}
						upto++;
					}
				}
				
				System.Diagnostics.Debug.Assert(upto == numPostings);
				postingsCompacted = true;
			}
		}
		
		/// <summary>Collapse the hash table &amp; sort in-place. </summary>
		public RawPostingList[] SortPostings()
		{
			CompactPostings();
            _sorter.Sort(postingsHash, 0, numPostings);
			return postingsHash;
		}

	    private struct PostingComparer : IComparer<RawPostingList>
	    {
	        private readonly TermsHashPerField _parent;

	        public PostingComparer(TermsHashPerField parent)
	        {
	            this._parent = parent;
	        }

	        [MethodImpl(MethodImplOptions.AggressiveInlining)]
	        public int Compare(RawPostingList p1, RawPostingList p2)
	        {
	            int result = 0;
	            if (p1 == p2)
	                goto Return;

	            var buffers = this._parent.charPool.buffers;

	            int p1TextStart = p1.textStart;
	            int p2TextStart = p2.textStart;

	            char[] text1 = buffers[p1TextStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
	            char[] text2 = buffers[p2TextStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];

	            int pos1 = p1TextStart & DocumentsWriter.CHAR_BLOCK_MASK;
	            int pos2 = p2TextStart & DocumentsWriter.CHAR_BLOCK_MASK;

	            System.Diagnostics.Debug.Assert(text1 != text2 || pos1 != pos2);

	            char c1;
	            char c2;
	            while (true)
	            {
	                c1 = text1[pos1++];
	                c2 = text2[pos2++];
	                if (c1 != c2)
	                    break;

	                // This method should never compare equal postings
	                // unless p1==p2
	                System.Diagnostics.Debug.Assert(c1 != 0xffff);
	            }

                if (0xffff == c2)
                    result = 1;
                else if (0xffff == c1)
                    result = -1;
                else
                    result = c1 - c2;

	            Return:
                return result;
	        }
	    }

        /// <summary>Test whether the text for current RawPostingList currentP equals
        /// current tokenText. 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool PostingEquals(ref RawPostingList currentP, char[] tokenText, int tokenTextLen)
		{		
			char[] text = perThread.charPool.buffers[currentP.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
			System.Diagnostics.Debug.Assert(text != null);
			int pos = currentP.textStart & DocumentsWriter.CHAR_BLOCK_MASK;
			
			int tokenPos = 0;
		    for (; tokenPos < tokenTextLen; pos++, tokenPos++)
		    {
		        if (tokenText[tokenPos] != text[pos])
		            goto ReturnFalse;
            }

            return 0xffff == text[pos];

            ReturnFalse:
		    return false;
		}
		
		private bool doCall;
		private bool doNextCall;
		
		internal override void  Start(IFieldable f)
		{
			termAtt = fieldState.attributeSource.AddAttribute<ITermAttribute>();
			consumer.Start(f);
		    nextPerField?.Start(f);
		}
		
		internal override bool Start(IFieldable[] fields, int count)
		{
			doCall = consumer.Start(fields, count);
			if (nextPerField != null)
				doNextCall = nextPerField.Start(fields, count);
			return doCall || doNextCall;
		}
		
		// Secondary entry point (for 2nd & subsequent TermsHash),
		// because token text has already been "interned" into
		// textStart, so we hash by textStart
		public void  Add(int textStart)
		{
			
			int code = textStart;
			
			int hashPos = code & postingsHashMask;
			
			System.Diagnostics.Debug.Assert(!postingsCompacted);
			
			// Locate RawPostingList in hash
			p = postingsHash[hashPos];
			
			if (p != null && p.textStart != textStart)
			{
				// Conflict: keep searching different locations in
				// the hash table.
				int inc = ((code >> 8) + code) | 1;
				do 
				{
					code += inc;
					hashPos = code & postingsHashMask;
					p = postingsHash[hashPos];
				}
				while (p != null && p.textStart != textStart);
			}
			
			if (p == null)
			{
				
				// First time we are seeing this token since we last
				// flushed the hash.
				
				// Refill?
				if (0 == perThread.freePostingsCount)
					perThread.MorePostings();
				
				// Pull next free RawPostingList from free list
				p = perThread.freePostings[--perThread.freePostingsCount];
				System.Diagnostics.Debug.Assert(p != null);
				
				p.textStart = textStart;
				
				System.Diagnostics.Debug.Assert(postingsHash [hashPos] == null);
				postingsHash[hashPos] = p;
				numPostings++;
				
				if (numPostings == postingsHashHalfSize)
					RehashPostings(2 * postingsHashSize);
				
				// Init stream slices
				if (numPostingInt + intPool.intUpto > DocumentsWriter.INT_BLOCK_SIZE)
					intPool.NextBuffer();
				
				if (DocumentsWriter.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt * ByteBlockPool.FIRST_LEVEL_SIZE)
					bytePool.NextBuffer();
				
				intUptos = intPool.buffer;
				intUptoStart = intPool.intUpto;
				intPool.intUpto += streamCount;
				
				p.intStart = intUptoStart + intPool.intOffset;
				
				for (int i = 0; i < streamCount; i++)
				{
					int upto = bytePool.NewSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
					intUptos[intUptoStart + i] = upto + bytePool.byteOffset;
				}
				p.byteStart = intUptos[intUptoStart];
				
				consumer.NewTerm(p);
			}
			else
			{
				intUptos = intPool.buffers[p.intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
				intUptoStart = p.intStart & DocumentsWriter.INT_BLOCK_MASK;
				consumer.AddTerm(p);
			}
		}
		
		// Primary entry point (for first TermsHash)
		internal override void Add()
		{			
			System.Diagnostics.Debug.Assert(!postingsCompacted);

            // We are first in the chain so we must "intern" the
            // term text into textStart address

		    char[] tokenText;
		    int tokenTextLen;

            // Get the text of this term.
            var termAttConcrete = termAtt as TermAttribute;
		    if (termAttConcrete != null)
		    {
                // PERF: Fast-path to avoid the method calls at the expense of a larger method.
		        tokenText = termAttConcrete.TermBuffer();
		        tokenTextLen = termAttConcrete.TermLength();
            }
		    else
		    {
		        tokenText = termAtt.TermBuffer();
		        tokenTextLen = termAtt.TermLength();
            }
			
			// Compute hashcode & replace any invalid UTF16 sequences
			int downto = tokenTextLen;
			int code = 0;
			while (downto > 0)
			{
				char ch = tokenText[--downto];
				
				if (ch >= UnicodeUtil.UNI_SUR_LOW_START && ch <= UnicodeUtil.UNI_SUR_LOW_END)
				{
					if (0 == downto)
					{
						// Unpaired
						ch = tokenText[downto] = (char) (UnicodeUtil.UNI_REPLACEMENT_CHAR);
					}
					else
					{
						char ch2 = tokenText[downto - 1];
						if (ch2 >= UnicodeUtil.UNI_SUR_HIGH_START && ch2 <= UnicodeUtil.UNI_SUR_HIGH_END)
						{
							// OK: high followed by low.  This is a valid
							// surrogate pair.
							code = ((code * 31) + ch) * 31 + ch2;
							downto--;
							continue;
						}
						else
						{
							// Unpaired
							ch = tokenText[downto] = (char) (UnicodeUtil.UNI_REPLACEMENT_CHAR);
						}
					}
				}
				else if (ch >= UnicodeUtil.UNI_SUR_HIGH_START && (ch <= UnicodeUtil.UNI_SUR_HIGH_END || ch == 0xffff))
				{
					// Unpaired or 0xffff
					ch = tokenText[downto] = (char) (UnicodeUtil.UNI_REPLACEMENT_CHAR);
				}
				
				code = (code * 31) + ch;
			}
			
			int hashPos = code & postingsHashMask;
			
			// Locate RawPostingList in hash
			var pAux = postingsHash[hashPos];			
			if (pAux != null && !PostingEquals(ref pAux, tokenText, tokenTextLen))
			{
				// Conflict: keep searching different locations in
				// the hash table.
				int inc = ((code >> 8) + code) | 1;
				do 
				{
					code += inc;
					hashPos = code & postingsHashMask;
				    pAux = postingsHash[hashPos];
				}
				while (pAux != null && !PostingEquals(ref pAux, tokenText, tokenTextLen));
			}
		    p = pAux;

			if (p == null)
			{				
				// First time we are seeing this token since we last
				// flushed the hash.
				int textLen1 = 1 + tokenTextLen;
				if (textLen1 + charPool.charUpto > DocumentsWriter.CHAR_BLOCK_SIZE)
				{
					if (textLen1 > DocumentsWriter.CHAR_BLOCK_SIZE)
					{
						// Just skip this term, to remain as robust as
						// possible during indexing.  A TokenFilter
						// can be inserted into the analyzer chain if
						// other behavior is wanted (pruning the term
						// to a prefix, throwing an exception, etc).
						
						if (docState.maxTermPrefix == null)
							docState.maxTermPrefix = new System.String(tokenText, 0, 30);
						
						consumer.SkippingLongTerm();
						return ;
					}
					charPool.NextBuffer();
				}
				
				// Refill?
				if (0 == perThread.freePostingsCount)
					perThread.MorePostings();
				
				// Pull next free RawPostingList from free list
				p = perThread.freePostings[--perThread.freePostingsCount];
				System.Diagnostics.Debug.Assert(p != null);
				
				char[] text = charPool.buffer;
				int textUpto = charPool.charUpto;
				p.textStart = textUpto + charPool.charOffset;
				charPool.charUpto += textLen1;
				Array.Copy(tokenText, 0, text, textUpto, tokenTextLen);
				text[textUpto + tokenTextLen] = (char) (0xffff);
				
				System.Diagnostics.Debug.Assert(postingsHash [hashPos] == null);
				postingsHash[hashPos] = p;
				numPostings++;
				
				if (numPostings == postingsHashHalfSize)
					RehashPostings(2 * postingsHashSize);
				
				// Init stream slices
				if (numPostingInt + intPool.intUpto > DocumentsWriter.INT_BLOCK_SIZE)
					intPool.NextBuffer();
				
				if (DocumentsWriter.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt * ByteBlockPool.FIRST_LEVEL_SIZE)
					bytePool.NextBuffer();
				
				intUptos = intPool.buffer;
				intUptoStart = intPool.intUpto;
				intPool.intUpto += streamCount;
				
				p.intStart = intUptoStart + intPool.intOffset;
				
				for (int i = 0; i < streamCount; i++)
				{
					int upto = bytePool.NewSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
					intUptos[intUptoStart + i] = upto + bytePool.byteOffset;
				}
				p.byteStart = intUptos[intUptoStart];
				
				consumer.NewTerm(p);
			}
			else
			{
				intUptos = intPool.buffers[p.intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
				intUptoStart = p.intStart & DocumentsWriter.INT_BLOCK_MASK;
				consumer.AddTerm(p);
			}
			
			if (doNextCall)
				nextPerField.Add(p.textStart);
		}
		
		internal int[] intUptos;
		internal int intUptoStart;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
	    internal void WriteByte(int stream, byte b)
	    {
	        int upto = intUptos[intUptoStart + stream];
	        byte[] bytes = bytePool.buffers[upto >> DocumentsWriter.BYTE_BLOCK_SHIFT];

	        System.Diagnostics.Debug.Assert(bytes != null);
	        int offset = upto & DocumentsWriter.BYTE_BLOCK_MASK;
	        if (bytes[offset] != 0)
                goto WithNewSlice;

	        bytes[offset] = b;
	        (intUptos[intUptoStart + stream])++;
	        return;
            
            WithNewSlice:
            WriteByteUnlikely(bytes, offset, stream, b);
	    }

	    internal void WriteByteUnlikely(byte[] bytes, int offset, int stream, byte b)
	    {
	        // End of slice; allocate a new one
	        offset = bytePool.AllocSlice(bytes, offset);
	        bytes = bytePool.buffer;
	        intUptos[intUptoStart + stream] = offset + bytePool.byteOffset;

            bytes[offset] = b;
	        (intUptos[intUptoStart + stream])++;
	    }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteBytes(int stream, byte[] b, int offset, int len)
		{
			// TODO: optimize
			int end = offset + len;
			for (int i = offset; i < end; i++)
				WriteByte(stream, b[i]);
		}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void  WriteVInt(int stream, int i)
		{
			System.Diagnostics.Debug.Assert(stream < streamCount);
			while ((i & ~ 0x7F) != 0)
			{
				WriteByte(stream, (byte) ((i & 0x7f) | 0x80));
				i = Number.URShift(i, 7);
			}
			WriteByte(stream, (byte) i);
		}
		
		internal override void  Finish()
		{
			consumer.Finish();
		    nextPerField?.Finish();
		}
		
		/// <summary>Called when postings hash is too small (> 50%
        /// occupied) or too large (&lt; 20% occupied). 
		/// </summary>
		internal void  RehashPostings(int newSize)
		{
			
			int newMask = newSize - 1;

		    RawPostingList[] newHash = ArrayPool<RawPostingList>.Shared.Rent(newSize);
			for (int i = 0; i < postingsHashSize; i++)
			{
				RawPostingList p0 = postingsHash[i];
				if (p0 != null)
				{
					int code;
					if (perThread.primary)
					{
						int start = p0.textStart & DocumentsWriter.CHAR_BLOCK_MASK;
						char[] text = charPool.buffers[p0.textStart >> DocumentsWriter.CHAR_BLOCK_SHIFT];
						int pos = start;
						while (text[pos] != 0xffff)
							pos++;
						code = 0;
						while (pos > start)
							code = (code * 31) + text[--pos];
					}
					else
						code = p0.textStart;
					
					int hashPos = code & newMask;
					System.Diagnostics.Debug.Assert(hashPos >= 0);
					if (newHash[hashPos] != null)
					{
						int inc = ((code >> 8) + code) | 1;
						do 
						{
							code += inc;
							hashPos = code & newMask;
						}
						while (newHash[hashPos] != null);
					}
					newHash[hashPos] = p0;
				}
			}
			
            ArrayPool<RawPostingList>.Shared.Return(postingsHash, clearArray: true);

			postingsHashMask = newMask;
			postingsHash = newHash;
			postingsHashSize = newSize;
			postingsHashHalfSize = newSize >> 1;
		}
	}
}