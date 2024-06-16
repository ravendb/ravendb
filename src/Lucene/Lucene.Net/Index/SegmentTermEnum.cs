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

using Lucene.Net.Store;
using Lucene.Net.Util;
using IndexInput = Lucene.Net.Store.IndexInput;

namespace Lucene.Net.Index
{
	internal sealed class SegmentTermEnum : TermEnum, ILuceneCloneable
	{
		private IndexInput input;
		internal FieldInfos fieldInfos;
		internal long size;
		internal long position = - 1;
		
        internal TermBuffer termBuffer = new TermBuffer();
		private TermBuffer prevBuffer = new TermBuffer();
		private TermBuffer scanBuffer = new TermBuffer(); // used for scanning
		
		private TermInfo termInfo;
		
		private readonly int format;
		private readonly bool isIndex = false;
		internal long indexPointer = 0;
		internal int indexInterval;
		internal int skipInterval;
		internal int maxSkipLevels;
		private readonly int formatM1SkipInterval;
		
		internal SegmentTermEnum(IndexInput i, FieldInfos fis, bool isi, IState state)
		{
			input = i;
			fieldInfos = fis;
			isIndex = isi;
			maxSkipLevels = 1; // use single-level skip lists for formats > -3 
			
			int firstInt = input.ReadInt(state);
			if (firstInt >= 0)
			{
				// original-format file, without explicit format version number
				format = 0;
				size = firstInt;
				
				// back-compatible settings
				indexInterval = 128;
				skipInterval = System.Int32.MaxValue; // switch off skipTo optimization
			}
			else
			{
				// we have a format version number
				format = firstInt;
				
				// check that it is a format we can understand
				if (format < TermInfosWriter.FORMAT_CURRENT)
					throw new CorruptIndexException("Unknown format version:" + format + " expected " + TermInfosWriter.FORMAT_CURRENT + " or higher");
				
				size = input.ReadLong(state); // read the size
				
				if (format == - 1)
				{
					if (!isIndex)
					{
						indexInterval = input.ReadInt(state);
						formatM1SkipInterval = input.ReadInt(state);
					}
					// switch off skipTo optimization for file format prior to 1.4rc2 in order to avoid a bug in 
					// skipTo implementation of these versions
					skipInterval = System.Int32.MaxValue;
				}
				else
				{
					indexInterval = input.ReadInt(state);
					skipInterval = input.ReadInt(state);
					if (format <= TermInfosWriter.FORMAT)
					{
						// this new format introduces multi-level skipping
						maxSkipLevels = input.ReadInt(state);
					}
				}
				System.Diagnostics.Debug.Assert(indexInterval > 0, "indexInterval=" + indexInterval + " is negative; must be > 0");
				System.Diagnostics.Debug.Assert(skipInterval > 0, "skipInterval=" + skipInterval + " is negative; must be > 0");
			}
			if (format > TermInfosWriter.FORMAT_VERSION_UTF8_LENGTH_IN_BYTES)
			{
				termBuffer.SetPreUTF8Strings();
				scanBuffer.SetPreUTF8Strings();
				prevBuffer.SetPreUTF8Strings();
			}
		}
		
		public System.Object Clone(IState state)
		{
			SegmentTermEnum clone = null;
			try
			{
				clone = (SegmentTermEnum) base.MemberwiseClone();
			}
			catch (System.Exception)
			{
			}
			
			clone.input = (IndexInput) input.Clone(state);
			clone.termInfo = new TermInfo(termInfo);
			
			clone.termBuffer = (TermBuffer) termBuffer.Clone();
			clone.prevBuffer = (TermBuffer) prevBuffer.Clone();
			clone.scanBuffer = new TermBuffer();
			
			return clone;
		}
		
		internal void  Seek(long pointer, long p, Term t, TermInfo ti, IState state)
		{
			input.Seek(pointer, state);
			position = p;
			termBuffer.Set(t);
			prevBuffer.Reset();
			termInfo.Set(ti);
		}
		
		/// <summary>Increments the enumeration to the next element.  True if one exists.</summary>
		public override bool Next(IState state)
		{
			if (position++ >= size - 1)
			{
				prevBuffer.Set(termBuffer);
				termBuffer.Reset();
				return false;
			}
			
			prevBuffer.Set(termBuffer);
			termBuffer.Read(input, fieldInfos, state);
			
			termInfo.docFreq = input.ReadVInt(state); // read doc freq
			termInfo.freqPointer += input.ReadVLong(state); // read freq pointer
			termInfo.proxPointer += input.ReadVLong(state); // read prox pointer
			
			if (format == - 1)
			{
				//  just read skipOffset in order to increment  file pointer; 
				// value is never used since skipTo is switched off
				if (!isIndex)
				{
					if (termInfo.docFreq > formatM1SkipInterval)
					{
						termInfo.skipOffset = input.ReadVInt(state);
					}
				}
			}
			else
			{
				if (termInfo.docFreq >= skipInterval)
					termInfo.skipOffset = input.ReadVInt(state);
			}
			
			if (isIndex)
				indexPointer += input.ReadVLong(state); // read index pointer
			
			return true;
		}
		
		/// <summary>Optimized scan, without allocating new terms. 
		/// Return number of invocations to next(). 
		/// </summary>
		internal int ScanTo(Term term, IState state)
		{
			scanBuffer.Set(term);
			int count = 0;
			while (scanBuffer.CompareTo(termBuffer) > 0 && Next(state))
			{
				count++;
			}
			return count;
		}

	    /// <summary>Returns the current Term in the enumeration.
	    /// Initially invalid, valid after next() called for the first time.
	    /// </summary>
	    public override Term Term
	    {
	        get { return termBuffer.ToTerm(); }
	    }

	    /// <summary>Returns the previous Term enumerated. Initially null.</summary>
		public /*internal*/ Term Prev()
		{
			return prevBuffer.ToTerm();
		}
		
		/// <summary>Returns the current TermInfo in the enumeration.
		/// Initially invalid, valid after next() called for the first time.
		/// </summary>
		internal TermInfo TermInfo()
		{
			return new TermInfo(termInfo);
		}
		
		/// <summary>Sets the argument to the current TermInfo in the enumeration.
		/// Initially invalid, valid after next() called for the first time.
		/// </summary>
		internal void  TermInfo(TermInfo ti)
		{
			ti.Set(termInfo);
		}
		
		/// <summary>Returns the docFreq from the current TermInfo in the enumeration.
		/// Initially invalid, valid after next() called for the first time.
		/// </summary>
		public override int DocFreq()
		{
			return termInfo.docFreq;
		}
		
		/* Returns the freqPointer from the current TermInfo in the enumeration.
		Initially invalid, valid after next() called for the first time.*/
		internal long FreqPointer()
		{
			return termInfo.freqPointer;
		}
		
		/* Returns the proxPointer from the current TermInfo in the enumeration.
		Initially invalid, valid after next() called for the first time.*/
		internal long ProxPointer()
		{
			return termInfo.proxPointer;
		}
		
		/// <summary>Closes the enumeration to further activity, freeing resources. </summary>
        protected override void Dispose(bool disposing)
        {
            input.Dispose();
        }
	}
}