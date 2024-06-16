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
using Lucene.Net.Util;

namespace Lucene.Net.Store
{
	
	/// <summary>Base implementation class for buffered <see cref="IndexInput" />. </summary>
	public abstract class BufferedIndexInput : IndexInput, ILuceneCloneable
	{
		
		/// <summary>Default buffer size </summary>
		public const int BUFFER_SIZE = 1024;
		
		private int _bufferSize = BUFFER_SIZE;
		
		protected internal byte[] buffer;
		
		protected long bufferStart = 0; // position in file of buffer
		protected int bufferLength = 0; // end of valid bytes
		protected int bufferPosition = 0; // next byte to read
		
		public override byte ReadByte(IState state)
		{
			if (bufferPosition >= bufferLength)
				Refill(state);
			return buffer[bufferPosition++];
		}

	    protected BufferedIndexInput()
		{
		}
		
		/// <summary>Inits BufferedIndexInput with a specific bufferSize </summary>
		protected BufferedIndexInput(int bufferSize)
		{
			CheckBufferSize(bufferSize);
			this._bufferSize = bufferSize;
		}
		
		/// <summary>Change the buffer size used by this IndexInput </summary>
		public virtual void  SetBufferSize(int newSize)
		{
			System.Diagnostics.Debug.Assert(buffer == null || _bufferSize == buffer.Length, "buffer=" + buffer + " bufferSize=" + _bufferSize + " buffer.length=" +(buffer != null ? buffer.Length: 0));
			if (newSize != _bufferSize)
			{
				CheckBufferSize(newSize);
				_bufferSize = newSize;
				if (buffer != null)
				{
					// Resize the existing buffer and carefully save as
					// many bytes as possible starting from the current
					// bufferPosition
					byte[] newBuffer = new byte[newSize];
					int leftInBuffer = bufferLength - bufferPosition;
					int numToCopy;
					if (leftInBuffer > newSize)
						numToCopy = newSize;
					else
						numToCopy = leftInBuffer;
					Array.Copy(buffer, bufferPosition, newBuffer, 0, numToCopy);
					bufferStart += bufferPosition;
					bufferPosition = 0;
					bufferLength = numToCopy;
					buffer = newBuffer;
				}
			}
		}
		
		protected virtual void NewBuffer(int newBufferSize)
		{
			// Subclasses can do something here
			buffer = new byte[newBufferSize];
		}

	    /// <seealso cref="SetBufferSize">
	    /// </seealso>
	    public virtual int BufferSize
	    {
	        get { return _bufferSize; }
	    }

	    private void  CheckBufferSize(int bufferSize)
		{
			if (bufferSize <= 0)
				throw new System.ArgumentException("bufferSize must be greater than 0 (got " + bufferSize + ")");
		}
		
		public override void  ReadBytes(byte[] b, int offset, int len, IState state)
		{
			ReadBytes(b, offset, len, true, state);
		}
		
		public override void  ReadBytes(byte[] b, int offset, int len, bool useBuffer, IState state)
		{
			
			if (len <= (bufferLength - bufferPosition))
			{
				// the buffer contains enough data to satisfy this request
				if (len > 0)
				// to allow b to be null if len is 0...
					Array.Copy(buffer, bufferPosition, b, offset, len);
				bufferPosition += len;
			}
			else
			{
				// the buffer does not have enough data. First serve all we've got.
				int available = bufferLength - bufferPosition;
				if (available > 0)
				{
					Array.Copy(buffer, bufferPosition, b, offset, available);
					offset += available;
					len -= available;
					bufferPosition += available;
				}
				// and now, read the remaining 'len' bytes:
				if (useBuffer && len < _bufferSize)
				{
					// If the amount left to read is small enough, and
					// we are allowed to use our buffer, do it in the usual
					// buffered way: fill the buffer and copy from it:
					Refill(state);
					if (bufferLength < len)
					{
						// Throw an exception when refill() could not read len bytes:
						Array.Copy(buffer, 0, b, offset, bufferLength);
						throw new System.IO.IOException("read past EOF");
					}
					else
					{
						Array.Copy(buffer, 0, b, offset, len);
						bufferPosition = len;
					}
				}
				else
				{
					// The amount left to read is larger than the buffer
					// or we've been asked to not use our buffer -
					// there's no performance reason not to read it all
					// at once. Note that unlike the previous code of
					// this function, there is no need to do a seek
					// here, because there's no need to reread what we
					// had in the buffer.
					long after = bufferStart + bufferPosition + len;
					if (after > Length(state))
						throw new System.IO.IOException("read past EOF");
					ReadInternal(b, offset, len, state);
					bufferStart = after;
					bufferPosition = 0;
					bufferLength = 0; // trigger refill() on read
				}
			}
		}
		
		protected void Refill(IState state)
		{
			long start = bufferStart + bufferPosition;
			long end = start + _bufferSize;
			if (end > Length(state))
			// don't read past EOF
				end = Length(state);
			int newLength = (int) (end - start);
			if (newLength <= 0)
				throw new System.IO.IOException("read past EOF");
			
			if (buffer == null)
			{
				NewBuffer(_bufferSize); // allocate buffer lazily
				SeekInternal(bufferStart, state);
			}
			ReadInternal(buffer, 0, newLength, state);
			bufferLength = newLength;
			bufferStart = start;
			bufferPosition = 0;
		}
		
		/// <summary>Expert: implements buffer refill.  Reads bytes from the current position
		/// in the input.
		/// </summary>
		/// <param name="b">the array to read bytes into
		/// </param>
		/// <param name="offset">the offset in the array to start storing bytes
		/// </param>
		/// <param name="length">the number of bytes to read
		/// </param>
		public abstract void  ReadInternal(byte[] b, int offset, int length, IState state);

	    public override long FilePointer(IState state)
	    {
	        return bufferStart + bufferPosition;
	    }

	    public override void  Seek(long pos, IState state)
		{
			if (pos >= bufferStart && pos < (bufferStart + bufferLength))
				bufferPosition = (int) (pos - bufferStart);
			// seek within buffer
			else
			{
				bufferStart = pos;
				bufferPosition = 0;
				bufferLength = 0; // trigger refill() on read()
				SeekInternal(pos, state);
			}
		}
		
		/// <summary>Expert: implements seek.  Sets current position in this file, where the
		/// next <see cref="ReadInternal(byte[],int,int)" /> will occur.
		/// </summary>
		/// <seealso cref="ReadInternal(byte[],int,int)">
		/// </seealso>
		public abstract void  SeekInternal(long pos, IState state);
		
		public override System.Object Clone(IState state)
		{
			BufferedIndexInput clone = (BufferedIndexInput) base.Clone(state);
			
			clone.buffer = null;
			clone.bufferLength = 0;
			clone.bufferPosition = 0;
			clone.bufferStart = FilePointer(state);
			
			return clone;
		}
	}
}