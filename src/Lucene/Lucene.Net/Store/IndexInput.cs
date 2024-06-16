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
using System.Collections.Generic;
using Lucene.Net.Support;
using Lucene.Net.Util;

namespace Lucene.Net.Store
{
	
	/// <summary>Abstract base class for input from a file in a <see cref="Directory" />.  A
	/// random-access input stream.  Used for all Lucene index input operations.
	/// </summary>
	/// <seealso cref="Directory">
	/// </seealso>
	public abstract class IndexInput : ILuceneCloneable, IDisposable
	{
		private bool preUTF8Strings; // true if we are reading old (modified UTF8) string format
		
		/// <summary>Reads and returns a single byte.</summary>
		/// <seealso cref="IndexOutput.WriteByte(byte)">
		/// </seealso>
		public abstract byte ReadByte(IState state);
		
		/// <summary>Reads a specified number of bytes into an array at the specified offset.</summary>
		/// <param name="b">the array to read bytes into
		/// </param>
		/// <param name="offset">the offset in the array to start storing bytes
		/// </param>
		/// <param name="len">the number of bytes to read
		/// </param>
		/// <seealso cref="IndexOutput.WriteBytes(byte[],int)">
		/// </seealso>
		public abstract void  ReadBytes(byte[] b, int offset, int len, IState state);
		
		/// <summary>Reads a specified number of bytes into an array at the
		/// specified offset with control over whether the read
		/// should be buffered (callers who have their own buffer
		/// should pass in "false" for useBuffer).  Currently only
		/// <see cref="BufferedIndexInput" /> respects this parameter.
		/// </summary>
		/// <param name="b">the array to read bytes into
		/// </param>
		/// <param name="offset">the offset in the array to start storing bytes
		/// </param>
		/// <param name="len">the number of bytes to read
		/// </param>
		/// <param name="useBuffer">set to false if the caller will handle
		/// buffering.
		/// </param>
		/// <seealso cref="IndexOutput.WriteBytes(byte[],int)">
		/// </seealso>
		public virtual void  ReadBytes(byte[] b, int offset, int len, bool useBuffer, IState state)
		{
			// Default to ignoring useBuffer entirely
			ReadBytes(b, offset, len, state);
		}
		
		/// <summary>Reads four bytes and returns an int.</summary>
		/// <seealso cref="IndexOutput.WriteInt(int)">
		/// </seealso>
		public virtual int ReadInt(IState state)
		{
			return ((ReadByte(state) & 0xFF) << 24) | ((ReadByte(state) & 0xFF) << 16) | ((ReadByte(state) & 0xFF) << 8) | (ReadByte(state) & 0xFF);
		}
		
		/// <summary>Reads an int stored in variable-length format.  Reads between one and
		/// five bytes.  Smaller values take fewer bytes.  Negative numbers are not
		/// supported.
		/// </summary>
		/// <seealso cref="IndexOutput.WriteVInt(int)">
		/// </seealso>
		public virtual int ReadVInt(IState state)
		{
			byte b = ReadByte(state);
			int i = b & 0x7F;
			for (int shift = 7; (b & 0x80) != 0; shift += 7)
			{
				b = ReadByte(state);
				i |= (b & 0x7F) << shift;
			}
			return i;
		}
		
		/// <summary>Reads eight bytes and returns a long.</summary>
		/// <seealso cref="IndexOutput.WriteLong(long)">
		/// </seealso>
		public virtual long ReadLong(IState state)
		{
			return (((long) ReadInt(state)) << 32) | (ReadInt(state) & 0xFFFFFFFFL);
		}
		
		/// <summary>Reads a long stored in variable-length format.  Reads between one and
		/// nine bytes.  Smaller values take fewer bytes.  Negative numbers are not
		/// supported. 
		/// </summary>
		public virtual long ReadVLong(IState state)
		{
			byte b = ReadByte(state);
			long i = b & 0x7F;
			for (int shift = 7; (b & 0x80) != 0; shift += 7)
			{
				b = ReadByte(state);
				i |= (b & 0x7FL) << shift;
			}
			return i;
		}
		
		/// <summary>Call this if readString should read characters stored
		/// in the old modified UTF8 format (length in java chars
		/// and java's modified UTF8 encoding).  This is used for
		/// indices written pre-2.4 See LUCENE-510 for details. 
		/// </summary>
		public virtual void  SetModifiedUTF8StringsMode()
		{
			preUTF8Strings = true;
		}
		
		/// <summary>Reads a string.</summary>
		/// <seealso cref="IndexOutput.WriteString(String)">
		/// </seealso>
		public virtual System.String ReadString(IState state)
		{
			if (preUTF8Strings)
				return ReadModifiedUTF8String(state);
			int length = ReadVInt(state);
            byte[] bytes = new byte[length];
			ReadBytes(bytes, 0, length, state);
            return System.Text.Encoding.UTF8.GetString(bytes, 0, length);
		}
		
		private System.String ReadModifiedUTF8String(IState state)
		{
			int length = ReadVInt(state);
            char[] chars = new char[length];
			ReadChars(chars, 0, length, state);
			return new System.String(chars, 0, length);
		}
		
		/// <summary>Reads Lucene's old "modified UTF-8" encoded
		/// characters into an array.
		/// </summary>
		/// <param name="buffer">the array to read characters into
		/// </param>
		/// <param name="start">the offset in the array to start storing characters
		/// </param>
		/// <param name="length">the number of characters to read
		/// </param>
		/// <seealso cref="IndexOutput.WriteChars(String,int,int)">
		/// </seealso>
		/// <deprecated> -- please use readString or readBytes
		/// instead, and construct the string
		/// from those utf8 bytes
		/// </deprecated>
        [Obsolete("-- please use ReadString or ReadBytes instead, and construct the string from those utf8 bytes")]
		public virtual void  ReadChars(char[] buffer, int start, int length, IState state)
		{
			int end = start + length;
			for (int i = start; i < end; i++)
			{
				byte b = ReadByte(state);
				if ((b & 0x80) == 0)
					buffer[i] = (char) (b & 0x7F);
				else if ((b & 0xE0) != 0xE0)
				{
					buffer[i] = (char) (((b & 0x1F) << 6) | (ReadByte(state) & 0x3F));
				}
				else
					buffer[i] = (char) (((b & 0x0F) << 12) | ((ReadByte(state) & 0x3F) << 6) | (ReadByte(state) & 0x3F));
			}
		}
		
		/// <summary> Expert
		/// 
		/// Similar to <see cref="ReadChars(char[], int, int)" /> but does not do any conversion operations on the bytes it is reading in.  It still
		/// has to invoke <see cref="ReadByte()" /> just as <see cref="ReadChars(char[], int, int)" /> does, but it does not need a buffer to store anything
		/// and it does not have to do any of the bitwise operations, since we don't actually care what is in the byte except to determine
		/// how many more bytes to read
		/// </summary>
		/// <param name="length">The number of chars to read
		/// </param>
		/// <deprecated> this method operates on old "modified utf8" encoded
		/// strings
		/// </deprecated>
        [Obsolete("this method operates on old \"modified utf8\" encoded strings")]
		public virtual void  SkipChars(int length, IState state)
		{
			for (int i = 0; i < length; i++)
			{
				byte b = ReadByte(state);
				if ((b & 0x80) == 0)
				{
					//do nothing, we only need one byte
				}
				else if ((b & 0xE0) != 0xE0)
				{
					ReadByte(state); //read an additional byte
				}
				else
				{
					//read two additional bytes.
					ReadByte(state);
					ReadByte(state);
				}
			}
		}
		
		[Obsolete("Use Dispose() instead.")]
		public void Close()
		{
		    Dispose();
		}

        /// <summary>Closes the stream to futher operations. </summary>
	    public void Dispose()
        {
            Dispose(true);
        }

	    protected abstract void Dispose(bool disposing);

	    /// <summary>Returns the current position in this file, where the next read will
	    /// occur.
	    /// </summary>
	    /// <seealso cref="Seek(long)">
	    /// </seealso>
	    public abstract long FilePointer(IState state);

	    /// <summary>Sets current position in this file, where the next read will occur.</summary>
		/// <seealso cref="FilePointer">
		/// </seealso>
		public abstract void  Seek(long pos, IState state);
		
		/// <summary>The number of bytes in the file. </summary>
		public abstract long Length(IState state);
		
		/// <summary>Returns a clone of this stream.
		/// 
		/// <p/>Clones of a stream access the same data, and are positioned at the same
		/// point as the stream they were cloned from.
		/// 
		/// <p/>Expert: Subclasses must ensure that clones may be positioned at
		/// different points in the input from each other and from the stream they
		/// were cloned from.
		/// </summary>
		public virtual System.Object Clone(IState state)
		{
			IndexInput clone = null;
			try
			{
				clone = (IndexInput) base.MemberwiseClone();
			}
			catch (System.Exception)
			{
			}
			
			return clone;
		}
		
		// returns Map<String, String>
		public virtual IDictionary<string,string> ReadStringStringMap(IState state)
		{
            var map = new HashMap<string, string>();
			int count = ReadInt(state);
			for (int i = 0; i < count; i++)
			{
				System.String key = ReadString(state);
				System.String val = ReadString(state);
				map[key] = val;
			}
			
			return map;
		}

	    /*public abstract void Dispose();*/
	}
}