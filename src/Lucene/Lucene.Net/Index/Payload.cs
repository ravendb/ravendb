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

using TokenStream = Lucene.Net.Analysis.TokenStream;
using ArrayUtil = Lucene.Net.Util.ArrayUtil;

namespace Lucene.Net.Index
{

    /// <summary>  A Payload is metadata that can be stored together with each occurrence 
    /// of a term. This metadata is stored inline in the posting list of the
    /// specific term.  
    /// <p/>
    /// To store payloads in the index a <see cref="TokenStream"/> has to be used that
    /// produces payload data.
    /// <p/>
    /// Use <see cref="TermPositions.PayloadLength"/> and <see cref="TermPositions.GetPayload(byte[], int)"/>
    /// to retrieve the payloads from the index.<br/>
    /// 
    /// </summary>
    [Serializable]
    public class Payload : System.ICloneable
	{
		/// <summary>the byte array containing the payload data </summary>
		protected internal byte[] data;
		
		/// <summary>the offset within the byte array </summary>
		protected internal int internalOffset;
		
		/// <summary>the length of the payload data </summary>
		protected internal int internalLength;
		
		/// <summary>Creates an empty payload and does not allocate a byte array. </summary>
		public Payload()
		{
			// nothing to do
		}
		
		/// <summary> Creates a new payload with the the given array as data.
		/// A reference to the passed-in array is held, i. e. no 
		/// copy is made.
		/// 
		/// </summary>
		/// <param name="data">the data of this payload
		/// </param>
		public Payload(byte[] data):this(data, 0, data.Length)
		{
		}
		
		/// <summary> Creates a new payload with the the given array as data. 
		/// A reference to the passed-in array is held, i. e. no 
		/// copy is made.
		/// 
		/// </summary>
		/// <param name="data">the data of this payload
		/// </param>
		/// <param name="offset">the offset in the data byte array
		/// </param>
		/// <param name="length">the length of the data
		/// </param>
		public Payload(byte[] data, int offset, int length)
		{
			if (offset < 0 || offset + length > data.Length)
			{
				throw new System.ArgumentException();
			}
			this.data = data;
			this.internalOffset = offset;
			this.internalLength = length;
		}

	    /// <summary> Sets this payloads data. 
		/// A reference to the passed-in array is held, i. e. no 
		/// copy is made.
		/// </summary>
		public virtual void  SetData(byte[] value, int offset, int length)
		{
			this.data = value;
			this.internalOffset = offset;
			this.internalLength = length;
		}

	    /// <summary> Gets or sets a reference to the underlying byte array
	    /// that holds this payloads data.  Data is not copied.
	    /// </summary>
	    public virtual void SetData(byte[] value)
	    {
	        SetData(value, 0, value.Length);
	    }

	    /// <summary> Gets or sets a reference to the underlying byte array
	    /// that holds this payloads data.  Data is not copied.
	    /// </summary>
	    public virtual byte[] GetData()
	    {
	        return this.data;
	    }

	    /// <summary> Returns the offset in the underlying byte array </summary>
	    public virtual int Offset
	    {
	        get { return this.internalOffset; }
	    }

	    /// <summary> Returns the length of the payload data. </summary>
	    public virtual int Length
	    {
	        get { return this.internalLength; }
	    }

	    /// <summary> Returns the byte at the given index.</summary>
		public virtual byte ByteAt(int index)
		{
			if (0 <= index && index < this.internalLength)
			{
				return this.data[this.internalOffset + index];
			}
			throw new System. IndexOutOfRangeException("Index of bound " + index);
		}
		
		/// <summary> Allocates a new byte array, copies the payload data into it and returns it. </summary>
		public virtual byte[] ToByteArray()
		{
			byte[] retArray = new byte[this.internalLength];
			Array.Copy(this.data, this.internalOffset, retArray, 0, this.internalLength);
			return retArray;
		}
		
		/// <summary> Copies the payload data to a byte array.
		/// 
		/// </summary>
		/// <param name="target">the target byte array
		/// </param>
		/// <param name="targetOffset">the offset in the target byte array
		/// </param>
		public virtual void  CopyTo(byte[] target, int targetOffset)
		{
			if (this.internalLength > target.Length + targetOffset)
			{
				throw new System.IndexOutOfRangeException();
			}
			Array.Copy(this.data, this.internalOffset, target, targetOffset, this.internalLength);
		}
		
		/// <summary> Clones this payload by creating a copy of the underlying
		/// byte array.
		/// </summary>
		public virtual System.Object Clone()
		{
			try
			{
				// Start with a shallow copy of data
				Payload clone = (Payload) base.MemberwiseClone();
				// Only copy the part of data that belongs to this Payload
				if (internalOffset == 0 && internalLength == data.Length)
				{
					// It is the whole thing, so just clone it.
					clone.data = new byte[data.Length];
					data.CopyTo(clone.data, 0);
				}
				else
				{
					// Just get the part
					clone.data = this.ToByteArray();
					clone.internalOffset = 0;
				}
				return clone;
			}
			catch (System.Exception e)
			{
				throw new System.SystemException(e.Message, e); // shouldn't happen
			}
		}
		
		public  override bool Equals(System.Object obj)
		{
			if (obj == this)
				return true;
			if (obj is Payload)
			{
				Payload other = (Payload) obj;
				if (internalLength == other.internalLength)
				{
					for (int i = 0; i < internalLength; i++)
						if (data[internalOffset + i] != other.data[other.internalOffset + i])
							return false;
					return true;
				}
				else
					return false;
			}
			else
				return false;
		}
		
		public override int GetHashCode()
		{
			return ArrayUtil.HashCode(data, internalOffset, internalOffset + internalLength);
		}
	}
}