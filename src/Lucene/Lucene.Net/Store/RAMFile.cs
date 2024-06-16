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

namespace Lucene.Net.Store
{


        [Serializable]
    public class RAMFile
	{
		
		private const long serialVersionUID = 1L;
		
		protected System.Collections.Generic.List<byte[]> buffers = new System.Collections.Generic.List<byte[]>();
		internal long length;
		internal RAMDirectory directory;
		internal long sizeInBytes; 
		
		// This is publicly modifiable via Directory.touchFile(), so direct access not supported
		private long lastModified = (DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond);
		
		// File used as buffer, in no RAMDirectory
		public /*internal*/ RAMFile()
		{
		}
		
		public /*internal*/ RAMFile(RAMDirectory directory)
		{
			this.directory = directory;
		}
		
		// For non-stream access from thread that might be concurrent with writing

	    internal virtual long Length
	    {
	        get
	        {
	            lock (this)
	            {
	                return length;
	            }
	        }
	        set
	        {
	            lock (this)
	            {
	                this.length = value;
	            }
	        }
	    }

	    // For non-stream access from thread that might be concurrent with writing

	    internal virtual long LastModified
	    {
	        get
	        {
	            lock (this)
	            {
	                return lastModified;
	            }
	        }
	        set
	        {
	            lock (this)
	            {
	                this.lastModified = value;
	            }
	        }
	    }

	    internal byte[] AddBuffer(int size)
		{
            byte[] buffer = NewBuffer(size);
            lock (this)
            {
                buffers.Add(buffer);
                sizeInBytes += size;
            }

            if (directory != null)
            {
                lock (directory) //{{DIGY}} what if directory gets null in the mean time?
                {
                    directory.internalSizeInBytes += size;
                }
            }

            return buffer;
		}
		
		public /*internal*/ byte[] GetBuffer(int index)
		{
			lock (this)
			{
				return buffers[index];
			}
		}
		
		public /*internal*/ int NumBuffers()
		{
			lock (this)
			{
				return buffers.Count;
			}
		}
		
		/// <summary> Expert: allocate a new buffer. 
		/// Subclasses can allocate differently. 
		/// </summary>
		/// <param name="size">size of allocated buffer.
		/// </param>
		/// <returns> allocated buffer.
		/// </returns>
		public /*internal*/ virtual byte[] NewBuffer(int size)
		{
			return new byte[size];
		}


	    public virtual long SizeInBytes
	    {
	        get
	        {
	            lock (this)
	            {
	                return sizeInBytes;
	            }
	        }
	    }
	}
}