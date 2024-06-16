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
using Lucene.Net.Support;

namespace Lucene.Net.Store
{
    using System.Threading;

    /// <summary> A memory-resident <see cref="Directory"/> implementation.  Locking
    /// implementation is by default the <see cref="SingleInstanceLockFactory"/>
    /// but can be changed with <see cref="Directory.SetLockFactory"/>.
    /// </summary>

        [Serializable]
    public class RAMDirectory:Directory
	{
		
		private const long serialVersionUID = 1L;

        internal protected HashMap<string, RAMFile> fileMap = new HashMap<string, RAMFile>();
		internal protected long internalSizeInBytes = 0;
		
		// *****
		// Lock acquisition sequence:  RAMDirectory, then RAMFile
		// *****

        /// <summary>Constructs an empty <see cref="Directory"/>. </summary>
		public RAMDirectory()
		{
			SetLockFactory(new SingleInstanceLockFactory());
		}
		
		/// <summary> Creates a new <c>RAMDirectory</c> instance from a different
		/// <c>Directory</c> implementation.  This can be used to load
		/// a disk-based index into memory.
		/// <p/>
		/// This should be used only with indices that can fit into memory.
		/// <p/>
		/// Note that the resulting <c>RAMDirectory</c> instance is fully
		/// independent from the original <c>Directory</c> (it is a
		/// complete copy).  Any subsequent changes to the
		/// original <c>Directory</c> will not be visible in the
		/// <c>RAMDirectory</c> instance.
		/// 
		/// </summary>
		/// <param name="dir">a <c>Directory</c> value
		/// </param>
		/// <exception cref="System.IO.IOException">if an error occurs
		/// </exception>
		public RAMDirectory(Directory dir, IState state) :this(dir, false, state)
		{
		}
		
		private RAMDirectory(Directory dir, bool closeDir, IState state) :this()
		{
			Directory.Copy(dir, this, closeDir, state);
		}

         //https://issues.apache.org/jira/browse/LUCENENET-174
        [System.Runtime.Serialization.OnDeserialized]
        void OnDeserialized(System.Runtime.Serialization.StreamingContext context)
        {
            if (interalLockFactory == null)
            {
                SetLockFactory(new SingleInstanceLockFactory());
            }

            if (ByteBlockPool == null)
                ByteBlockPool = ArrayPool<byte>.Create();
        }
		
		public override System.String[] ListAll(IState state)
		{
			lock (this)
			{
				EnsureOpen();
                // TODO: may have better performance if our HashMap implmented KeySet() instead of generating one via HashSet
                System.Collections.Generic.ISet<string> fileNames = Support.Compatibility.SetFactory.CreateHashSet(fileMap.Keys);
				System.String[] result = new System.String[fileNames.Count];
				int i = 0;
				foreach(string filename in fileNames)
				{
                    result[i++] = filename;
				}
				return result;
			}
		}
		
		/// <summary>Returns true iff the named file exists in this directory. </summary>
		public override bool FileExists(System.String name, IState state)
		{
			EnsureOpen();
			RAMFile file;
			lock (this)
			{
				file = fileMap[name];
			}
			return file != null;
		}
		
		/// <summary>Returns the time the named file was last modified.</summary>
		/// <throws>  IOException if the file does not exist </throws>
		public override long FileModified(System.String name, IState state)
		{
			EnsureOpen();
			RAMFile file;
			lock (this)
			{
				file = fileMap[name];
			}
			if (file == null)
				throw new System.IO.FileNotFoundException(name);
            
            // RAMOutputStream.Flush() was changed to use DateTime.UtcNow.
            // Convert it back to local time before returning (previous behavior)
		    return new DateTime(file.LastModified*TimeSpan.TicksPerMillisecond, DateTimeKind.Utc).ToLocalTime().Ticks/
		           TimeSpan.TicksPerMillisecond;
		}
		
		/// <summary>Set the modified time of an existing file to now.</summary>
		/// <throws>  IOException if the file does not exist </throws>
		public override void  TouchFile(System.String name, IState state)
		{
			EnsureOpen();
			RAMFile file;
			lock (this)
			{
				file = fileMap[name];
			}
			if (file == null)
				throw new System.IO.FileNotFoundException(name);
			
			long ts2, ts1 = System.DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
			do 
			{
				try
				{
					System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 0 + 100 * 1));
				}
				catch (System.Threading.ThreadInterruptedException ie)
				{
                    // In 3.0 we will change this to throw
                    // InterruptedException instead
                    ThreadClass.Current().Interrupt();
                    throw new System.SystemException(ie.Message, ie);
				}
                ts2 = System.DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
			}
			while (ts1 == ts2);
			
			file.LastModified = ts2;
		}
		
		/// <summary>Returns the length in bytes of a file in the directory.</summary>
		/// <throws>  IOException if the file does not exist </throws>
		public override long FileLength(System.String name, IState state)
		{
			EnsureOpen();
			RAMFile file;
			lock (this)
			{
				file = fileMap[name];
			}
			if (file == null)
				throw new System.IO.FileNotFoundException(name);
			return file.Length;
		}
		
		/// <summary>Return total size in bytes of all files in this
		/// directory.  This is currently quantized to
		/// RAMOutputStream.BUFFER_SIZE. 
		/// </summary>
		public long SizeInBytes()
		{
			lock (this)
			{
				EnsureOpen();
				return internalSizeInBytes;
			}
		}
		
		/// <summary>Removes an existing file in the directory.</summary>
		/// <throws>  IOException if the file does not exist </throws>
		public override void  DeleteFile(System.String name, IState state)
		{
			lock (this)
			{
				EnsureOpen();
				RAMFile file = fileMap[name];
				if (file != null)
				{
					fileMap.Remove(name);
					file.directory = null;
					internalSizeInBytes -= file.sizeInBytes; 
				}
				else
					throw new System.IO.FileNotFoundException(name);
			}
		}
		
		/// <summary>Creates a new, empty file in the directory with the given name. Returns a stream writing this file. </summary>
		public override IndexOutput CreateOutput(System.String name, IState state)
		{
			EnsureOpen();
			RAMFile file = new RAMFile(this);
			lock (this)
			{
				RAMFile existing = fileMap[name];
				if (existing != null)
				{
					internalSizeInBytes -= existing.sizeInBytes;
					existing.directory = null;
				}
				fileMap[name] = file;
			}
			return new RAMOutputStream(file);
		}
		
		/// <summary>Returns a stream reading an existing file. </summary>
		public override IndexInput OpenInput(System.String name, IState state)
		{
			EnsureOpen();
			RAMFile file;
			lock (this)
			{
				file = fileMap[name];
			}
			if (file == null)
				throw new System.IO.FileNotFoundException(name);
			return new RAMInputStream(file);
		}

        /// <summary>Closes the store to future operations, releasing associated memory. </summary>
        protected override void Dispose(bool disposing)
        {
            isOpen = false;
            fileMap = null;
        }

        //public HashMap<string, RAMFile> fileMap_ForNUnit
        //{
        //    get { return fileMap; }
        //}

        //public long sizeInBytes_ForNUnitTest
        //{
        //    get { return sizeInBytes; }
        //    set { sizeInBytes = value; }
        //}
	}
}