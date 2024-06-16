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
	
	/// <summary>A straightforward implementation of <see cref="FSDirectory" />
	/// using java.io.RandomAccessFile.  However, this class has
	/// poor concurrent performance (multiple threads will
	/// bottleneck) as it synchronizes when multiple threads
	/// read from the same file.  It's usually better to use
	/// <see cref="NIOFSDirectory" /> or <see cref="MMapDirectory" /> instead. 
	/// </summary>
	public class SimpleFSDirectory : FSDirectory
	{
        /// <summary>Create a new SimpleFSDirectory for the named location.
        /// 
        /// </summary>
        /// <param name="path">the path of the directory
        /// </param>
        /// <param name="lockFactory">the lock factory to use, or null for the default.
        /// </param>
        /// <throws>  IOException </throws>
        public SimpleFSDirectory(System.IO.DirectoryInfo path, LockFactory lockFactory)
            : base(path, lockFactory)
        {
        }

	    /// <summary>Create a new SimpleFSDirectory for the named location and the default lock factory.
        /// 
        /// </summary>
        /// <param name="path">the path of the directory
        /// </param>
        /// <throws>  IOException </throws>
        public SimpleFSDirectory(System.IO.DirectoryInfo path) : base(path, null)
	    {
	    }

	    /// <summary>Creates an IndexOutput for the file with the given name. </summary>
		public override IndexOutput CreateOutput(System.String name, IState state)
		{
			InitOutput(name);
			return new SimpleFSIndexOutput(new System.IO.FileInfo(System.IO.Path.Combine(internalDirectory.FullName, name)));
		}
		
		/// <summary>Creates an IndexInput for the file with the given name. </summary>
		public override IndexInput OpenInput(System.String name, int bufferSize, IState state)
		{
			EnsureOpen();

            Exception e = null;
            for (var i = 0; i < 10; i++)
            {
                try
                {
                    return new SimpleFSIndexInput(new System.IO.FileInfo(
                        System.IO.Path.Combine(internalDirectory.FullName, name)), bufferSize, ReadChunkSize);
                }
                catch (System.UnauthorizedAccessException ex)
                {
                    e = ex;
                    System.Threading.Thread.Sleep(1);
                }
            }

		    throw e;
		}
		
		protected internal class SimpleFSIndexInput : BufferedIndexInput
		{
			// TODO: This is a bad way to handle memory and disposing
			protected internal class Descriptor : System.IO.BinaryReader
			{
				// remember if the file is open, so that we don't try to close it
				// more than once
				protected internal volatile bool isOpen;
				internal long position;
				internal long length;

			    private bool isDisposed;
				
				public Descriptor(/*FSIndexInput enclosingInstance,*/ System.IO.FileInfo file, System.IO.FileAccess mode) 
					: base(new System.IO.FileStream(file.FullName, System.IO.FileMode.Open, mode, System.IO.FileShare.ReadWrite))
				{
					isOpen = true;
					length = file.Length;
				}

                protected override void Dispose(bool disposing)
                {
                    if (isDisposed) return;

                    if (disposing)
                    {
                        if (isOpen)
                        {
                            isOpen = false;
                        }
                    }

                    isDisposed = true;
                    base.Dispose(disposing);
                }
			
				~Descriptor()
				{
					try
					{
						Dispose(false);
					}
					finally
					{
					}
				}
			}
			
			protected internal Descriptor file;
			internal bool isClone;
		    private bool isDisposed;
			//  LUCENE-1566 - maximum read length on a 32bit JVM to prevent incorrect OOM 
			protected internal int chunkSize;

            public SimpleFSIndexInput(System.IO.FileInfo path, int bufferSize, int chunkSize)
                : base(bufferSize)
            {
                file = new Descriptor(path, System.IO.FileAccess.Read);
                this.chunkSize = chunkSize;
            }

		    /// <summary>IndexInput methods </summary>
			public override void  ReadInternal(byte[] b, int offset, int len, IState state)
			{
				lock (file)
				{
					long position = FilePointer(state);
					if (position != file.position)
					{
						file.BaseStream.Seek(position, System.IO.SeekOrigin.Begin);
						file.position = position;
					}
					int total = 0;
					
					try
					{
						do 
						{
							int readLength;
							if (total + chunkSize > len)
							{
								readLength = len - total;
							}
							else
							{
								// LUCENE-1566 - work around JVM Bug by breaking very large reads into chunks
								readLength = chunkSize;
							}
							int i = file.Read(b, offset + total, readLength);
							if (i == - 1)
							{
								throw new System.IO.IOException("read past EOF");
							}
							file.position += i;
							total += i;
						}
						while (total < len);
					}
					catch (System.OutOfMemoryException e)
					{
						// propagate OOM up and add a hint for 32bit VM Users hitting the bug
						// with a large chunk size in the fast path.
						System.OutOfMemoryException outOfMemoryError = new System.OutOfMemoryException("OutOfMemoryError likely caused by the Sun VM Bug described in " + "https://issues.apache.org/jira/browse/LUCENE-1566; try calling FSDirectory.setReadChunkSize " + "with a a value smaller than the current chunks size (" + chunkSize + ")", e);
						throw outOfMemoryError;
					}
				}
			}

            protected override void Dispose(bool disposing)
            {
                if (isDisposed) return;
                if (disposing)
                {
                    // only close the file if this is not a clone
                    if (!isClone && file != null)
                    {
                        file.Close();
                        file = null;
                    }
                }

                isDisposed = true;
            }

		    public override void  SeekInternal(long position, IState state)
			{
			}
			
			public override long Length(IState state)
			{
				return file.length;
			}
			
			public override System.Object Clone(IState state)
			{
				SimpleFSIndexInput clone = (SimpleFSIndexInput) base.Clone(state);
				clone.isClone = true;
				return clone;
			}
			
			/// <summary>Method used for testing. Returns true if the underlying
			/// file descriptor is valid.
			/// </summary>
			public /*internal*/ virtual bool IsFDValid()
			{
				return file.BaseStream != null;
			}

            public bool isClone_ForNUnit
            {
                get { return isClone; }
            }
		}
		
		/*protected internal*/ public class SimpleFSIndexOutput:BufferedIndexOutput
		{
			internal System.IO.FileStream file = null;
			
			// remember if the file is open, so that we don't try to close it
			// more than once
			private volatile bool isOpen;

		    public SimpleFSIndexOutput(System.IO.FileInfo path)
			{
				file = new System.IO.FileStream(path.FullName, System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.ReadWrite);
				isOpen = true;
			}
			
			/// <summary>output methods: </summary>
			public override void  FlushBuffer(byte[] b, int offset, int size)
			{
				file.Write(b, offset, size);
                // {{dougsale-2.4.0}}
                // FSIndexOutput.Flush
                // When writing frequently with small amounts of data, the data isn't flushed to disk.
                // Thus, attempting to read the data soon after this method is invoked leads to
                // BufferedIndexInput.Refill() throwing an IOException for reading past EOF.
                // Test\Index\TestDoc.cs demonstrates such a situation.
                // Forcing a flush here prevents said issue.
                // {{DIGY 2.9.0}}
                // This code is not available in Lucene.Java 2.9.X.
                // Can there be a indexing-performance problem?
                file.Flush();
			}

            protected override void Dispose(bool disposing)
            {
                // only close the file if it has not been closed yet
                if (isOpen)
                {
                    bool success = false;
                    try
                    {
                        base.Dispose(disposing);
                        success = true;
                    }
                    finally
                    {
                        isOpen = false;
                        if (!success)
                        {
                            try
                            {
                                file.Dispose();
                            }
                            catch (System.Exception)
                            {
                                // Suppress so we don't mask original exception
                            }
                        }
                        else
                            file.Dispose();
                    }
                }
            }
			
			/// <summary>Random-access methods </summary>
			public override void  Seek(long pos)
			{
				base.Seek(pos);
				file.Seek(pos, System.IO.SeekOrigin.Begin);
			}

		    public override long Length
		    {
		        get { return file.Length; }
		    }

		    public override void  SetLength(long length)
			{
				file.SetLength(length);
			}
		}
	}
}