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
using System.Collections.Concurrent;
using System.Runtime.Serialization;
using Lucene.Net.Index;
using IndexFileNameFilter = Lucene.Net.Index.IndexFileNameFilter;

namespace Lucene.Net.Store
{

    /// <summary>A Directory is a flat list of files.  Files may be written once, when they
    /// are created.  Once a file is created it may only be opened for read, or
    /// deleted.  Random access is permitted both when reading and writing.
    /// 
    /// <p/> Java's i/o APIs not used directly, but rather all i/o is
    /// through this API.  This permits things such as: <list>
    /// <item> implementation of RAM-based indices;</item>
    /// <item> implementation indices stored in a database, via JDBC;</item>
    /// <item> implementation of an index as a single file;</item>
    /// </list>
    /// 
    /// Directory locking is implemented by an instance of <see cref="LockFactory" />
    ///, and can be changed for each Directory
    /// instance using <see cref="SetLockFactory" />.
    /// 
    /// </summary>

        [Serializable]
    public abstract class Directory : System.IDisposable
	{
        [NonSerialized]
        private ConcurrentDictionary<string, Lazy<ArrayHolder>> _termsIndexCachePerSegment = new ConcurrentDictionary<string, Lazy<ArrayHolder>>();

		protected internal volatile bool isOpen = true;
		
		/// <summary>Holds the LockFactory instance (implements locking for
		/// this Directory instance). 
		/// </summary>
		[NonSerialized]
		protected internal LockFactory interalLockFactory;

	    /// <summary>Returns an array of strings, one for each file in the directory.</summary>
	    /// <exception cref="System.IO.IOException"></exception>
	    public abstract System.String[] ListAll(IState state);
		
		/// <summary>Returns true iff a file with the given name exists. </summary>
		public abstract bool FileExists(System.String name, IState state);
		
		/// <summary>Returns the time the named file was last modified. </summary>
		public abstract long FileModified(System.String name, IState state);
		
		/// <summary>Set the modified time of an existing file to now. </summary>
		public abstract void TouchFile(System.String name, IState state);
		
		/// <summary>Removes an existing file in the directory. </summary>
		public abstract void  DeleteFile(System.String name, IState state);
		
		/// <summary>Returns the length of a file in the directory. </summary>
		public abstract long FileLength(System.String name, IState state);
		
		
		/// <summary>Creates a new, empty file in the directory with the given name.
		/// Returns a stream writing this file. 
		/// </summary>
		public abstract IndexOutput CreateOutput(System.String name, IState state);
		
		/// <summary>Ensure that any writes to this file are moved to
		/// stable storage.  Lucene uses this to properly commit
		/// changes to the index, to prevent a machine/OS crash
		/// from corrupting the index. 
		/// </summary>
		public virtual void  Sync(System.String name)
		{
		}
		
		/// <summary>Returns a stream reading an existing file. </summary>
		public abstract IndexInput OpenInput(System.String name, IState state);
		
		/// <summary>Returns a stream reading an existing file, with the
		/// specified read buffer size.  The particular Directory
		/// implementation may ignore the buffer size.  Currently
		/// the only Directory implementations that respect this
		/// parameter are <see cref="FSDirectory" /> and <see cref="Lucene.Net.Index.CompoundFileReader" />
		///.
		/// </summary>
		public virtual IndexInput OpenInput(System.String name, int bufferSize, IState state)
		{
			return OpenInput(name, state);
		}

        public virtual ArrayHolder GetCache(string name, FieldInfos fieldInfos, int readBufferSize, int indexDivisor, IState state)
        {
            return GetCache(this, name, fieldInfos, readBufferSize, indexDivisor, state);
        }

        public ArrayHolder GetCache(Directory directory, string name, FieldInfos fieldInfos, int readBufferSize, int indexDivisor, IState state)
        {
            var lazyArrayHolder = _termsIndexCachePerSegment.GetOrAdd(name,
                new Lazy<ArrayHolder>(() => ArrayHolder.GenerateArrayHolder(directory, name, fieldInfos, readBufferSize, indexDivisor, state)));

            return lazyArrayHolder.Value;
        }

        public virtual void RemoveFromTermsIndexCache(string name)
        {
            _termsIndexCachePerSegment.TryRemove(name, out _);
            // intentionally not disposing the cache here since it might be in use by a TemInfosReader instance.
            // we'll let the finalizer clean it when it isn't in use anymore.
        }

		/// <summary>Construct a <see cref="Lock" />.</summary>
		/// <param name="name">the name of the lock file
		/// </param>
		public virtual Lock MakeLock(System.String name)
		{
			return interalLockFactory.MakeLock(name);
		}
		/// <summary> Attempt to clear (forcefully unlock and remove) the
		/// specified lock.  Only call this at a time when you are
		/// certain this lock is no longer in use.
		/// </summary>
		/// <param name="name">name of the lock to be cleared.
		/// </param>
		public virtual void  ClearLock(System.String name)
		{
			if (interalLockFactory != null)
			{
				interalLockFactory.ClearLock(name);
			}
		}
		
		[Obsolete("Use Dispose() instead")]
		public void Close()
		{
		    Dispose();
		}

        /// <summary>Closes the store. </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            foreach ((_, Lazy<ArrayHolder> cacheLazy) in _termsIndexCachePerSegment)
            {
                cacheLazy.Value.Dispose();
            }
        }

	    /// <summary> Set the LockFactory that this Directory instance should
		/// use for its locking implementation.  Each * instance of
		/// LockFactory should only be used for one directory (ie,
		/// do not share a single instance across multiple
		/// Directories).
		/// 
		/// </summary>
		/// <param name="lockFactory">instance of <see cref="LockFactory" />.
		/// </param>
		public virtual void  SetLockFactory(LockFactory lockFactory)
		{
		    System.Diagnostics.Debug.Assert(lockFactory != null);
			this.interalLockFactory = lockFactory;
			lockFactory.LockPrefix = this.GetLockId();
		}

	    /// <summary> Get the LockFactory that this Directory instance is
	    /// using for its locking implementation.  Note that this
	    /// may be null for Directory implementations that provide
	    /// their own locking implementation.
	    /// </summary>
	    public virtual LockFactory LockFactory
	    {
	        get { return this.interalLockFactory; }
	    }

	    /// <summary> Return a string identifier that uniquely differentiates
        /// this Directory instance from other Directory instances.
        /// This ID should be the same if two Directory instances
        /// (even in different JVMs and/or on different machines)
        /// are considered "the same index".  This is how locking
        /// "scopes" to the right index.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public virtual string GetLockId()
        {
            return ToString();
        }

	    public override string ToString()
        {
            return base.ToString() + " lockFactory=" + LockFactory;
        }
		
		/// <summary> Copy contents of a directory src to a directory dest.
		/// If a file in src already exists in dest then the
		/// one in dest will be blindly overwritten.
		/// 
		/// <p/><b>NOTE:</b> the source directory cannot change
		/// while this method is running.  Otherwise the results
		/// are undefined and you could easily hit a
		/// FileNotFoundException.
		/// 
		/// <p/><b>NOTE:</b> this method only copies files that look
		/// like index files (ie, have extensions matching the
		/// known extensions of index files).
		/// 
		/// </summary>
		/// <param name="src">source directory
		/// </param>
		/// <param name="dest">destination directory
		/// </param>
		/// <param name="closeDirSrc">if <c>true</c>, call <see cref="Close()" /> method on source directory
		/// </param>
		/// <throws>  IOException </throws>
		public static void  Copy(Directory src, Directory dest, bool closeDirSrc, IState state)
		{
			System.String[] files = src.ListAll(state);
			
			IndexFileNameFilter filter = IndexFileNameFilter.Filter;
			
			byte[] buf = new byte[BufferedIndexOutput.BUFFER_SIZE];
			for (int i = 0; i < files.Length; i++)
			{
				
				if (!filter.Accept(null, files[i]))
					continue;
				
				IndexOutput os = null;
				IndexInput is_Renamed = null;
				try
				{
					// create file in dest directory
					os = dest.CreateOutput(files[i], state);
					// read current file
					is_Renamed = src.OpenInput(files[i], state);
					// and copy to dest directory
					long len = is_Renamed.Length(state);
					long readCount = 0;
					while (readCount < len)
					{
						int toRead = readCount + BufferedIndexOutput.BUFFER_SIZE > len?(int) (len - readCount):BufferedIndexOutput.BUFFER_SIZE;
						is_Renamed.ReadBytes(buf, 0, toRead, state);
						os.WriteBytes(buf, toRead);
						readCount += toRead;
					}
				}
				finally
				{
					// graceful cleanup
					try
					{
						if (os != null)
							os.Close();
					}
					finally
					{
						if (is_Renamed != null)
							is_Renamed.Close();
					}
				}
			}
			if (closeDirSrc)
				src.Close();
		}
		
		/// <throws>  AlreadyClosedException if this Directory is closed </throws>
		public /*protected internal*/ void  EnsureOpen()
		{
			if (!isOpen)
				throw new AlreadyClosedException("this Directory is closed");
		}

        public bool isOpen_ForNUnit
        {
            get { return isOpen; }
        }

        [NonSerialized]
        internal ArrayPool<byte> ByteBlockPool = ArrayPool<byte>.Create();

		[OnDeserialized]
        public void OnDeserialized(StreamingContext _)
        {
            _termsIndexCachePerSegment = new ConcurrentDictionary<string, Lazy<ArrayHolder>>();
        }
    }
}