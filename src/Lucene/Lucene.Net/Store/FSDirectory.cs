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

// Used only for WRITE_LOCK_NAME in deprecated create=true case:
using System.IO;
using Lucene.Net.Support;
using IndexFileNameFilter = Lucene.Net.Index.IndexFileNameFilter;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Constants = Lucene.Net.Util.Constants;

namespace Lucene.Net.Store
{
	
	/// <summary> <a name="subclasses"/>
	/// Base class for Directory implementations that store index
	/// files in the file system.  There are currently three core
	/// subclasses:
	/// 
	/// <list type="bullet">
	/// 
	/// <item> <see cref="SimpleFSDirectory" /> is a straightforward
	/// implementation using java.io.RandomAccessFile.
	/// However, it has poor concurrent performance
	/// (multiple threads will bottleneck) as it
	/// synchronizes when multiple threads read from the
	/// same file.</item>
	/// 
	/// <item> <see cref="NIOFSDirectory" /> uses java.nio's
	/// FileChannel's positional io when reading to avoid
	/// synchronization when reading from the same file.
	/// Unfortunately, due to a Windows-only <a
	/// href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6265734">Sun
	/// JRE bug</a> this is a poor choice for Windows, but
	/// on all other platforms this is the preferred
	/// choice. Applications using <see cref="System.Threading.Thread.Interrupt()" /> or
    /// <c>Future#cancel(boolean)</c> (on Java 1.5) should use
    /// <see cref="SimpleFSDirectory" /> instead. See <see cref="NIOFSDirectory" /> java doc
    /// for details.
    ///        
    ///        
	/// 
	/// <item> <see cref="MMapDirectory" /> uses memory-mapped IO when
	/// reading. This is a good choice if you have plenty
	/// of virtual memory relative to your index size, eg
	/// if you are running on a 64 bit JRE, or you are
	/// running on a 32 bit JRE but your index sizes are
	/// small enough to fit into the virtual memory space.
	/// Java has currently the limitation of not being able to
	/// unmap files from user code. The files are unmapped, when GC
	/// releases the byte buffers. Due to
	/// <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038">
	/// this bug</a> in Sun's JRE, MMapDirectory's <see cref="IndexInput.Close" />
	/// is unable to close the underlying OS file handle. Only when
	/// GC finally collects the underlying objects, which could be
	/// quite some time later, will the file handle be closed.
	/// This will consume additional transient disk usage: on Windows,
	/// attempts to delete or overwrite the files will result in an
	/// exception; on other platforms, which typically have a &quot;delete on
	/// last close&quot; semantics, while such operations will succeed, the bytes
	/// are still consuming space on disk.  For many applications this
	/// limitation is not a problem (e.g. if you have plenty of disk space,
	/// and you don't rely on overwriting files on Windows) but it's still
	/// an important limitation to be aware of. This class supplies a
	/// (possibly dangerous) workaround mentioned in the bug report,
	/// which may fail on non-Sun JVMs.</item>
    ///       
    /// Applications using <see cref="System.Threading.Thread.Interrupt()" /> or
    /// <c>Future#cancel(boolean)</c> (on Java 1.5) should use
    /// <see cref="SimpleFSDirectory" /> instead. See <see cref="MMapDirectory" />
    /// java doc for details.</item>
	/// </list>
	/// 
	/// Unfortunately, because of system peculiarities, there is
	/// no single overall best implementation.  Therefore, we've
	/// added the <see cref="Open(System.IO.DirectoryInfo)" /> method, to allow Lucene to choose
	/// the best FSDirectory implementation given your
	/// environment, and the known limitations of each
	/// implementation.  For users who have no reason to prefer a
	/// specific implementation, it's best to simply use <see cref="FSDirectory.Open(System.IO.DirectoryInfo)" />
	///.  For all others, you should instantiate the
	/// desired implementation directly.
	/// 
	/// <p/>The locking implementation is by default <see cref="NativeFSLockFactory" />
	///, but can be changed by
	/// passing in a custom <see cref="LockFactory" /> instance.
	/// </summary>
	public abstract class FSDirectory : Directory
	{
		private static System.Security.Cryptography.HashAlgorithm DIGESTER;

        static FSDirectory()
        {
            try
            {
                DIGESTER = Cryptography.HashAlgorithm;
            }
            catch (System.Exception e)
            {
                throw new System.SystemException(e.ToString(), e);
            }
        }
		
		private bool checked_Renamed;
		
		internal void  CreateDir()
		{
			if (!checked_Renamed)
			{
                if (!this.internalDirectory.Exists)
                {
                    try
                    {
                        this.internalDirectory.Create();
                    }
                    catch (Exception)
                    {
                        throw new System.IO.IOException("Cannot create directory: " + internalDirectory);
                    }
                    this.internalDirectory.Refresh(); // need to see the creation
                }
				
				checked_Renamed = true;
			}
		}
		
		/// <summary>Initializes the directory to create a new file with the given name.
		/// This method should be used in <see cref="Lucene.Net.Store.Directory.CreateOutput(string)" />. 
		/// </summary>
		protected internal void  InitOutput(System.String name)
		{
			EnsureOpen();
			CreateDir();
			System.IO.FileInfo file = new System.IO.FileInfo(System.IO.Path.Combine(internalDirectory.FullName, name));
            if (file.Exists) // delete existing, if any
            {
                try
                {
                    file.Delete();
                }
                catch (Exception)
                {
                    throw new System.IO.IOException("Cannot overwrite: " + file);
                }
            }
		}
		
		/// <summary>The underlying filesystem directory </summary>
		protected internal System.IO.DirectoryInfo internalDirectory = null;
		
		/// <summary>Create a new FSDirectory for the named location (ctor for subclasses).</summary>
		/// <param name="path">the path of the directory
		/// </param>
		/// <param name="lockFactory">the lock factory to use, or null for the default
		/// (<see cref="NativeFSLockFactory" />);
		/// </param>
		/// <throws>  IOException </throws>
		protected internal FSDirectory(System.IO.DirectoryInfo path, LockFactory lockFactory)
		{
			// new ctors use always NativeFSLockFactory as default:
			if (lockFactory == null)
			{
				lockFactory = new NativeFSLockFactory();
			}
            // Set up lockFactory with cascaded defaults: if an instance was passed in,
            // use that; else if locks are disabled, use NoLockFactory; else if the
            // system property Lucene.Net.Store.FSDirectoryLockFactoryClass is set,
            // instantiate that; else, use SimpleFSLockFactory:

            internalDirectory = path;

            // due to differences in how Java & .NET refer to files, the checks are a bit different
            if (!internalDirectory.Exists && System.IO.File.Exists(internalDirectory.FullName))
            {
                throw new NoSuchDirectoryException("file '" + internalDirectory.FullName + "' exists but is not a directory");
            }
            SetLockFactory(lockFactory);
            
            // for filesystem based LockFactory, delete the lockPrefix, if the locks are placed
            // in index dir. If no index dir is given, set ourselves
            if (lockFactory is FSLockFactory)
            {
                FSLockFactory lf = (FSLockFactory)lockFactory;
                System.IO.DirectoryInfo dir = lf.LockDir;
                // if the lock factory has no lockDir set, use the this directory as lockDir
                if (dir == null)
                {
                    lf.LockDir = this.internalDirectory;
                    lf.LockPrefix = null;
                }
                else if (dir.FullName.Equals(this.internalDirectory.FullName))
                {
                    lf.LockPrefix = null;
                }
            }
		}

		/// <summary>Creates an FSDirectory instance, trying to pick the
		/// best implementation given the current environment.
		/// The directory returned uses the <see cref="NativeFSLockFactory" />.
		/// 
		/// <p/>Currently this returns <see cref="SimpleFSDirectory" /> as
		/// NIOFSDirectory is currently not supported.
		/// 
		/// <p/><b>NOTE</b>: this method may suddenly change which
		/// implementation is returned from release to release, in
		/// the event that higher performance defaults become
		/// possible; if the precise implementation is important to
		/// your application, please instantiate it directly,
		/// instead. On 64 bit systems, it may also good to
		/// return <see cref="MMapDirectory" />, but this is disabled
		/// because of officially missing unmap support in Java.
		/// For optimal performance you should consider using
		/// this implementation on 64 bit JVMs.
		/// 
		/// <p/>See <a href="#subclasses">above</a> 
		/// </summary>
		public static FSDirectory Open(string path)
		{
			return Open(new DirectoryInfo(path), null);
		}
		
		/// <summary>Creates an FSDirectory instance, trying to pick the
		/// best implementation given the current environment.
		/// The directory returned uses the <see cref="NativeFSLockFactory" />.
		/// 
		/// <p/>Currently this returns <see cref="SimpleFSDirectory" /> as
		/// NIOFSDirectory is currently not supported.
		/// 
		/// <p/><b>NOTE</b>: this method may suddenly change which
		/// implementation is returned from release to release, in
		/// the event that higher performance defaults become
		/// possible; if the precise implementation is important to
		/// your application, please instantiate it directly,
		/// instead. On 64 bit systems, it may also good to
		/// return <see cref="MMapDirectory" />, but this is disabled
		/// because of officially missing unmap support in Java.
		/// For optimal performance you should consider using
		/// this implementation on 64 bit JVMs.
		/// 
		/// <p/>See <a href="#subclasses">above</a> 
		/// </summary>
		public static FSDirectory Open(System.IO.DirectoryInfo path)
		{
			return Open(path, null);
		}

        /// <summary>Just like <see cref="Open(System.IO.DirectoryInfo)" />, but allows you to
		/// also specify a custom <see cref="LockFactory" />. 
		/// </summary>
		public static FSDirectory Open(System.IO.DirectoryInfo path, LockFactory lockFactory)
		{
			/* For testing:
			MMapDirectory dir=new MMapDirectory(path, lockFactory);
			dir.setUseUnmap(true);
			return dir;
			*/
			
			if (Constants.WINDOWS)
			{
				return new SimpleFSDirectory(path, lockFactory);
			}
			else
			{
                //NIOFSDirectory is not implemented in Lucene.Net
				//return new NIOFSDirectory(path, lockFactory);
                return new SimpleFSDirectory(path, lockFactory);
			}
        }
		
        /// <summary>Lists all files (not subdirectories) in the
        /// directory.  This method never returns null (throws
        /// <see cref="System.IO.IOException" /> instead).
        /// 
        /// </summary>
        /// <throws>  NoSuchDirectoryException if the directory </throws>
        /// <summary>   does not exist, or does exist but is not a
        /// directory.
        /// </summary>
        /// <throws>  IOException if list() returns null  </throws>
        public static System.String[] ListAll(System.IO.DirectoryInfo dir)
        {
            if (!dir.Exists)
            {
                throw new NoSuchDirectoryException("directory '" + dir.FullName + "' does not exist");
            }
            else if (System.IO.File.Exists(dir.FullName))
            {
                throw new NoSuchDirectoryException("File '" + dir.FullName + "' does not exist");
            }

            // Exclude subdirs, only the file names, not the paths
            System.IO.FileInfo[] files = dir.GetFiles();
            System.String[] result = new System.String[files.Length];
            for (int i = 0; i < files.Length; i++)
            {
                result[i] = files[i].Name;
            }

            // no reason to return null, if the directory cannot be listed, an exception 
            // will be thrown on the above call to dir.GetFiles()
            // use of LINQ to create the return value array may be a bit more efficient

            return result;
        }
		
		/// <summary>Lists all files (not subdirectories) in the
		/// directory.
		/// </summary>
		/// <seealso cref="ListAll(System.IO.DirectoryInfo)">
		/// </seealso>
		public override System.String[] ListAll(IState state)
		{
			EnsureOpen();
			return ListAll(internalDirectory);
		}
		
		/// <summary>Returns true iff a file with the given name exists. </summary>
		public override bool FileExists(System.String name, IState state)
		{
			EnsureOpen();
			System.IO.FileInfo file = new System.IO.FileInfo(System.IO.Path.Combine(internalDirectory.FullName, name));
            return file.Exists;
		}
		
		/// <summary>Returns the time the named file was last modified. </summary>
		public override long FileModified(System.String name, IState state)
		{
			EnsureOpen();
			System.IO.FileInfo file = new System.IO.FileInfo(System.IO.Path.Combine(internalDirectory.FullName, name));
            return (long)file.LastWriteTime.ToUniversalTime().Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds; //{{LUCENENET-353}}
		}
		
		/// <summary>Returns the time the named file was last modified. </summary>
		public static long FileModified(System.IO.FileInfo directory, System.String name)
		{
			System.IO.FileInfo file = new System.IO.FileInfo(System.IO.Path.Combine(directory.FullName, name));
            return (long)file.LastWriteTime.ToUniversalTime().Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds; //{{LUCENENET-353}}
		}
		
		/// <summary>Set the modified time of an existing file to now. </summary>
		public override void  TouchFile(System.String name, IState state)
		{
			EnsureOpen();
			System.IO.FileInfo file = new System.IO.FileInfo(System.IO.Path.Combine(internalDirectory.FullName, name));
			file.LastWriteTime = System.DateTime.Now;
		}
		
		/// <summary>Returns the length in bytes of a file in the directory. </summary>
		public override long FileLength(System.String name, IState state)
		{
			EnsureOpen();
			System.IO.FileInfo file = new System.IO.FileInfo(System.IO.Path.Combine(internalDirectory.FullName, name));
			return file.Exists ? file.Length : 0;
		}
		
		/// <summary>Removes an existing file in the directory. </summary>
		public override void  DeleteFile(System.String name, IState state)
		{
			EnsureOpen();
			System.IO.FileInfo file = new System.IO.FileInfo(System.IO.Path.Combine(internalDirectory.FullName, name));
            try
            {
                file.Delete();
            }
            catch (Exception)
            {
                throw new System.IO.IOException("Cannot delete " + file);
            }
		}
		
		public override void  Sync(System.String name)
		{
			EnsureOpen();
			System.IO.FileInfo fullFile = new System.IO.FileInfo(System.IO.Path.Combine(internalDirectory.FullName, name));
			bool success = false;
			int retryCount = 0;
			System.IO.IOException exc = null;
			while (!success && retryCount < 5)
			{
				retryCount++;
				System.IO.FileStream file = null;
				try
				{
					try
					{
                        file = new System.IO.FileStream(fullFile.FullName, System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.Write, System.IO.FileShare.ReadWrite);
                        FileSupport.Sync(file);
                        success = true;
					}
					finally
					{
					    if (file != null)
					    {
                            file.Close();
                        }
					}
				}
				catch (System.IO.IOException ioe)
				{
					if (exc == null)
						exc = ioe;
					
                    // Pause 5 msec
					System.Threading.Thread.Sleep(5);
					
				}
			}

			if (!success && exc != null)
			// Throw original exception
				throw exc;
		}
		
		// Inherit javadoc
		public override IndexInput OpenInput(System.String name, IState state)
		{
			EnsureOpen();
			return OpenInput(name, BufferedIndexInput.BUFFER_SIZE, state);
		}
		
		/// <summary> So we can do some byte-to-hexchar conversion below</summary>
		private static readonly char[] HEX_DIGITS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};


	    public override string GetLockId()
	    {
	        EnsureOpen();
	        System.String dirName; // name to be hashed
	        try
	        {
	            dirName = internalDirectory.FullName;
	        }
	        catch (System.IO.IOException e)
	        {
	            throw new System.SystemException(e.ToString(), e);
	        }

	        byte[] digest;
	        lock (DIGESTER)
	        {
	            digest = DIGESTER.ComputeHash(System.Text.Encoding.UTF8.GetBytes(dirName));
	        }
	        System.Text.StringBuilder buf = new System.Text.StringBuilder();
	        buf.Append("lucene-");
	        for (int i = 0; i < digest.Length; i++)
	        {
	            int b = digest[i];
	            buf.Append(HEX_DIGITS[(b >> 4) & 0xf]);
	            buf.Append(HEX_DIGITS[b & 0xf]);
	        }

	        return buf.ToString();
	    }

	    protected override void Dispose(bool disposing)
        {
            lock (this)
            {
                isOpen = false;
            }
        }

        // Java Lucene implements GetFile() which returns a FileInfo.
        // For Lucene.Net, GetDirectory() is more appropriate

	    public virtual DirectoryInfo Directory
	    {
	        get
	        {
	            EnsureOpen();
	            return internalDirectory;
	        }
	    }

	    /// <summary>For debug output. </summary>
		public override System.String ToString()
		{
            return this.GetType().FullName + "@" + internalDirectory + " lockFactory=" + LockFactory;
		}
		
		/// <summary> Default read chunk size.  This is a conditional
		/// default: on 32bit JVMs, it defaults to 100 MB.  On
		/// 64bit JVMs, it's <c>Integer.MAX_VALUE</c>.
		/// </summary>
		/// <seealso cref="ReadChunkSize">
		/// </seealso>
		public static readonly int DEFAULT_READ_CHUNK_SIZE = Constants.JRE_IS_64BIT ? int.MaxValue: 100 * 1024 * 1024;
		
		// LUCENE-1566
		private int chunkSize = DEFAULT_READ_CHUNK_SIZE;

	    /// <summary> The maximum number of bytes to read at once from the
	    /// underlying file during <see cref="IndexInput.ReadBytes(byte[],int,int)" />.
	    /// </summary>
	    /// <seealso cref="ReadChunkSize">
	    /// </seealso>
	    public int ReadChunkSize
	    {
	        get
	        {
	            // LUCENE-1566
	            return chunkSize;
	        }
	        set
	        {
	            // LUCENE-1566
	            if (value <= 0)
	            {
	                throw new System.ArgumentException("chunkSize must be positive");
	            }
	            if (!Constants.JRE_IS_64BIT)
	            {
	                this.chunkSize = value;
	            }
	        }
	    }
	}
}