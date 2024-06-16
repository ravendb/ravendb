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
	
	/// <summary> <p/>Implements <see cref="LockFactory" /> using <see cref="System.IO.FileInfo.Create()" />
	///.<p/>
	/// 
	/// <p/><b>NOTE:</b> the <a target="_top"
	/// href="http://java.sun.com/j2se/1.4.2/docs/api/java/io/File.html#createNewFile()">javadocs
	/// for <c>File.createNewFile</c></a> contain a vague
	/// yet spooky warning about not using the API for file
	/// locking.  This warning was added due to <a target="_top"
	/// href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4676183">this
	/// bug</a>, and in fact the only known problem with using
	/// this API for locking is that the Lucene write lock may
	/// not be released when the JVM exits abnormally.<p/>
	/// <p/>When this happens, a <see cref="LockObtainFailedException" />
	/// is hit when trying to create a writer, in which case you
	/// need to explicitly clear the lock file first.  You can
	/// either manually remove the file, or use the 
	/// <see cref="Lucene.Net.Index.IndexWriter.Unlock(Directory)" />
	/// API.  But, first be certain that no writer is in fact
	/// writing to the index otherwise you can easily corrupt
	/// your index.<p/>
	/// 
	/// <p/>If you suspect that this or any other LockFactory is
	/// not working properly in your environment, you can easily
	/// test it by using <see cref="VerifyingLockFactory" />, <see cref="LockVerifyServer" />
	/// and <see cref="LockStressTest" />.<p/>
	/// 
	/// </summary>
	/// <seealso cref="LockFactory">
	/// </seealso>
	
	public class SimpleFSLockFactory:FSLockFactory
	{
		
		/// <summary> Create a SimpleFSLockFactory instance, with null (unset)
		/// lock directory. When you pass this factory to a <see cref="FSDirectory" />
		/// subclass, the lock directory is automatically set to the
		/// directory itsself. Be sure to create one instance for each directory
		/// your create!
		/// </summary>
		public SimpleFSLockFactory():this((System.IO.DirectoryInfo) null)
		{
		}

        /// <summary> Instantiate using the provided directory (as a File instance).</summary>
        /// <param name="lockDir">where lock files should be created.
        /// </param>
        public SimpleFSLockFactory(System.IO.DirectoryInfo lockDir)
        {
            LockDir = lockDir;
        }
		
		/// <summary> Instantiate using the provided directory name (String).</summary>
		/// <param name="lockDirName">where lock files should be created.
		/// </param>
		public SimpleFSLockFactory(System.String lockDirName)
            : this(new System.IO.DirectoryInfo(lockDirName))
		{
		}
		
		public override Lock MakeLock(System.String lockName)
		{
			if (internalLockPrefix != null)
			{
				lockName = internalLockPrefix + "-" + lockName;
			}
			return new SimpleFSLock(internalLockDir, lockName);
		}
		
		public override void  ClearLock(System.String lockName)
		{
			bool tmpBool;
			if (System.IO.File.Exists(internalLockDir.FullName))
				tmpBool = true;
			else
				tmpBool = System.IO.Directory.Exists(internalLockDir.FullName);
			if (tmpBool)
			{
				if (internalLockPrefix != null)
				{
					lockName = internalLockPrefix + "-" + lockName;
				}
				System.IO.FileInfo lockFile = new System.IO.FileInfo(System.IO.Path.Combine(internalLockDir.FullName, lockName));
				bool tmpBool2;
				if (System.IO.File.Exists(lockFile.FullName))
					tmpBool2 = true;
				else
					tmpBool2 = System.IO.Directory.Exists(lockFile.FullName);
				bool tmpBool3;
				if (System.IO.File.Exists(lockFile.FullName))
				{
					System.IO.File.Delete(lockFile.FullName);
					tmpBool3 = true;
				}
				else if (System.IO.Directory.Exists(lockFile.FullName))
				{
					System.IO.Directory.Delete(lockFile.FullName);
					tmpBool3 = true;
				}
				else
					tmpBool3 = false;
				if (tmpBool2 && !tmpBool3)
				{
					throw new System.IO.IOException("Cannot delete " + lockFile);
				}
			}
		}
	}
	
	
	class SimpleFSLock:Lock
	{
		
		internal System.IO.FileInfo lockFile;
		internal System.IO.DirectoryInfo lockDir;

		[System.Obsolete("Use the constructor that takes a DirectoryInfo, this will be removed in the 3.0 release")]
		public SimpleFSLock(System.IO.FileInfo lockDir, System.String lockFileName) : this(new System.IO.DirectoryInfo(lockDir.FullName), lockFileName)
		{
		}

        public SimpleFSLock(System.IO.DirectoryInfo lockDir, System.String lockFileName)
        {
            this.lockDir = new System.IO.DirectoryInfo(lockDir.FullName);
            lockFile = new System.IO.FileInfo(System.IO.Path.Combine(lockDir.FullName, lockFileName));
        }
		
		public override bool Obtain()
		{
			
			// Ensure that lockDir exists and is a directory:
			bool tmpBool;
			if (System.IO.File.Exists(lockDir.FullName))
				tmpBool = true;
			else
				tmpBool = System.IO.Directory.Exists(lockDir.FullName);
			if (!tmpBool)
			{
				try
                {
                    System.IO.Directory.CreateDirectory(lockDir.FullName);
                }
                catch
                {
					throw new System.IO.IOException("Cannot create directory: " + lockDir.FullName);
                }
			}
			else
			{
                try
                {
                     System.IO.Directory.Exists(lockDir.FullName);
                }
                catch
                {
    				throw new System.IO.IOException("Found regular file where directory expected: " + lockDir.FullName);
                }
			}

			if (lockFile.Exists)
			{
				return false;
			}
			else
			{
				System.IO.FileStream createdFile = lockFile.Create();
                createdFile.Close();
                return true;
			}
		}
		
		public override void  Release()
		{
			bool tmpBool;
			if (System.IO.File.Exists(lockFile.FullName))
				tmpBool = true;
			else
				tmpBool = System.IO.Directory.Exists(lockFile.FullName);
			bool tmpBool2;
			if (System.IO.File.Exists(lockFile.FullName))
			{
				System.IO.File.Delete(lockFile.FullName);
				tmpBool2 = true;
			}
			else if (System.IO.Directory.Exists(lockFile.FullName))
			{
				System.IO.Directory.Delete(lockFile.FullName);
				tmpBool2 = true;
			}
			else
				tmpBool2 = false;
			if (tmpBool && !tmpBool2)
				throw new LockReleaseFailedException("failed to delete " + lockFile);
		}
		
		public override bool IsLocked()
		{
			bool tmpBool;
			if (System.IO.File.Exists(lockFile.FullName))
				tmpBool = true;
			else
				tmpBool = System.IO.Directory.Exists(lockFile.FullName);
			return tmpBool;
		}
		
		public override System.String ToString()
		{
			return "SimpleFSLock@" + lockFile;
		}
	}
}