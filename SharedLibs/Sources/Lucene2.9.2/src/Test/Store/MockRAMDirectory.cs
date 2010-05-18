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

using NUnit.Framework;

namespace Lucene.Net.Store
{
	
	/// <summary> This is a subclass of RAMDirectory that adds methods
	/// intended to be used only by unit tests.
	/// </summary>
	/// <version>  $Id: RAMDirectory.java 437897 2006-08-29 01:13:10Z yonik $
	/// </version>
	
	[Serializable]
	public class MockRAMDirectory:RAMDirectory
	{
		internal long maxSize;
		
		// Max actual bytes used. This is set by MockRAMOutputStream:
		internal long maxUsedSize;
		internal double randomIOExceptionRate;
		internal System.Random randomState;
		internal bool noDeleteOpenFile = true;
		internal bool preventDoubleWrite = true;
		private System.Collections.Hashtable unSyncedFiles;
		private System.Collections.Hashtable createdFiles;
		internal volatile bool crashed;
		
		// NOTE: we cannot initialize the Map here due to the
		// order in which our constructor actually does this
		// member initialization vs when it calls super.  It seems
		// like super is called, then our members are initialized:
		internal System.Collections.IDictionary openFiles;
		
		private void  Init()
		{
			lock (this)
			{
				if (openFiles == null)
				{
					openFiles = new System.Collections.Hashtable();
				}
				if (createdFiles == null)
				{
					createdFiles = new System.Collections.Hashtable();
				}
				if (unSyncedFiles == null)
				{
					unSyncedFiles = new System.Collections.Hashtable();
				}
			}
		}
		
		public MockRAMDirectory():base()
		{
			Init();
		}
		public MockRAMDirectory(System.String dir):base(dir)
		{
			Init();
		}
		public MockRAMDirectory(Directory dir):base(dir)
		{
			Init();
		}
		public MockRAMDirectory(System.IO.FileInfo dir):base(dir)
		{
			Init();
		}
		
		/// <summary>If set to true, we throw an IOException if the same
		/// file is opened by createOutput, ever. 
		/// </summary>
		public virtual void  SetPreventDoubleWrite(bool value_Renamed)
		{
			preventDoubleWrite = value_Renamed;
		}
		
		public override void  Sync(System.String name)
		{
			lock (this)
			{
				MaybeThrowDeterministicException();
				if (crashed)
					throw new System.IO.IOException("cannot sync after crash");
				if (unSyncedFiles.Contains(name))
					unSyncedFiles.Remove(name);
			}
		}
		
		/// <summary>Simulates a crash of OS or machine by overwriting
		/// unsynced files. 
		/// </summary>
		public virtual void  Crash()
		{
			lock (this)
			{
				crashed = true;
				openFiles = new System.Collections.Hashtable();
				System.Collections.IEnumerator it = unSyncedFiles.GetEnumerator();
				unSyncedFiles = new System.Collections.Hashtable();
				int count = 0;
				while (it.MoveNext())
				{
					System.String name = (System.String) ((System.Collections.DictionaryEntry)it.Current).Value;
					RAMFile file = (RAMFile) fileMap_ForNUnit[name];
					if (count % 3 == 0)
					{
						DeleteFile(name, true);
					}
					else if (count % 3 == 1)
					{
						// Zero out file entirely
						int numBuffers = file.NumBuffers();
						for (int i = 0; i < numBuffers; i++)
						{
							byte[] buffer = file.GetBuffer(i);
							for (int j = 0; j < buffer.Length; j++) buffer[j] = (byte) 0;
						}
					}
					else if (count % 3 == 2)
					{
						// Truncate the file:
						file.SetLength(file.GetLength() / 2);
					}
					count++;
				}
			}
		}
		
		public virtual void  ClearCrash()
		{
			lock (this)
			{
				crashed = false;
			}
		}
		
		public virtual void  SetMaxSizeInBytes(long maxSize)
		{
			this.maxSize = maxSize;
		}
		public virtual long GetMaxSizeInBytes()
		{
			return this.maxSize;
		}
		
		/// <summary> Returns the peek actual storage used (bytes) in this
		/// directory.
		/// </summary>
		public virtual long GetMaxUsedSizeInBytes()
		{
			return this.maxUsedSize;
		}
		public virtual void  ResetMaxUsedSizeInBytes()
		{
			this.maxUsedSize = GetRecomputedActualSizeInBytes();
		}
		
		/// <summary> Emulate windows whereby deleting an open file is not
		/// allowed (raise IOException).
		/// </summary>
		public virtual void  SetNoDeleteOpenFile(bool value_Renamed)
		{
			this.noDeleteOpenFile = value_Renamed;
		}
		public virtual bool GetNoDeleteOpenFile()
		{
			return noDeleteOpenFile;
		}
		
		/// <summary> If 0.0, no exceptions will be thrown.  Else this should
		/// be a double 0.0 - 1.0.  We will randomly throw an
		/// IOException on the first write to an OutputStream based
		/// on this probability.
		/// </summary>
		public virtual void  SetRandomIOExceptionRate(double rate, long seed)
		{
			randomIOExceptionRate = rate;
			// seed so we have deterministic behaviour:
			randomState = new System.Random((System.Int32) seed);
		}
		public virtual double GetRandomIOExceptionRate()
		{
			return randomIOExceptionRate;
		}
		
		internal virtual void  MaybeThrowIOException()
		{
			if (randomIOExceptionRate > 0.0)
			{
				int number = System.Math.Abs(randomState.Next() % 1000);
				if (number < randomIOExceptionRate * 1000)
				{
					throw new System.IO.IOException("a random IOException");
				}
			}
		}
		
		public override void  DeleteFile(System.String name)
		{
			lock (this)
			{
				DeleteFile(name, false);
			}
		}
		
		private void  DeleteFile(System.String name, bool forced)
		{
			lock (this)
			{
				
				MaybeThrowDeterministicException();
				
				if (crashed && !forced)
					throw new System.IO.IOException("cannot delete after crash");
				
				if (unSyncedFiles.Contains(name))
					unSyncedFiles.Remove(name);
				if (!forced)
				{
					if (noDeleteOpenFile && openFiles.Contains(name))
					{
						throw new System.IO.IOException("MockRAMDirectory: file \"" + name + "\" is still open: cannot delete");
					}
				}
				base.DeleteFile(name);
			}
		}
		
		public override IndexOutput CreateOutput(System.String name)
		{
			lock (this)
			{
				if (crashed)
					throw new System.IO.IOException("cannot createOutput after crash");
				Init();
				if (preventDoubleWrite && createdFiles.Contains(name) && !name.Equals("segments.gen"))
					throw new System.IO.IOException("file \"" + name + "\" was already written to");
				if (noDeleteOpenFile && openFiles.Contains(name))
					throw new System.IO.IOException("MockRAMDirectory: file \"" + name + "\" is still open: cannot overwrite");
				RAMFile file = new RAMFile(this);
				if (crashed)
					throw new System.IO.IOException("cannot createOutput after crash");
				SupportClass.CollectionsHelper.AddIfNotContains(unSyncedFiles, name);
				SupportClass.CollectionsHelper.AddIfNotContains(createdFiles, name);
				RAMFile existing = (RAMFile) fileMap_ForNUnit[name];
				// Enforce write once:
				if (existing != null && !name.Equals("segments.gen") && preventDoubleWrite)
					throw new System.IO.IOException("file " + name + " already exists");
				else
				{
					if (existing != null)
					{
						sizeInBytes_ForNUnitTest -= existing.sizeInBytes_ForNUnit;
						existing.directory_ForNUnit = null;
					}
					
					fileMap_ForNUnit[name] = file;
				}
				
				return new MockRAMOutputStream(this, file, name);
			}
		}
		
		public override IndexInput OpenInput(System.String name)
		{
			lock (this)
			{
				RAMFile file = (RAMFile) fileMap_ForNUnit[name];
				if (file == null)
					throw new System.IO.FileNotFoundException(name);
				else
				{
					if (openFiles.Contains(name))
					{
						System.Int32 v = (System.Int32) openFiles[name];
						v = (System.Int32) (v + 1);
						openFiles[name] = v;
					}
					else
					{
						openFiles[name] = 1;
					}
				}
				return new MockRAMInputStream(this, name, file);
			}
		}
		
		/// <summary>Provided for testing purposes.  Use sizeInBytes() instead. </summary>
		public long GetRecomputedSizeInBytes()
		{
			lock (this)
			{
				long size = 0;
				System.Collections.IEnumerator it = fileMap_ForNUnit.Values.GetEnumerator();
				while (it.MoveNext())
				{
					size += ((RAMFile) it.Current).GetSizeInBytes();
				}
				return size;
			}
		}
		
		/// <summary>Like getRecomputedSizeInBytes(), but, uses actual file
		/// lengths rather than buffer allocations (which are
		/// quantized up to nearest
		/// RAMOutputStream.BUFFER_SIZE (now 1024) bytes.
		/// </summary>
		
		public long GetRecomputedActualSizeInBytes()
		{
			lock (this)
			{
				long size = 0;
				System.Collections.IEnumerator it = fileMap_ForNUnit.Values.GetEnumerator();
				while (it.MoveNext())
				{
					size += ((RAMFile) it.Current).length_ForNUnit;
				}
				return size;
			}
		}
		
		public override void  Close()
		{
			lock (this)
			{
				if (openFiles == null)
				{
					openFiles = new System.Collections.Hashtable();
				}
				if (noDeleteOpenFile && openFiles.Count > 0)
				{
					// RuntimeException instead of IOException because
					// super() does not throw IOException currently:
					throw new System.SystemException("MockRAMDirectory: cannot close: there are still open files: " + SupportClass.CollectionsHelper.CollectionToString(openFiles));
				}
			}
		}
		
		/// <summary> Objects that represent fail-able conditions. Objects of a derived
		/// class are created and registered with the mock directory. After
		/// register, each object will be invoked once for each first write
		/// of a file, giving the object a chance to throw an IOException.
		/// </summary>
		public class Failure
		{
			/// <summary> eval is called on the first write of every new file.</summary>
			public virtual void  Eval(MockRAMDirectory dir)
			{
			}
			
			/// <summary> reset should set the state of the failure to its default
			/// (freshly constructed) state. Reset is convenient for tests
			/// that want to create one failure object and then reuse it in
			/// multiple cases. This, combined with the fact that Failure
			/// subclasses are often anonymous classes makes reset difficult to
			/// do otherwise.
			/// 
			/// A typical example of use is
			/// Failure failure = new Failure() { ... };
			/// ...
			/// mock.failOn(failure.reset())
			/// </summary>
			public virtual Failure Reset()
			{
				return this;
			}
			
			protected internal bool doFail;
			
			public virtual void  SetDoFail()
			{
				doFail = true;
			}
			
			public virtual void  ClearDoFail()
			{
				doFail = false;
			}
		}
		
		internal System.Collections.ArrayList failures;
		
		/// <summary> add a Failure object to the list of objects to be evaluated
		/// at every potential failure point
		/// </summary>
		public virtual void  FailOn(Failure fail)
		{
			lock (this)
			{
				if (failures == null)
				{
					failures = new System.Collections.ArrayList();
				}
				failures.Add(fail);
			}
		}
		
		/// <summary> Iterate through the failures list, giving each object a
		/// chance to throw an IOE
		/// </summary>
		internal virtual void  MaybeThrowDeterministicException()
		{
			lock (this)
			{
				if (failures != null)
				{
					for (int i = 0; i < failures.Count; i++)
					{
						((Failure) failures[i]).Eval(this);
					}
				}
			}
		}
	}
}