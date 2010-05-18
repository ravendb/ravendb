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

using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using TermQuery = Lucene.Net.Search.TermQuery;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Store
{
	
	[TestFixture]
	public class TestLockFactory:LuceneTestCase
	{
		
		// Verify: we can provide our own LockFactory implementation, the right
		// methods are called at the right time, locks are created, etc.
		
		[Test]
		public virtual void  TestCustomLockFactory()
		{
			Directory dir = new RAMDirectory();
			MockLockFactory lf = new MockLockFactory(this);
			dir.SetLockFactory(lf);
			
			// Lock prefix should have been set:
			Assert.IsTrue(lf.lockPrefixSet, "lock prefix was not set by the RAMDirectory");
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			// add 100 documents (so that commit lock is used)
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer);
			}
			
			// Both write lock and commit lock should have been created:
			Assert.AreEqual(1, lf.locksCreated.Count, "# of unique locks created (after instantiating IndexWriter)");
			Assert.IsTrue(lf.makeLockCount >= 1, "# calls to makeLock is 0 (after instantiating IndexWriter)");
			
			for (System.Collections.IEnumerator e = lf.locksCreated.Keys.GetEnumerator(); e.MoveNext(); )
			{
				System.String lockName = (System.String) e.Current;
				MockLockFactory.MockLock lock_Renamed = (MockLockFactory.MockLock) lf.locksCreated[lockName];
				Assert.IsTrue(lock_Renamed.lockAttempts > 0, "# calls to Lock.obtain is 0 (after instantiating IndexWriter)");
			}
			
			writer.Close();
		}
		
		// Verify: we can use the NoLockFactory with RAMDirectory w/ no
		// exceptions raised:
		// Verify: NoLockFactory allows two IndexWriters
		[Test]
		public virtual void  TestRAMDirectoryNoLocking()
		{
			Directory dir = new RAMDirectory();
			dir.SetLockFactory(NoLockFactory.GetNoLockFactory());
			
			Assert.IsTrue(typeof(NoLockFactory).IsInstanceOfType(dir.GetLockFactory()), "RAMDirectory.setLockFactory did not take");
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			// Create a 2nd IndexWriter.  This is normally not allowed but it should run through since we're not
			// using any locks:
			IndexWriter writer2 = null;
			try
			{
				writer2 = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			}
			catch (System.Exception e)
			{
				System.Console.Out.WriteLine(e.StackTrace);
				Assert.Fail("Should not have hit an IOException with no locking");
			}
			
			writer.Close();
			if (writer2 != null)
			{
				writer2.Close();
			}
		}
		
		// Verify: SingleInstanceLockFactory is the default lock for RAMDirectory
		// Verify: RAMDirectory does basic locking correctly (can't create two IndexWriters)
		[Test]
		public virtual void  TestDefaultRAMDirectory()
		{
			Directory dir = new RAMDirectory();
			
			Assert.IsTrue(typeof(SingleInstanceLockFactory).IsInstanceOfType(dir.GetLockFactory()), "RAMDirectory did not use correct LockFactory: got " + dir.GetLockFactory());
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			// Create a 2nd IndexWriter.  This should fail:
			IndexWriter writer2 = null;
			try
			{
				writer2 = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
				Assert.Fail("Should have hit an IOException with two IndexWriters on default SingleInstanceLockFactory");
			}
			catch (System.IO.IOException e)
			{
			}
			
			writer.Close();
			if (writer2 != null)
			{
				writer2.Close();
			}
		}
		
		// Verify: SimpleFSLockFactory is the default for FSDirectory
		// Verify: FSDirectory does basic locking correctly
		[Test]
		public virtual void  TestDefaultFSDirectory()
		{
			System.IO.FileInfo indexDirName = _TestUtil.GetTempDir("index.TestLockFactory1");
			
			IndexWriter writer = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			Assert.IsTrue(typeof(SimpleFSLockFactory).IsInstanceOfType(writer.GetDirectory().GetLockFactory()) || typeof(NativeFSLockFactory).IsInstanceOfType(writer.GetDirectory().GetLockFactory()), "FSDirectory did not use correct LockFactory: got " + writer.GetDirectory().GetLockFactory());
			
			IndexWriter writer2 = null;
			
			// Create a 2nd IndexWriter.  This should fail:
			try
			{
				writer2 = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
				Assert.Fail("Should have hit an IOException with two IndexWriters on default SimpleFSLockFactory");
			}
			catch (System.IO.IOException e)
			{
			}
			
			writer.Close();
			if (writer2 != null)
			{
				writer2.Close();
			}
			
			// Cleanup
			_TestUtil.RmDir(indexDirName);
		}
		
		// Verify: FSDirectory's default lockFactory clears all locks correctly
		[Test]
		public virtual void  TestFSDirectoryTwoCreates()
		{
			System.IO.FileInfo indexDirName = _TestUtil.GetTempDir("index.TestLockFactory2");
			
			IndexWriter writer = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			Assert.IsTrue(typeof(SimpleFSLockFactory).IsInstanceOfType(writer.GetDirectory().GetLockFactory()) || typeof(NativeFSLockFactory).IsInstanceOfType(writer.GetDirectory().GetLockFactory()), "FSDirectory did not use correct LockFactory: got " + writer.GetDirectory().GetLockFactory());
			
			// Intentionally do not close the first writer here.
			// The goal is to "simulate" a crashed writer and
			// ensure the second writer, with create=true, is
			// able to remove the lock files.  This works OK
			// with SimpleFSLockFactory as the locking
			// implementation.  Note, however, that this test
			// will not work on WIN32 when we switch to
			// NativeFSLockFactory as the default locking for
			// FSDirectory because the second IndexWriter cannot
			// remove those lock files since they are held open
			// by the first writer.  This is because leaving the
			// first IndexWriter open is not really a good way
			// to simulate a crashed writer.
			
			// Create a 2nd IndexWriter.  This should not fail:
			IndexWriter writer2 = null;
			try
			{
				writer2 = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			}
			catch (System.IO.IOException e)
			{
				System.Console.Out.WriteLine(e.StackTrace);
				Assert.Fail("Should not have hit an IOException with two IndexWriters with create=true, on default SimpleFSLockFactory");
			}
			
			writer.Close();
			if (writer2 != null)
			{
				try
				{
					writer2.Close();
					// expected
				}
				catch (LockReleaseFailedException e)
				{
					Assert.Fail("writer2.close() should not have hit LockReleaseFailedException");
				}
			}
			
			// Cleanup
			_TestUtil.RmDir(indexDirName);
		}
		
		
		// Verify: setting custom lock factory class (as system property) works:
		// Verify: all 4 builtin LockFactory implementations are
		//         settable this way 
		// Verify: FSDirectory does basic locking correctly
		[Test]
		public virtual void  TestLockClassProperty()
		{
			System.IO.FileInfo indexDirName = _TestUtil.GetTempDir("index.TestLockFactory3");
			System.String prpName = "Lucene.Net.Store.FSDirectoryLockFactoryClass";
			
			try
			{
				
				// NoLockFactory:
				SupportClass.AppSettings.Set(prpName, "Lucene.Net.Store.NoLockFactory");
				IndexWriter writer = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				Assert.IsTrue(typeof(NoLockFactory).IsInstanceOfType(writer.GetDirectory().GetLockFactory()), "FSDirectory did not use correct LockFactory: got " + writer.GetDirectory().GetLockFactory());
				writer.Close();
				
				// SingleInstanceLockFactory:
				SupportClass.AppSettings.Set(prpName, "Lucene.Net.Store.SingleInstanceLockFactory");
				writer = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				Assert.IsTrue(typeof(SingleInstanceLockFactory).IsInstanceOfType(writer.GetDirectory().GetLockFactory()), "FSDirectory did not use correct LockFactory: got " + writer.GetDirectory().GetLockFactory());
				writer.Close();
				
				// NativeFSLockFactory:
				SupportClass.AppSettings.Set(prpName, "Lucene.Net.Store.NativeFSLockFactory");
				writer = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				Assert.IsTrue(typeof(NativeFSLockFactory).IsInstanceOfType(writer.GetDirectory().GetLockFactory()), "FSDirectory did not use correct LockFactory: got " + writer.GetDirectory().GetLockFactory());
				writer.Close();
				
				// SimpleFSLockFactory:
				SupportClass.AppSettings.Set(prpName, "Lucene.Net.Store.SimpleFSLockFactory");
				writer = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				Assert.IsTrue(typeof(SimpleFSLockFactory).IsInstanceOfType(writer.GetDirectory().GetLockFactory()), "FSDirectory did not use correct LockFactory: got " + writer.GetDirectory().GetLockFactory());
				writer.Close();
			}
			finally
			{
				// Put back to the correct default for subsequent tests:
				SupportClass.AppSettings.Set("Lucene.Net.Store.FSDirectoryLockFactoryClass", "");
			}
			
			// Cleanup
			_TestUtil.RmDir(indexDirName);
		}
		
		// Verify: setDisableLocks works
		[Test]
		public virtual void  TestDisableLocks()
		{
			System.IO.FileInfo indexDirName = _TestUtil.GetTempDir("index.TestLockFactory4");
			
			Assert.IsTrue(!FSDirectory.GetDisableLocks(), "Locks are already disabled");
			FSDirectory.SetDisableLocks(true);
			
			IndexWriter writer = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			Assert.IsTrue(typeof(NoLockFactory).IsInstanceOfType(writer.GetDirectory().GetLockFactory()), "FSDirectory did not use correct default LockFactory: got " + writer.GetDirectory().GetLockFactory());
			
			// Should be no error since locking is disabled:
			IndexWriter writer2 = null;
			try
			{
				writer2 = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			}
			catch (System.IO.IOException e)
			{
				System.Console.Out.WriteLine(e.StackTrace);
				Assert.Fail("Should not have hit an IOException with locking disabled");
			}
			
			FSDirectory.SetDisableLocks(false);
			writer.Close();
			if (writer2 != null)
			{
				writer2.Close();
			}
			// Cleanup
			_TestUtil.RmDir(indexDirName);
		}
		
		// Verify: if I try to getDirectory() with two different locking implementations, I get an IOException
		[Test]
		public virtual void  TestFSDirectoryDifferentLockFactory()
		{
			System.IO.FileInfo indexDirName = _TestUtil.GetTempDir("index.TestLockFactory5");
			
			LockFactory lf = new SingleInstanceLockFactory();
			FSDirectory fs1 = FSDirectory.GetDirectory(indexDirName, lf);
			
			// Different lock factory instance should hit IOException:
			try
			{
				FSDirectory.GetDirectory(indexDirName, new SingleInstanceLockFactory());
				Assert.Fail("Should have hit an IOException because LockFactory instances differ");
			}
			catch (System.IO.IOException e)
			{
			}
			
			FSDirectory fs2 = null;
			
			// Same lock factory instance should not:
			try
			{
				fs2 = FSDirectory.GetDirectory(indexDirName, lf);
			}
			catch (System.IO.IOException e)
			{
				System.Console.Out.WriteLine(e.StackTrace);
				Assert.Fail("Should not have hit an IOException because LockFactory instances are the same");
			}
			
			fs1.Close();
			if (fs2 != null)
			{
				fs2.Close();
			}
			// Cleanup
			_TestUtil.RmDir(indexDirName);
		}
		
		// Verify: do stress test, by opening IndexReaders and
		// IndexWriters over & over in 2 threads and making sure
		// no unexpected exceptions are raised:
		[Test]
		public virtual void  TestStressLocks()
		{
			_testStressLocks(null, _TestUtil.GetTempDir("index.TestLockFactory6"));
		}
		
		// Verify: do stress test, by opening IndexReaders and
		// IndexWriters over & over in 2 threads and making sure
		// no unexpected exceptions are raised, but use
		// NativeFSLockFactory:
		[Test]
		public virtual void  TestStressLocksNativeFSLockFactory()
		{
			System.IO.FileInfo dir = _TestUtil.GetTempDir("index.TestLockFactory7");
			_testStressLocks(new NativeFSLockFactory(dir), dir);
		}
		
		public virtual void  _testStressLocks(LockFactory lockFactory, System.IO.FileInfo indexDir)
		{
			FSDirectory fs1 = FSDirectory.Open(new System.IO.DirectoryInfo(indexDir.FullName), lockFactory);
			
			// First create a 1 doc index:
			IndexWriter w = new IndexWriter(fs1, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDoc(w);
			w.Close();
			
			WriterThread writer = new WriterThread(this, 100, fs1);
			SearcherThread searcher = new SearcherThread(this, 100, fs1);
			writer.Start();
			searcher.Start();
			
			while (writer.IsAlive || searcher.IsAlive)
			{
				System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 1000));
			}
			
			Assert.IsTrue(!writer.hitException, "IndexWriter hit unexpected exceptions");
			Assert.IsTrue(!searcher.hitException, "IndexSearcher hit unexpected exceptions");
			
			// Cleanup
			_TestUtil.RmDir(indexDir);
		}
		
		// Verify: NativeFSLockFactory works correctly
		[Test]
		public virtual void  TestNativeFSLockFactory()
		{
			
			NativeFSLockFactory f = new NativeFSLockFactory(SupportClass.AppSettings.Get("tempDir", System.IO.Path.GetTempPath()));
			
			f.SetLockPrefix("test");
			Lock l = f.MakeLock("commit");
			Lock l2 = f.MakeLock("commit");
			
			Assert.IsTrue(l.Obtain(), "failed to obtain lock");
			Assert.IsTrue(!l2.Obtain(), "succeeded in obtaining lock twice");
			l.Release();
			
			Assert.IsTrue(l2.Obtain(), "failed to obtain 2nd lock after first one was freed");
			l2.Release();
			
			// Make sure we can obtain first one again, test isLocked():
			Assert.IsTrue(l.Obtain(), "failed to obtain lock");
			Assert.IsTrue(l.IsLocked());
			Assert.IsTrue(l2.IsLocked());
			l.Release();
			Assert.IsFalse(l.IsLocked());
			Assert.IsFalse(l2.IsLocked());
		}
		
		// Verify: NativeFSLockFactory assigns null as lockPrefix if the lockDir is inside directory
		[Test]
		public virtual void  TestNativeFSLockFactoryPrefix()
		{
			
			System.IO.FileInfo fdir1 = _TestUtil.GetTempDir("TestLockFactory.8");
			System.IO.FileInfo fdir2 = _TestUtil.GetTempDir("TestLockFactory.8.Lockdir");
			Directory dir1 = FSDirectory.Open(new System.IO.DirectoryInfo(fdir1.FullName), new NativeFSLockFactory(fdir1));
			// same directory, but locks are stored somewhere else. The prefix of the lock factory should != null
			Directory dir2 = FSDirectory.Open(new System.IO.DirectoryInfo(fdir1.FullName), new NativeFSLockFactory(fdir2));
			
			System.String prefix1 = dir1.GetLockFactory().GetLockPrefix();
			Assert.IsNull(prefix1, "Lock prefix for lockDir same as directory should be null");
			
			System.String prefix2 = dir2.GetLockFactory().GetLockPrefix();
			Assert.IsNotNull(prefix2, "Lock prefix for lockDir outside of directory should be not null");
			
			_TestUtil.RmDir(fdir1);
			_TestUtil.RmDir(fdir2);
		}
		
		// Verify: default LockFactory has no prefix (ie
		// write.lock is stored in index):
		[Test]
		public virtual void  TestDefaultFSLockFactoryPrefix()
		{
			
			// Make sure we get null prefix:
			System.IO.FileInfo dirName = _TestUtil.GetTempDir("TestLockFactory.10");
			Directory dir = FSDirectory.Open(dirName);
			
			System.String prefix = dir.GetLockFactory().GetLockPrefix();
			
			Assert.IsTrue(null == prefix, "Default lock prefix should be null");
			
			_TestUtil.RmDir(dirName);
		}
		
		private class WriterThread:SupportClass.ThreadClass
		{
			private void  InitBlock(TestLockFactory enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestLockFactory enclosingInstance;
			public TestLockFactory Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private Directory dir;
			private int numIteration;
			public bool hitException = false;
			public WriterThread(TestLockFactory enclosingInstance, int numIteration, Directory dir)
			{
				InitBlock(enclosingInstance);
				this.numIteration = numIteration;
				this.dir = dir;
			}
			override public void  Run()
			{
				WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer();
				IndexWriter writer = null;
				for (int i = 0; i < this.numIteration; i++)
				{
					try
					{
						writer = new IndexWriter(dir, analyzer, false, IndexWriter.MaxFieldLength.LIMITED);
					}
					catch (System.IO.IOException e)
					{
						if (e.ToString().IndexOf(" timed out:") == - 1)
						{
							hitException = true;
							System.Console.Out.WriteLine("Stress Test Index Writer: creation hit unexpected IOException: " + e.ToString());
							System.Console.Out.WriteLine(e.StackTrace);
						}
						else
						{
							// lock obtain timed out
							// NOTE: we should at some point
							// consider this a failure?  The lock
							// obtains, across IndexReader &
							// IndexWriters should be "fair" (ie
							// FIFO).
						}
					}
					catch (System.Exception e)
					{
						hitException = true;
						System.Console.Out.WriteLine("Stress Test Index Writer: creation hit unexpected exception: " + e.ToString());
						System.Console.Out.WriteLine(e.StackTrace);
						break;
					}
					if (writer != null)
					{
						try
						{
							Enclosing_Instance.AddDoc(writer);
						}
						catch (System.IO.IOException e)
						{
							hitException = true;
							System.Console.Out.WriteLine("Stress Test Index Writer: addDoc hit unexpected exception: " + e.ToString());
							System.Console.Out.WriteLine(e.StackTrace);
							break;
						}
						try
						{
							writer.Close();
						}
						catch (System.IO.IOException e)
						{
							hitException = true;
							System.Console.Out.WriteLine("Stress Test Index Writer: close hit unexpected exception: " + e.ToString());
							System.Console.Out.WriteLine(e.StackTrace);
							break;
						}
						writer = null;
					}
				}
			}
		}
		
		private class SearcherThread:SupportClass.ThreadClass
		{
			private void  InitBlock(TestLockFactory enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestLockFactory enclosingInstance;
			public TestLockFactory Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private Directory dir;
			private int numIteration;
			public bool hitException = false;
			public SearcherThread(TestLockFactory enclosingInstance, int numIteration, Directory dir)
			{
				InitBlock(enclosingInstance);
				this.numIteration = numIteration;
				this.dir = dir;
			}
			override public void  Run()
			{
				IndexSearcher searcher = null;
				Query query = new TermQuery(new Term("content", "aaa"));
				for (int i = 0; i < this.numIteration; i++)
				{
					try
					{
						searcher = new IndexSearcher(dir);
					}
					catch (System.Exception e)
					{
						hitException = true;
						System.Console.Out.WriteLine("Stress Test Index Searcher: create hit unexpected exception: " + e.ToString());
						System.Console.Out.WriteLine(e.StackTrace);
						break;
					}
					if (searcher != null)
					{
						ScoreDoc[] hits = null;
						try
						{
							hits = searcher.Search(query, null, 1000).scoreDocs;
						}
						catch (System.IO.IOException e)
						{
							hitException = true;
							System.Console.Out.WriteLine("Stress Test Index Searcher: search hit unexpected exception: " + e.ToString());
							System.Console.Out.WriteLine(e.StackTrace);
							break;
						}
						// System.out.println(hits.length() + " total results");
						try
						{
							searcher.Close();
						}
						catch (System.IO.IOException e)
						{
							hitException = true;
							System.Console.Out.WriteLine("Stress Test Index Searcher: close hit unexpected exception: " + e.ToString());
							System.Console.Out.WriteLine(e.StackTrace);
							break;
						}
						searcher = null;
					}
				}
			}
		}
		
		public class MockLockFactory:LockFactory
		{
			public MockLockFactory(TestLockFactory enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestLockFactory enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestLockFactory enclosingInstance;
			public TestLockFactory Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public bool lockPrefixSet;
			public System.Collections.IDictionary locksCreated = System.Collections.Hashtable.Synchronized(new System.Collections.Hashtable(new System.Collections.Hashtable()));
			public int makeLockCount = 0;
			
			public override void  SetLockPrefix(System.String lockPrefix)
			{
				base.SetLockPrefix(lockPrefix);
				lockPrefixSet = true;
			}
			
			public override Lock MakeLock(System.String lockName)
			{
				lock (this)
				{
					Lock lock_Renamed = new MockLock(this);
					locksCreated[lockName] = lock_Renamed;
					makeLockCount++;
					return lock_Renamed;
				}
			}
			
			public override void  ClearLock(System.String specificLockName)
			{
			}
			
			public class MockLock:Lock
			{
				public MockLock(MockLockFactory enclosingInstance)
				{
					InitBlock(enclosingInstance);
				}
				private void  InitBlock(MockLockFactory enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
				}
				private MockLockFactory enclosingInstance;
				public MockLockFactory Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				public int lockAttempts;
				
				public override bool Obtain()
				{
					lockAttempts++;
					return true;
				}
				public override void  Release()
				{
					// do nothing
				}
				public override bool IsLocked()
				{
					return false;
				}
			}
		}
		
		private void  AddDoc(IndexWriter writer)
		{
			Document doc = new Document();
			doc.Add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
		}
	}
}