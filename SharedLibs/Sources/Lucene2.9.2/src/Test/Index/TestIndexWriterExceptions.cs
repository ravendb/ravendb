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

using Analyzer = Lucene.Net.Analysis.Analyzer;
using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestIndexWriterExceptions:LuceneTestCase
	{
		
		private const bool DEBUG = false;
		
		private class IndexerThread:SupportClass.ThreadClass
		{
			private void  InitBlock(TestIndexWriterExceptions enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriterExceptions enclosingInstance;
			public TestIndexWriterExceptions Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			internal IndexWriter writer;
			
			internal System.Random r = new System.Random((System.Int32) 47);
			internal System.Exception failure;
			
			public IndexerThread(TestIndexWriterExceptions enclosingInstance, int i, IndexWriter writer)
			{
				InitBlock(enclosingInstance);
				Name = "Indexer " + i;
				this.writer = writer;
			}
			
			override public void  Run()
			{
				
				Document doc = new Document();
				
				doc.Add(new Field("content1", "aaa bbb ccc ddd", Field.Store.YES, Field.Index.ANALYZED));
				doc.Add(new Field("content6", "aaa bbb ccc ddd", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
				doc.Add(new Field("content2", "aaa bbb ccc ddd", Field.Store.YES, Field.Index.NOT_ANALYZED));
				doc.Add(new Field("content3", "aaa bbb ccc ddd", Field.Store.YES, Field.Index.NO));
				
				doc.Add(new Field("content4", "aaa bbb ccc ddd", Field.Store.NO, Field.Index.ANALYZED));
				doc.Add(new Field("content5", "aaa bbb ccc ddd", Field.Store.NO, Field.Index.NOT_ANALYZED));
				
				doc.Add(new Field("content7", "aaa bbb ccc ddd", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
				
				Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
				doc.Add(idField);
				
				long stopTime = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) + 3000;
				
				while ((DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) < stopTime)
				{
					System.Threading.Thread.SetData(Enclosing_Instance.doFail, this);
					System.String id = "" + r.Next(50);
					idField.SetValue(id);
					Term idTerm = new Term("id", id);
					try
					{
						writer.UpdateDocument(idTerm, doc);
					}
					catch (System.SystemException re)
					{
						if (Lucene.Net.Index.TestIndexWriterExceptions.DEBUG)
						{
							System.Console.Out.WriteLine("EXC: ");
							System.Console.Out.WriteLine(re.StackTrace);
						}
						try
						{
							_TestUtil.CheckIndex(writer.GetDirectory());
						}
						catch (System.IO.IOException ioe)
						{
							System.Console.Out.WriteLine(SupportClass.ThreadClass.Current().Name + ": unexpected exception1");
							System.Console.Out.WriteLine(ioe.StackTrace);
							failure = ioe;
							break;
						}
					}
					catch (System.Exception t)
					{
						System.Console.Out.WriteLine(SupportClass.ThreadClass.Current().Name + ": unexpected exception2");
						System.Console.Out.WriteLine(t.StackTrace);
						failure = t;
						break;
					}
					
					System.Threading.Thread.SetData(Enclosing_Instance.doFail, null);
					
					// After a possible exception (above) I should be able
					// to add a new document without hitting an
					// exception:
					try
					{
						writer.UpdateDocument(idTerm, doc);
					}
					catch (System.Exception t)
					{
						System.Console.Out.WriteLine(SupportClass.ThreadClass.Current().Name + ": unexpected exception3");
						System.Console.Out.WriteLine(t.StackTrace);
						failure = t;
						break;
					}
				}
			}
		}
		
		internal System.LocalDataStoreSlot doFail = System.Threading.Thread.AllocateDataSlot();
		
		public class MockIndexWriter:IndexWriter
		{
			private void  InitBlock(TestIndexWriterExceptions enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriterExceptions enclosingInstance;
			public TestIndexWriterExceptions Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal System.Random r = new System.Random((System.Int32) 17);
			
			public MockIndexWriter(TestIndexWriterExceptions enclosingInstance, Directory dir, Analyzer a, bool create, MaxFieldLength mfl):base(dir, a, create, mfl)
			{
				InitBlock(enclosingInstance);
			}
			
			public /*internal*/ override bool TestPoint(System.String name)
			{
				if (System.Threading.Thread.GetData(Enclosing_Instance.doFail) != null && !name.Equals("startDoFlush") && r.Next(20) == 17)
				{
					if (Lucene.Net.Index.TestIndexWriterExceptions.DEBUG)
					{
						System.Console.Out.WriteLine(SupportClass.ThreadClass.Current().Name + ": NOW FAIL: " + name);
						//new Throwable().printStackTrace(System.out);
					}
					throw new System.SystemException(SupportClass.ThreadClass.Current().Name + ": intentionally failing at " + name);
				}
				return true;
			}
		}
		
		[Test]
		public virtual void  TestRandomExceptions()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			
			MockIndexWriter writer = new MockIndexWriter(this, dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			((ConcurrentMergeScheduler) writer.GetMergeScheduler()).SetSuppressExceptions();
			//writer.setMaxBufferedDocs(10);
			writer.SetRAMBufferSizeMB(0.1);
			
			if (DEBUG)
			{
				System.IO.StreamWriter temp_writer;
				temp_writer = new System.IO.StreamWriter(System.Console.OpenStandardOutput(), System.Console.Out.Encoding);
				temp_writer.AutoFlush = true;
				writer.SetInfoStream(temp_writer);
			}
			
			IndexerThread thread = new IndexerThread(this, 0, writer);
			thread.Run();
			if (thread.failure != null)
			{
				System.Console.Out.WriteLine(thread.failure.StackTrace);
				Assert.Fail("thread " + thread.Name + ": hit unexpected failure");
			}
			
			writer.Commit();
			
			try
			{
				writer.Close();
			}
			catch (System.Exception t)
			{
				System.Console.Out.WriteLine("exception during close:");
				System.Console.Out.WriteLine(t.StackTrace);
				writer.Rollback();
			}
			
			// Confirm that when doc hits exception partway through tokenization, it's deleted:
			IndexReader r2 = IndexReader.Open(dir);
			int count = r2.DocFreq(new Term("content4", "aaa"));
			int count2 = r2.DocFreq(new Term("content4", "ddd"));
			Assert.AreEqual(count, count2);
			r2.Close();
			
			_TestUtil.CheckIndex(dir);
		}
		
		[Test]
		public virtual void  TestRandomExceptionsThreads()
		{
			
			MockRAMDirectory dir = new MockRAMDirectory();
			MockIndexWriter writer = new MockIndexWriter(this, dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			((ConcurrentMergeScheduler) writer.GetMergeScheduler()).SetSuppressExceptions();
			//writer.setMaxBufferedDocs(10);
			writer.SetRAMBufferSizeMB(0.2);
			
			if (DEBUG)
			{
				System.IO.StreamWriter temp_writer;
				temp_writer = new System.IO.StreamWriter(System.Console.OpenStandardOutput(), System.Console.Out.Encoding);
				temp_writer.AutoFlush = true;
				writer.SetInfoStream(temp_writer);
			}
			
			int NUM_THREADS = 4;
			
			IndexerThread[] threads = new IndexerThread[NUM_THREADS];
			for (int i = 0; i < NUM_THREADS; i++)
			{
				threads[i] = new IndexerThread(this, i, writer);
				threads[i].Start();
			}
			
			for (int i = 0; i < NUM_THREADS; i++)
				threads[i].Join();
			
			for (int i = 0; i < NUM_THREADS; i++)
				if (threads[i].failure != null)
					Assert.Fail("thread " + threads[i].Name + ": hit unexpected failure");
			
			writer.Commit();
			
			try
			{
				writer.Close();
			}
			catch (System.Exception t)
			{
				System.Console.Out.WriteLine("exception during close:");
				System.Console.Out.WriteLine(t.StackTrace);
				writer.Rollback();
			}
			
			// Confirm that when doc hits exception partway through tokenization, it's deleted:
			IndexReader r2 = IndexReader.Open(dir);
			int count = r2.DocFreq(new Term("content4", "aaa"));
			int count2 = r2.DocFreq(new Term("content4", "ddd"));
			Assert.AreEqual(count, count2);
			r2.Close();
			
			_TestUtil.CheckIndex(dir);
		}
	}
}