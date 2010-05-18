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
using Index = Lucene.Net.Documents.Field.Index;
using Store = Lucene.Net.Documents.Field.Store;
using TermVector = Lucene.Net.Documents.Field.TermVector;
using AlreadyClosedException = Lucene.Net.Store.AlreadyClosedException;
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using TermQuery = Lucene.Net.Search.TermQuery;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestIndexWriterReader:LuceneTestCase
	{
		private class AnonymousClassThread:SupportClass.ThreadClass
		{
			public AnonymousClassThread(long endTime, Lucene.Net.Index.IndexWriter writer, Lucene.Net.Store.Directory[] dirs, System.Collections.IList excs, TestIndexWriterReader enclosingInstance)
			{
				InitBlock(endTime, writer, dirs, excs, enclosingInstance);
			}
			private void  InitBlock(long endTime, Lucene.Net.Index.IndexWriter writer, Lucene.Net.Store.Directory[] dirs, System.Collections.IList excs, TestIndexWriterReader enclosingInstance)
			{
				this.endTime = endTime;
				this.writer = writer;
				this.dirs = dirs;
				this.excs = excs;
				this.enclosingInstance = enclosingInstance;
			}
			private long endTime;
			private Lucene.Net.Index.IndexWriter writer;
			private Lucene.Net.Store.Directory[] dirs;
			private System.Collections.IList excs;
			private TestIndexWriterReader enclosingInstance;
			public TestIndexWriterReader Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			override public void  Run()
			{
				while ((DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) < endTime)
				{
					try
					{
						writer.AddIndexesNoOptimize(dirs);
					}
					catch (System.Exception t)
					{
						excs.Add(t);
						throw new System.SystemException("", t);
					}
				}
			}
		}
		private class AnonymousClassThread1:SupportClass.ThreadClass
		{
			public AnonymousClassThread1(long endTime, Lucene.Net.Index.IndexWriter writer, System.Collections.IList excs, TestIndexWriterReader enclosingInstance)
			{
				InitBlock(endTime, writer, excs, enclosingInstance);
			}
			private void  InitBlock(long endTime, Lucene.Net.Index.IndexWriter writer, System.Collections.IList excs, TestIndexWriterReader enclosingInstance)
			{
				this.endTime = endTime;
				this.writer = writer;
				this.excs = excs;
				this.enclosingInstance = enclosingInstance;
			}
			private long endTime;
			private Lucene.Net.Index.IndexWriter writer;
			private System.Collections.IList excs;
			private TestIndexWriterReader enclosingInstance;
			public TestIndexWriterReader Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			override public void  Run()
			{
				int count = 0;
				System.Random r = new System.Random();
				while ((DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) < endTime)
				{
					try
					{
						for (int i = 0; i < 10; i++)
						{
							writer.AddDocument(Lucene.Net.Index.TestIndexWriterReader.CreateDocument(10 * count + i, "test", 4));
						}
						count++;
						int limit = count * 10;
						for (int i = 0; i < 5; i++)
						{
							int x = r.Next(limit);
							writer.DeleteDocuments(new Term("field3", "b" + x));
						}
					}
					catch (System.Exception t)
					{
						excs.Add(t);
						throw new System.SystemException("", t);
					}
				}
			}
		}
		internal static System.IO.StreamWriter infoStream;
		
		public class HeavyAtomicInt
		{
			private int value_Renamed;
			public HeavyAtomicInt(int start)
			{
				value_Renamed = start;
			}
			public virtual int AddAndGet(int inc)
			{
				lock (this)
				{
					value_Renamed += inc;
					return value_Renamed;
				}
			}
			public virtual int IncrementAndGet()
			{
				lock (this)
				{
					value_Renamed++;
					return value_Renamed;
				}
			}
			public virtual int IntValue()
			{
				lock (this)
				{
					return value_Renamed;
				}
			}
		}
		
		public static int Count(Term t, IndexReader r)
		{
			int count = 0;
			TermDocs td = r.TermDocs(t);
			while (td.Next())
			{
				td.Doc();
				count++;
			}
			td.Close();
			return count;
		}
		
		[Test]
		public virtual void  TestUpdateDocument()
		{
			bool optimize = true;
			
			Directory dir1 = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			
			// create the index
			CreateIndexNoClose(!optimize, "index1", writer);
			
			// writer.flush(false, true, true);
			
			// get a reader
			IndexReader r1 = writer.GetReader();
			Assert.IsTrue(r1.IsCurrent());
			
			System.String id10 = r1.Document(10).GetField("id").StringValue();
			
			Document newDoc = r1.Document(10);
			newDoc.RemoveField("id");
			newDoc.Add(new Field("id", System.Convert.ToString(8000), Field.Store.YES, Field.Index.NOT_ANALYZED));
			writer.UpdateDocument(new Term("id", id10), newDoc);
			Assert.IsFalse(r1.IsCurrent());
			
			IndexReader r2 = writer.GetReader();
			Assert.IsTrue(r2.IsCurrent());
			Assert.AreEqual(0, Count(new Term("id", id10), r2));
			Assert.AreEqual(1, Count(new Term("id", System.Convert.ToString(8000)), r2));
			
			r1.Close();
			writer.Close();
			Assert.IsTrue(r2.IsCurrent());
			
			IndexReader r3 = IndexReader.Open(dir1);
			Assert.IsTrue(r3.IsCurrent());
			Assert.IsTrue(r2.IsCurrent());
			Assert.AreEqual(0, Count(new Term("id", id10), r3));
			Assert.AreEqual(1, Count(new Term("id", System.Convert.ToString(8000)), r3));
			
			writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "a b c", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			Assert.IsTrue(r2.IsCurrent());
			Assert.IsTrue(r3.IsCurrent());
			
			writer.Close();
			
			Assert.IsFalse(r2.IsCurrent());
			Assert.IsTrue(!r3.IsCurrent());
			
			r2.Close();
			r3.Close();
			
			dir1.Close();
		}
		
		/// <summary> Test using IW.addIndexes
		/// 
		/// </summary>
		/// <throws>  Exception </throws>
		[Test]
		public virtual void  TestAddIndexes()
		{
			bool optimize = false;
			
			Directory dir1 = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetInfoStream(infoStream);
			// create the index
			CreateIndexNoClose(!optimize, "index1", writer);
			writer.Flush(false, true, true);
			
			// create a 2nd index
			Directory dir2 = new MockRAMDirectory();
			IndexWriter writer2 = new IndexWriter(dir2, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer2.SetInfoStream(infoStream);
			CreateIndexNoClose(!optimize, "index2", writer2);
			writer2.Close();
			
			IndexReader r0 = writer.GetReader();
			Assert.IsTrue(r0.IsCurrent());
			writer.AddIndexesNoOptimize(new Directory[]{dir2});
			Assert.IsFalse(r0.IsCurrent());
			r0.Close();
			
			IndexReader r1 = writer.GetReader();
			Assert.IsTrue(r1.IsCurrent());
			
			writer.Commit();
			Assert.IsTrue(r1.IsCurrent());
			
			Assert.AreEqual(200, r1.MaxDoc());
			
			int index2df = r1.DocFreq(new Term("indexname", "index2"));
			
			Assert.AreEqual(100, index2df);
			
			// verify the docs are from different indexes
			Document doc5 = r1.Document(5);
			Assert.AreEqual("index1", doc5.Get("indexname"));
			Document doc150 = r1.Document(150);
			Assert.AreEqual("index2", doc150.Get("indexname"));
			r1.Close();
			writer.Close();
			dir1.Close();
		}
		
		[Test]
		public virtual void  TestAddIndexes2()
		{
			bool optimize = false;
			
			Directory dir1 = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetInfoStream(infoStream);
			
			// create a 2nd index
			Directory dir2 = new MockRAMDirectory();
			IndexWriter writer2 = new IndexWriter(dir2, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer2.SetInfoStream(infoStream);
			CreateIndexNoClose(!optimize, "index2", writer2);
			writer2.Close();
			
			writer.AddIndexesNoOptimize(new Directory[]{dir2});
			writer.AddIndexesNoOptimize(new Directory[]{dir2});
			writer.AddIndexesNoOptimize(new Directory[]{dir2});
			writer.AddIndexesNoOptimize(new Directory[]{dir2});
			writer.AddIndexesNoOptimize(new Directory[]{dir2});
			
			IndexReader r1 = writer.GetReader();
			Assert.AreEqual(500, r1.MaxDoc());
			
			r1.Close();
			writer.Close();
			dir1.Close();
		}
		
		/// <summary> Deletes using IW.deleteDocuments
		/// 
		/// </summary>
		/// <throws>  Exception </throws>
		[Test]
		public virtual void  TestDeleteFromIndexWriter()
		{
			bool optimize = true;
			
			Directory dir1 = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetInfoStream(infoStream);
			// create the index
			CreateIndexNoClose(!optimize, "index1", writer);
			writer.Flush(false, true, true);
			// get a reader
			IndexReader r1 = writer.GetReader();
			
			System.String id10 = r1.Document(10).GetField("id").StringValue();
			
			// deleted IW docs should not show up in the next getReader
			writer.DeleteDocuments(new Term("id", id10));
			IndexReader r2 = writer.GetReader();
			Assert.AreEqual(1, Count(new Term("id", id10), r1));
			Assert.AreEqual(0, Count(new Term("id", id10), r2));
			
			System.String id50 = r1.Document(50).GetField("id").StringValue();
			Assert.AreEqual(1, Count(new Term("id", id50), r1));
			
			writer.DeleteDocuments(new Term("id", id50));
			
			IndexReader r3 = writer.GetReader();
			Assert.AreEqual(0, Count(new Term("id", id10), r3));
			Assert.AreEqual(0, Count(new Term("id", id50), r3));
			
			System.String id75 = r1.Document(75).GetField("id").StringValue();
			writer.DeleteDocuments(new TermQuery(new Term("id", id75)));
			IndexReader r4 = writer.GetReader();
			Assert.AreEqual(1, Count(new Term("id", id75), r3));
			Assert.AreEqual(0, Count(new Term("id", id75), r4));
			
			r1.Close();
			r2.Close();
			r3.Close();
			r4.Close();
			writer.Close();
			
			// reopen the writer to verify the delete made it to the directory
			writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetInfoStream(infoStream);
			IndexReader w2r1 = writer.GetReader();
			Assert.AreEqual(0, Count(new Term("id", id10), w2r1));
			w2r1.Close();
			writer.Close();
			dir1.Close();
		}
		
		[Test]
		public virtual void  TestAddIndexesAndDoDeletesThreads()
		{
			int numIter = 5;
			int numDirs = 3;
			
			Directory mainDir = new MockRAMDirectory();
			IndexWriter mainWriter = new IndexWriter(mainDir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			mainWriter.SetInfoStream(infoStream);
			AddDirectoriesThreads addDirThreads = new AddDirectoriesThreads(this, numIter, mainWriter);
			addDirThreads.LaunchThreads(numDirs);
			addDirThreads.JoinThreads();
			
			//Assert.AreEqual(100 + numDirs * (3 * numIter / 4) * addDirThreads.NUM_THREADS
			//    * addDirThreads.NUM_INIT_DOCS, addDirThreads.mainWriter.numDocs());
			Assert.AreEqual(addDirThreads.count.IntValue(), addDirThreads.mainWriter.NumDocs());
			
			addDirThreads.Close(true);
			
			Assert.IsTrue(addDirThreads.failures.Count == 0);
			
			_TestUtil.CheckIndex(mainDir);
			
			IndexReader reader = IndexReader.Open(mainDir);
			Assert.AreEqual(addDirThreads.count.IntValue(), reader.NumDocs());
			//Assert.AreEqual(100 + numDirs * (3 * numIter / 4) * addDirThreads.NUM_THREADS
			//    * addDirThreads.NUM_INIT_DOCS, reader.numDocs());
			reader.Close();
			
			addDirThreads.CloseDir();
			mainDir.Close();
		}
		
		private class DeleteThreads
		{
			private class AnonymousClassThread2:SupportClass.ThreadClass
			{
				public AnonymousClassThread2(DeleteThreads enclosingInstance)
				{
					InitBlock(enclosingInstance);
				}
				private void  InitBlock(DeleteThreads enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
				}
				private DeleteThreads enclosingInstance;
				public DeleteThreads Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				override public void  Run()
				{
					try
					{
						Term term = Enclosing_Instance.GetDeleteTerm();
						Enclosing_Instance.mainWriter.DeleteDocuments(term);
						lock (Enclosing_Instance.deletedTerms.SyncRoot)
						{
							Enclosing_Instance.deletedTerms.Add(term);
						}
					}
					catch (System.Exception t)
					{
						Enclosing_Instance.Handle(t);
					}
				}
			}
			private void  InitBlock(TestIndexWriterReader enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
				threads = new SupportClass.ThreadClass[NUM_THREADS];
			}
			private TestIndexWriterReader enclosingInstance;
			public TestIndexWriterReader Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal const int NUM_THREADS = 5;
			internal SupportClass.ThreadClass[] threads;
			internal IndexWriter mainWriter;
			internal System.Collections.IList deletedTerms = new System.Collections.ArrayList();
			internal System.Collections.ArrayList toDeleteTerms = new System.Collections.ArrayList();
			internal System.Random random;
			internal System.Collections.IList failures = new System.Collections.ArrayList();
			
			public DeleteThreads(TestIndexWriterReader enclosingInstance, IndexWriter mainWriter)
			{
				InitBlock(enclosingInstance);
				this.mainWriter = mainWriter;
				IndexReader reader = mainWriter.GetReader();
				int maxDoc = reader.MaxDoc();
				random = Enclosing_Instance.NewRandom();
				int iter = random.Next(maxDoc);
				for (int x = 0; x < iter; x++)
				{
					int doc = random.Next(iter);
					System.String id = reader.Document(doc).Get("id");
					toDeleteTerms.Add(new Term("id", id));
				}
			}
			
			internal virtual Term GetDeleteTerm()
			{
				lock (toDeleteTerms.SyncRoot)
				{
					System.Object tempObject;
					tempObject = toDeleteTerms[0];
					toDeleteTerms.RemoveAt(0);
					return (Term) tempObject;
				}
			}
			
			internal virtual void  LaunchThreads(int numIter)
			{
				for (int i = 0; i < NUM_THREADS; i++)
				{
					threads[i] = new AnonymousClassThread2(this);
				}
			}
			
			internal virtual void  Handle(System.Exception t)
			{
				System.Console.Out.WriteLine(t.StackTrace);
				lock (failures.SyncRoot)
				{
					failures.Add(t);
				}
			}
			
			internal virtual void  JoinThreads()
			{
				for (int i = 0; i < NUM_THREADS; i++)
					try
					{
						threads[i].Join();
					}
					catch (System.Threading.ThreadInterruptedException ie)
					{
						SupportClass.ThreadClass.Current().Interrupt();
					}
			}
		}
		
		private class AddDirectoriesThreads
		{
			private class AnonymousClassThread2:SupportClass.ThreadClass
			{
				public AnonymousClassThread2(int numIter, AddDirectoriesThreads enclosingInstance)
				{
					InitBlock(numIter, enclosingInstance);
				}
				private void  InitBlock(int numIter, AddDirectoriesThreads enclosingInstance)
				{
					this.numIter = numIter;
					this.enclosingInstance = enclosingInstance;
				}
				private int numIter;
				private AddDirectoriesThreads enclosingInstance;
				public AddDirectoriesThreads Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				override public void  Run()
				{
					try
					{
						Directory[] dirs = new Directory[Enclosing_Instance.numDirs];
						for (int k = 0; k < Enclosing_Instance.numDirs; k++)
							dirs[k] = new MockRAMDirectory(Enclosing_Instance.addDir);
						//int j = 0;
						//while (true) {
						// System.out.println(Thread.currentThread().getName() + ": iter
						// j=" + j);
						for (int x = 0; x < numIter; x++)
						{
							// only do addIndexesNoOptimize
							Enclosing_Instance.DoBody(x, dirs);
						}
						//if (numIter > 0 && j == numIter)
						//  break;
						//doBody(j++, dirs);
						//doBody(5, dirs);
						//}
					}
					catch (System.Exception t)
					{
						Enclosing_Instance.Handle(t);
					}
				}
			}
			private void  InitBlock(TestIndexWriterReader enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
				threads = new SupportClass.ThreadClass[NUM_THREADS];
			}
			private TestIndexWriterReader enclosingInstance;
			public TestIndexWriterReader Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal Directory addDir;
			internal const int NUM_THREADS = 5;
			internal const int NUM_INIT_DOCS = 100;
			internal int numDirs;
			internal SupportClass.ThreadClass[] threads;
			internal IndexWriter mainWriter;
			internal System.Collections.IList failures = new System.Collections.ArrayList();
			internal IndexReader[] readers;
			internal bool didClose = false;
			internal HeavyAtomicInt count = new HeavyAtomicInt(0);
			internal HeavyAtomicInt numAddIndexesNoOptimize = new HeavyAtomicInt(0);
			
			public AddDirectoriesThreads(TestIndexWriterReader enclosingInstance, int numDirs, IndexWriter mainWriter)
			{
				InitBlock(enclosingInstance);
				this.numDirs = numDirs;
				this.mainWriter = mainWriter;
				addDir = new MockRAMDirectory();
				IndexWriter writer = new IndexWriter(addDir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
				writer.SetMaxBufferedDocs(2);
				for (int i = 0; i < NUM_INIT_DOCS; i++)
				{
					Document doc = Lucene.Net.Index.TestIndexWriterReader.CreateDocument(i, "addindex", 4);
					writer.AddDocument(doc);
				}
				
				writer.Close();
				
				readers = new IndexReader[numDirs];
				for (int i = 0; i < numDirs; i++)
					readers[i] = IndexReader.Open(addDir);
			}
			
			internal virtual void  JoinThreads()
			{
				for (int i = 0; i < NUM_THREADS; i++)
					try
					{
						threads[i].Join();
					}
					catch (System.Threading.ThreadInterruptedException ie)
					{
						SupportClass.ThreadClass.Current().Interrupt();
					}
			}
			
			internal virtual void  Close(bool doWait)
			{
				didClose = true;
				mainWriter.Close(doWait);
			}
			
			internal virtual void  CloseDir()
			{
				for (int i = 0; i < numDirs; i++)
					readers[i].Close();
				addDir.Close();
			}
			
			internal virtual void  Handle(System.Exception t)
			{
				System.Console.Out.WriteLine(t.StackTrace);
				lock (failures.SyncRoot)
				{
					failures.Add(t);
				}
			}
			
			internal virtual void  LaunchThreads(int numIter)
			{
				for (int i = 0; i < NUM_THREADS; i++)
				{
					threads[i] = new AnonymousClassThread2(numIter, this);
				}
				for (int i = 0; i < NUM_THREADS; i++)
					threads[i].Start();
			}
			
			internal virtual void  DoBody(int j, Directory[] dirs)
			{
				switch (j % 4)
				{
					
					case 0: 
						mainWriter.AddIndexes(dirs);
						break;
					
					case 1: 
						mainWriter.AddIndexesNoOptimize(dirs);
						numAddIndexesNoOptimize.IncrementAndGet();
						break;
					
					case 2: 
						mainWriter.AddIndexes(readers);
						break;
					
					case 3: 
						mainWriter.Commit();
						break;
					}
				count.AddAndGet(dirs.Length * NUM_INIT_DOCS);
			}
		}
		
		[Test]
		public virtual void  TestIndexWriterReopenSegmentOptimize()
		{
			DoTestIndexWriterReopenSegment(true);
		}
		
		[Test]
		public virtual void  TestIndexWriterReopenSegment()
		{
			DoTestIndexWriterReopenSegment(false);
		}
		
		/// <summary> Tests creating a segment, then check to insure the segment can be seen via
		/// IW.getReader
		/// </summary>
		public virtual void  DoTestIndexWriterReopenSegment(bool optimize)
		{
			Directory dir1 = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetInfoStream(infoStream);
			IndexReader r1 = writer.GetReader();
			Assert.AreEqual(0, r1.MaxDoc());
			CreateIndexNoClose(false, "index1", writer);
			writer.Flush(!optimize, true, true);
			
			IndexReader iwr1 = writer.GetReader();
			Assert.AreEqual(100, iwr1.MaxDoc());
			
			IndexReader r2 = writer.GetReader();
			Assert.AreEqual(r2.MaxDoc(), 100);
			// add 100 documents
			for (int x = 10000; x < 10000 + 100; x++)
			{
				Document d = CreateDocument(x, "index1", 5);
				writer.AddDocument(d);
			}
			writer.Flush(false, true, true);
			// verify the reader was reopened internally
			IndexReader iwr2 = writer.GetReader();
			Assert.IsTrue(iwr2 != r1);
			Assert.AreEqual(200, iwr2.MaxDoc());
			// should have flushed out a segment
			IndexReader r3 = writer.GetReader();
			Assert.IsTrue(r2 != r3);
			Assert.AreEqual(200, r3.MaxDoc());
			
			// dec ref the readers rather than close them because
			// closing flushes changes to the writer
			r1.Close();
			iwr1.Close();
			r2.Close();
			r3.Close();
			iwr2.Close();
			writer.Close();
			
			// test whether the changes made it to the directory
			writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			IndexReader w2r1 = writer.GetReader();
			// insure the deletes were actually flushed to the directory
			Assert.AreEqual(200, w2r1.MaxDoc());
			w2r1.Close();
			writer.Close();
			
			dir1.Close();
		}
		
		
		public static Document CreateDocument(int n, System.String indexName, int numFields)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			Document doc = new Document();
			doc.Add(new Field("id", System.Convert.ToString(n), Field.Store.YES, Field.Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
			doc.Add(new Field("indexname", indexName, Field.Store.YES, Field.Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
			sb.Append("a");
			sb.Append(n);
			doc.Add(new Field("field1", sb.ToString(), Field.Store.YES, Field.Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
			sb.Append(" b");
			sb.Append(n);
			for (int i = 1; i < numFields; i++)
			{
				doc.Add(new Field("field" + (i + 1), sb.ToString(), Field.Store.YES, Field.Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
			}
			return doc;
		}
		
		/// <summary> Delete a document by term and return the doc id
		/// 
		/// </summary>
		/// <returns>
		/// 
		/// public static int deleteDocument(Term term, IndexWriter writer) throws
		/// IOException { IndexReader reader = writer.getReader(); TermDocs td =
		/// reader.termDocs(term); int doc = -1; //if (td.next()) { // doc = td.doc();
		/// //} //writer.deleteDocuments(term); td.close(); return doc; }
		/// </returns>
		public static void  CreateIndex(Directory dir1, System.String indexName, bool multiSegment)
		{
			IndexWriter w = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			w.SetMergePolicy(new LogDocMergePolicy(w));
			for (int i = 0; i < 100; i++)
			{
				w.AddDocument(CreateDocument(i, indexName, 4));
				if (multiSegment && (i % 10) == 0)
				{
				}
			}
			if (!multiSegment)
			{
				w.Optimize();
			}
			w.Close();
		}
		
		public static void  CreateIndexNoClose(bool multiSegment, System.String indexName, IndexWriter w)
		{
			for (int i = 0; i < 100; i++)
			{
				w.AddDocument(CreateDocument(i, indexName, 4));
			}
			if (!multiSegment)
			{
				w.Optimize();
			}
		}
		
		private class MyWarmer:IndexWriter.IndexReaderWarmer
		{
			internal int warmCount;
			public override void  Warm(IndexReader reader)
			{
				warmCount++;
			}
		}
		
		[Test]
		public virtual void  TestMergeWarmer()
		{
			
			Directory dir1 = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetInfoStream(infoStream);
			
			// create the index
			CreateIndexNoClose(false, "test", writer);
			
			// get a reader to put writer into near real-time mode
			IndexReader r1 = writer.GetReader();
			
			// Enroll warmer
			MyWarmer warmer = new MyWarmer();
			writer.SetMergedSegmentWarmer(warmer);
			writer.SetMergeFactor(2);
			writer.SetMaxBufferedDocs(2);
			
			for (int i = 0; i < 10; i++)
			{
				writer.AddDocument(CreateDocument(i, "test", 4));
			}
			((ConcurrentMergeScheduler) writer.GetMergeScheduler()).Sync();
			
			Assert.IsTrue(warmer.warmCount > 0);
			int count = warmer.warmCount;
			
			writer.AddDocument(CreateDocument(17, "test", 4));
			writer.Optimize();
			Assert.IsTrue(warmer.warmCount > count);
			
			writer.Close();
			r1.Close();
			dir1.Close();
		}
		
		[Test]
		public virtual void  TestAfterCommit()
		{
			Directory dir1 = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetInfoStream(infoStream);
			
			// create the index
			CreateIndexNoClose(false, "test", writer);
			
			// get a reader to put writer into near real-time mode
			IndexReader r1 = writer.GetReader();
			_TestUtil.CheckIndex(dir1);
			writer.Commit();
			_TestUtil.CheckIndex(dir1);
			Assert.AreEqual(100, r1.NumDocs());
			
			for (int i = 0; i < 10; i++)
			{
				writer.AddDocument(CreateDocument(i, "test", 4));
			}
			((ConcurrentMergeScheduler) writer.GetMergeScheduler()).Sync();
			
			IndexReader r2 = r1.Reopen();
			if (r2 != r1)
			{
				r1.Close();
				r1 = r2;
			}
			Assert.AreEqual(110, r1.NumDocs());
			writer.Close();
			r1.Close();
			dir1.Close();
		}
		
		// Make sure reader remains usable even if IndexWriter closes
		[Test]
		public virtual void  TestAfterClose()
		{
			Directory dir1 = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetInfoStream(infoStream);
			
			// create the index
			CreateIndexNoClose(false, "test", writer);
			
			IndexReader r = writer.GetReader();
			writer.Close();
			
			_TestUtil.CheckIndex(dir1);
			
			// reader should remain usable even after IndexWriter is closed:
			Assert.AreEqual(100, r.NumDocs());
			Query q = new TermQuery(new Term("indexname", "test"));
			Assert.AreEqual(100, new IndexSearcher(r).Search(q, 10).totalHits);
			
			try
			{
				r.Reopen();
				Assert.Fail("failed to hit AlreadyClosedException");
			}
			catch (AlreadyClosedException ace)
			{
				// expected
			}
			r.Close();
			dir1.Close();
		}
		
		// Stress test reopen during addIndexes
		[Test]
		public virtual void  TestDuringAddIndexes()
		{
			Directory dir1 = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetInfoStream(infoStream);
			writer.SetMergeFactor(2);
			
			// create the index
			CreateIndexNoClose(false, "test", writer);
			writer.Commit();
			
			Directory[] dirs = new Directory[10];
			for (int i = 0; i < 10; i++)
			{
				dirs[i] = new MockRAMDirectory(dir1);
			}
			
			IndexReader r = writer.GetReader();
			
			int NUM_THREAD = 5;
			float SECONDS = 3;
			
			long endTime = (long) ((DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) + 1000.0 * SECONDS);
			System.Collections.IList excs = (System.Collections.IList) System.Collections.ArrayList.Synchronized(new System.Collections.ArrayList(new System.Collections.ArrayList()));
			
			SupportClass.ThreadClass[] threads = new SupportClass.ThreadClass[NUM_THREAD];
			for (int i = 0; i < NUM_THREAD; i++)
			{
				threads[i] = new AnonymousClassThread(endTime, writer, dirs, excs, this);
				threads[i].IsBackground = true;
				threads[i].Start();
			}
			
			int lastCount = 0;
			while ((DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) < endTime)
			{
				IndexReader r2 = r.Reopen();
				if (r2 != r)
				{
					r.Close();
					r = r2;
				}
				Query q = new TermQuery(new Term("indexname", "test"));
				int count = new IndexSearcher(r).Search(q, 10).totalHits;
				Assert.IsTrue(count >= lastCount);
				lastCount = count;
			}
			
			for (int i = 0; i < NUM_THREAD; i++)
			{
				threads[i].Join();
			}
			
			Assert.AreEqual(0, excs.Count);
			writer.Close();
			
			_TestUtil.CheckIndex(dir1);
			r.Close();
			dir1.Close();
		}
		
		// Stress test reopen during add/delete
		[Test]
		public virtual void  TestDuringAddDelete()
		{
			Directory dir1 = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetInfoStream(infoStream);
			writer.SetMergeFactor(2);
			
			// create the index
			CreateIndexNoClose(false, "test", writer);
			writer.Commit();
			
			IndexReader r = writer.GetReader();
			
			int NUM_THREAD = 5;
			float SECONDS = 3;
			
			long endTime = (long) ((DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) + 1000.0 * SECONDS);
			System.Collections.IList excs = (System.Collections.IList) System.Collections.ArrayList.Synchronized(new System.Collections.ArrayList(new System.Collections.ArrayList()));
			
			SupportClass.ThreadClass[] threads = new SupportClass.ThreadClass[NUM_THREAD];
			for (int i = 0; i < NUM_THREAD; i++)
			{
				threads[i] = new AnonymousClassThread1(endTime, writer, excs, this);
				threads[i].IsBackground = true;
				threads[i].Start();
			}
			
			int sum = 0;
			while ((DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) < endTime)
			{
				IndexReader r2 = r.Reopen();
				if (r2 != r)
				{
					r.Close();
					r = r2;
				}
				Query q = new TermQuery(new Term("indexname", "test"));
				sum += new IndexSearcher(r).Search(q, 10).totalHits;
			}
			
			for (int i = 0; i < NUM_THREAD; i++)
			{
				threads[i].Join();
			}
			Assert.IsTrue(sum > 0);
			
			Assert.AreEqual(0, excs.Count);
			writer.Close();
			
			_TestUtil.CheckIndex(dir1);
			r.Close();
			dir1.Close();
		}
		
		[Test]
		public virtual void  TestExpungeDeletes()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "a b c", Field.Store.NO, Field.Index.ANALYZED));
			Field id = new Field("id", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
			doc.Add(id);
			id.SetValue("0");
			w.AddDocument(doc);
			id.SetValue("1");
			w.AddDocument(doc);
			w.DeleteDocuments(new Term("id", "0"));
			
			IndexReader r = w.GetReader();
			w.ExpungeDeletes();
			w.Close();
			r.Close();
			r = IndexReader.Open(dir);
			Assert.AreEqual(1, r.NumDocs());
			Assert.IsFalse(r.HasDeletions());
			r.Close();
			dir.Close();
		}

        [Test]
        public void TestDeletesNumDocs()
        {
            Directory dir = new MockRAMDirectory();
            IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(),
                                                       IndexWriter.MaxFieldLength.LIMITED);
            Document doc = new Document();
            doc.Add(new Field("field", "a b c", Field.Store.NO, Field.Index.ANALYZED));
            Field id = new Field("id", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
            doc.Add(id);
            id.SetValue("0");
            w.AddDocument(doc);
            id.SetValue("1");
            w.AddDocument(doc);
            IndexReader r = w.GetReader();
            Assert.AreEqual(2, r.NumDocs());
            r.Close();

            w.DeleteDocuments(new Term("id", "0"));
            r = w.GetReader();
            Assert.AreEqual(1, r.NumDocs());
            r.Close();

            w.DeleteDocuments(new Term("id", "1"));
            r = w.GetReader();
            Assert.AreEqual(0, r.NumDocs());
            r.Close();

            w.Close();
            dir.Close();
        }
	}
}