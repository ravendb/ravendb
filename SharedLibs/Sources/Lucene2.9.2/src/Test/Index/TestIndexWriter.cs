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
using CachingTokenFilter = Lucene.Net.Analysis.CachingTokenFilter;
using SimpleAnalyzer = Lucene.Net.Analysis.SimpleAnalyzer;
using StopAnalyzer = Lucene.Net.Analysis.StopAnalyzer;
using TeeSinkTokenFilter = Lucene.Net.Analysis.TeeSinkTokenFilter;
using TokenFilter = Lucene.Net.Analysis.TokenFilter;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using WhitespaceTokenizer = Lucene.Net.Analysis.WhitespaceTokenizer;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using StandardTokenizer = Lucene.Net.Analysis.Standard.StandardTokenizer;
using PositionIncrementAttribute = Lucene.Net.Analysis.Tokenattributes.PositionIncrementAttribute;
using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using AlreadyClosedException = Lucene.Net.Store.AlreadyClosedException;
using Directory = Lucene.Net.Store.Directory;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using IndexInput = Lucene.Net.Store.IndexInput;
using IndexOutput = Lucene.Net.Store.IndexOutput;
using Lock = Lucene.Net.Store.Lock;
using LockFactory = Lucene.Net.Store.LockFactory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using SingleInstanceLockFactory = Lucene.Net.Store.SingleInstanceLockFactory;
using UnicodeUtil = Lucene.Net.Util.UnicodeUtil;
using BaseTokenStreamTestCase = Lucene.Net.Analysis.BaseTokenStreamTestCase;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using PhraseQuery = Lucene.Net.Search.PhraseQuery;
using Query = Lucene.Net.Search.Query;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using TermQuery = Lucene.Net.Search.TermQuery;
using SpanTermQuery = Lucene.Net.Search.Spans.SpanTermQuery;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Index
{
	
	/// <summary> </summary>
	/// <version>  $Id: TestIndexWriter.java 831036 2009-10-29 17:15:12Z mikemccand $
	/// </version>
    [TestFixture]
	public class TestIndexWriter:BaseTokenStreamTestCase
	{
        internal static System.Collections.Hashtable testWithNewApiData = new System.Collections.Hashtable();
        static TestIndexWriter()
        {
            System.String[] data = new System.String[] {"TestExceptionFromTokenStream", "TestDocumentsWriterExceptions", "TestNegativePositions", "TestEndOffsetPositionWithCachingTokenFilter", "TestEndOffsetPositionWithTeeSinkTokenFilter", "TestEndOffsetPositionStandard", "TestEndOffsetPositionStandardEmptyField", "TestEndOffsetPositionStandardEmptyField2"};
            for (int i = 0; i < data.Length; i++)
            {
                testWithNewApiData.Add(data[i], data[i]);
            }
        }

		public class MyRAMDirectory:RAMDirectory
		{
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private LockFactory myLockFactory;
			internal MyRAMDirectory(TestIndexWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
				lockFactory = null;
				myLockFactory = new SingleInstanceLockFactory();
			}
			public override Lock MakeLock(System.String name)
			{
				return myLockFactory.MakeLock(name);
			}
		}
		private class AnonymousClassAnalyzer : Analyzer
		{
			public AnonymousClassAnalyzer(TestIndexWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private class AnonymousClassTokenFilter : TokenFilter
			{
				public AnonymousClassTokenFilter(AnonymousClassAnalyzer enclosingInstance, StandardTokenizer standardTokenizer) : base(standardTokenizer)
				{
					InitBlock(enclosingInstance);
				}
				private void  InitBlock(AnonymousClassAnalyzer enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
				}
				private AnonymousClassAnalyzer enclosingInstance;
				public AnonymousClassAnalyzer Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				private int count = 0;
				
				public override bool IncrementToken()
				{
					if (count++ == 5)
					{
						throw new System.IO.IOException();
					}
					return input.IncrementToken();
				}
			}
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public override /*virtual*/ TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				return new AnonymousClassTokenFilter(this, new StandardTokenizer(reader));
			}
		}
		private class AnonymousClassAnalyzer1 : Analyzer
		{
			public AnonymousClassAnalyzer1(TestIndexWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override /*virtual*/ TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				return new CrashingFilter(this.enclosingInstance, fieldName, new WhitespaceTokenizer(reader));
			}
		}
		private class AnonymousClassAnalyzer2 : Analyzer
		{
			public AnonymousClassAnalyzer2(TestIndexWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override /*virtual*/ TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
                return new CrashingFilter(this.enclosingInstance, fieldName, new WhitespaceTokenizer(reader));
			}
		}
		private class AnonymousClassThread:SupportClass.ThreadClass
		{
			public AnonymousClassThread(int NUM_ITER, IndexWriter writer, int finalI, TestIndexWriter enclosingInstance)
			{
				InitBlock(NUM_ITER, writer, finalI, enclosingInstance);
			}
			private void  InitBlock(int NUM_ITER, IndexWriter writer, int finalI, TestIndexWriter enclosingInstance)
			{
				this.NUM_ITER = NUM_ITER;
				this.writer = writer;
				this.finalI = finalI;
				this.enclosingInstance = enclosingInstance;
			}
			private int NUM_ITER;
			private IndexWriter writer;
			private int finalI;
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
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
					for (int iter = 0; iter < NUM_ITER; iter++)
					{
						Document doc = new Document();
						doc.Add(new Field("contents", "here are some contents", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
						writer.AddDocument(doc);
						writer.AddDocument(doc);
						doc.Add(new Field("crash", "this should crash after 4 terms", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
						doc.Add(new Field("other", "this will not get indexed", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
						try
						{
							writer.AddDocument(doc);
							Assert.Fail("did not hit expected exception");
						}
						catch (System.IO.IOException ioe)
						{
						}
						
						if (0 == finalI)
						{
							doc = new Document();
							doc.Add(new Field("contents", "here are some contents", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
							writer.AddDocument(doc);
							writer.AddDocument(doc);
						}
					}
				}
				catch (System.Exception t)
				{
					lock (this)
					{
						System.Console.Out.WriteLine(SupportClass.ThreadClass.Current().Name + ": ERROR: hit unexpected exception");
						System.Console.Out.WriteLine(t.StackTrace);
					}
					Assert.Fail();
				}
			}
		}
		private class AnonymousClassThread1:SupportClass.ThreadClass
		{
			public AnonymousClassThread1(IndexWriter finalWriter, Document doc, System.Collections.ArrayList failure, TestIndexWriter enclosingInstance)
			{
				InitBlock(finalWriter, doc, failure, enclosingInstance);
			}
			private void  InitBlock(IndexWriter finalWriter, Document doc, System.Collections.ArrayList failure, TestIndexWriter enclosingInstance)
			{
				this.finalWriter = finalWriter;
				this.doc = doc;
				this.failure = failure;
				this.enclosingInstance = enclosingInstance;
			}
			private IndexWriter finalWriter;
			private Document doc;
			private System.Collections.ArrayList failure;
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			override public void  Run()
			{
				bool done = false;
				while (!done)
				{
					for (int i = 0; i < 100; i++)
					{
						try
						{
							finalWriter.AddDocument(doc);
						}
						catch (AlreadyClosedException e)
						{
							done = true;
							break;
						}
						catch (System.NullReferenceException e)
						{
							done = true;
							break;
						}
						catch (System.Exception e)
						{
							System.Console.Out.WriteLine(e.StackTrace);
							failure.Add(e);
							done = true;
							break;
						}
					}
					System.Threading.Thread.Sleep(0);
				}
			}
		}
		private class AnonymousClassAnalyzer3 : Analyzer
		{
			public AnonymousClassAnalyzer3(TestIndexWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override /*virtual*/ TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				return new CrashingFilter(this.enclosingInstance, fieldName, new WhitespaceTokenizer(reader));
			}
		}
		private class AnonymousClassTokenStream : TokenStream
		{
			public AnonymousClassTokenStream(TestIndexWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
                this.termAtt = (TermAttribute)this.AddAttribute(typeof(TermAttribute));
                this.posIncrAtt = (PositionIncrementAttribute)this.AddAttribute(typeof(PositionIncrementAttribute));
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal TermAttribute termAtt;
			internal PositionIncrementAttribute posIncrAtt;
			
			internal System.Collections.IEnumerator tokens = new System.Collections.ArrayList(new System.String[]{"a", "b", "c"}).GetEnumerator();
			internal bool first = true;
			
			public override bool IncrementToken()
			{
				if (!tokens.MoveNext())
					return false;
				ClearAttributes();
				termAtt.SetTermBuffer((System.String) tokens.Current);
				posIncrAtt.SetPositionIncrement(first?0:1);
				first = false;
				return true;
			}
		}
		private class AnonymousClassIndexWriter : IndexWriter
		{
			public AnonymousClassIndexWriter(System.Collections.IList thrown, TestIndexWriter enclosingInstance, MockRAMDirectory mockRAMDir, StandardAnalyzer standardAnalyzer) : base(mockRAMDir, standardAnalyzer)
			{
				InitBlock(thrown, enclosingInstance);
			}
			private void  InitBlock(System.Collections.IList thrown, TestIndexWriter enclosingInstance)
			{
				this.thrown = thrown;
				this.enclosingInstance = enclosingInstance;
			}
			private System.Collections.IList thrown;
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override void  Message(System.String message)
			{
				if (message.StartsWith("now flush at close") && 0 == thrown.Count)
				{
					thrown.Add(null);
					throw new System.OutOfMemoryException("fake OOME at " + message);
				}
			}
		}
		public TestIndexWriter(System.String name):base(name, testWithNewApiData)
		{
		}

        public TestIndexWriter() : base("", testWithNewApiData)
        {
        }
		
        [Test]
		public virtual void  TestDocCount()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = null;
			IndexReader reader = null;
			int i;
			
			IndexWriter.SetDefaultWriteLockTimeout(2000);
			Assert.AreEqual(2000, IndexWriter.GetDefaultWriteLockTimeout());
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			
			IndexWriter.SetDefaultWriteLockTimeout(1000);
			
			// add 100 documents
			for (i = 0; i < 100; i++)
			{
				AddDoc(writer);
			}
			Assert.AreEqual(100, writer.DocCount());
			writer.Close();
			
			// delete 40 documents
			reader = IndexReader.Open(dir);
			for (i = 0; i < 40; i++)
			{
				reader.DeleteDocument(i);
			}
			reader.Close();
			
			// test doc count before segments are merged/index is optimized
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Assert.AreEqual(100, writer.DocCount());
			writer.Close();
			
			reader = IndexReader.Open(dir);
			Assert.AreEqual(100, reader.MaxDoc());
			Assert.AreEqual(60, reader.NumDocs());
			reader.Close();
			
			// optimize the index and check that the new doc count is correct
			writer = new IndexWriter(dir, true, new WhitespaceAnalyzer());
			Assert.AreEqual(100, writer.MaxDoc());
			Assert.AreEqual(60, writer.NumDocs());
			writer.Optimize();
			Assert.AreEqual(60, writer.MaxDoc());
			Assert.AreEqual(60, writer.NumDocs());
			writer.Close();
			
			// check that the index reader gives the same numbers.
			reader = IndexReader.Open(dir);
			Assert.AreEqual(60, reader.MaxDoc());
			Assert.AreEqual(60, reader.NumDocs());
			reader.Close();
			
			// make sure opening a new index for create over
			// this existing one works correctly:
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Assert.AreEqual(0, writer.MaxDoc());
			Assert.AreEqual(0, writer.NumDocs());
			writer.Close();
		}
		
		private static void  AddDoc(IndexWriter writer)
		{
			Document doc = new Document();
			doc.Add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
		}
		
		private void  AddDocWithIndex(IndexWriter writer, int index)
		{
			Document doc = new Document();
			doc.Add(new Field("content", "aaa " + index, Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("id", "" + index, Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
		}
		
		/*
		Test: make sure when we run out of disk space or hit
		random IOExceptions in any of the addIndexes(*) calls
		that 1) index is not corrupt (searcher can open/search
		it) and 2) transactional semantics are followed:
		either all or none of the incoming documents were in
		fact added.
		*/
        [Test]
		public virtual void  TestAddIndexOnDiskFull()
		{
			int START_COUNT = 57;
			int NUM_DIR = 50;
			int END_COUNT = START_COUNT + NUM_DIR * 25;
			
			bool debug = false;
			
			// Build up a bunch of dirs that have indexes which we
			// will then merge together by calling addIndexes(*):
			Directory[] dirs = new Directory[NUM_DIR];
			long inputDiskUsage = 0;
			for (int i = 0; i < NUM_DIR; i++)
			{
				dirs[i] = new RAMDirectory();
				IndexWriter writer = new IndexWriter(dirs[i], new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				for (int j = 0; j < 25; j++)
				{
					AddDocWithIndex(writer, 25 * i + j);
				}
				writer.Close();
				System.String[] files = dirs[i].ListAll();
				for (int j = 0; j < files.Length; j++)
				{
					inputDiskUsage += dirs[i].FileLength(files[j]);
				}
			}
			
			// Now, build a starting index that has START_COUNT docs.  We
			// will then try to addIndexes into a copy of this:
			RAMDirectory startDir = new RAMDirectory();
			IndexWriter writer2 = new IndexWriter(startDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int j = 0; j < START_COUNT; j++)
			{
				AddDocWithIndex(writer2, j);
			}
			writer2.Close();
			
			// Make sure starting index seems to be working properly:
			Term searchTerm = new Term("content", "aaa");
			IndexReader reader = IndexReader.Open(startDir);
			Assert.AreEqual(57, reader.DocFreq(searchTerm), "first docFreq");
			
			IndexSearcher searcher = new IndexSearcher(reader);
			ScoreDoc[] hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
			Assert.AreEqual(57, hits.Length, "first number of hits");
			searcher.Close();
			reader.Close();
			
			// Iterate with larger and larger amounts of free
			// disk space.  With little free disk space,
			// addIndexes will certainly run out of space &
			// fail.  Verify that when this happens, index is
			// not corrupt and index in fact has added no
			// documents.  Then, we increase disk space by 2000
			// bytes each iteration.  At some point there is
			// enough free disk space and addIndexes should
			// succeed and index should show all documents were
			// added.
			
			// String[] files = startDir.listAll();
			long diskUsage = startDir.SizeInBytes();
			
			long startDiskUsage = 0;
			System.String[] files2 = startDir.ListAll();
			for (int i = 0; i < files2.Length; i++)
			{
				startDiskUsage += startDir.FileLength(files2[i]);
			}
			
			for (int iter = 0; iter < 6; iter++)
			{
				
				if (debug)
					System.Console.Out.WriteLine("TEST: iter=" + iter);
				
				// Start with 100 bytes more than we are currently using:
				long diskFree = diskUsage + 100;
				
				bool autoCommit = iter % 2 == 0;
				int method = iter / 2;
				
				bool success = false;
				bool done = false;
				
				System.String methodName;
				if (0 == method)
				{
					methodName = "addIndexes(Directory[])";
				}
				else if (1 == method)
				{
					methodName = "addIndexes(IndexReader[])";
				}
				else
				{
					methodName = "addIndexesNoOptimize(Directory[])";
				}
				
				while (!done)
				{
					
					// Make a new dir that will enforce disk usage:
					MockRAMDirectory dir = new MockRAMDirectory(startDir);
					writer2 = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false);
					System.IO.IOException err = null;
					
					MergeScheduler ms = writer2.GetMergeScheduler();
					for (int x = 0; x < 2; x++)
					{
						if (ms is ConcurrentMergeScheduler)
						// This test intentionally produces exceptions
						// in the threads that CMS launches; we don't
						// want to pollute test output with these.
							if (0 == x)
								((ConcurrentMergeScheduler) ms).SetSuppressExceptions();
							else
								((ConcurrentMergeScheduler) ms).ClearSuppressExceptions();
						
						// Two loops: first time, limit disk space &
						// throw random IOExceptions; second time, no
						// disk space limit:
						
						double rate = 0.05;
						double diskRatio = ((double) diskFree) / diskUsage;
						long thisDiskFree;
						
						System.String testName = null;
						
						if (0 == x)
						{
							thisDiskFree = diskFree;
							if (diskRatio >= 2.0)
							{
								rate /= 2;
							}
							if (diskRatio >= 4.0)
							{
								rate /= 2;
							}
							if (diskRatio >= 6.0)
							{
								rate = 0.0;
							}
							if (debug)
								testName = "disk full test " + methodName + " with disk full at " + diskFree + " bytes autoCommit=" + autoCommit;
						}
						else
						{
							thisDiskFree = 0;
							rate = 0.0;
							if (debug)
								testName = "disk full test " + methodName + " with unlimited disk space autoCommit=" + autoCommit;
						}
						
						if (debug)
							System.Console.Out.WriteLine("\ncycle: " + testName);
						
						dir.SetMaxSizeInBytes(thisDiskFree);
						dir.SetRandomIOExceptionRate(rate, diskFree);
						
						try
						{
							
							if (0 == method)
							{
								writer2.AddIndexes(dirs);
							}
							else if (1 == method)
							{
								IndexReader[] readers = new IndexReader[dirs.Length];
								for (int i = 0; i < dirs.Length; i++)
								{
									readers[i] = IndexReader.Open(dirs[i]);
								}
								try
								{
									writer2.AddIndexes(readers);
								}
								finally
								{
									for (int i = 0; i < dirs.Length; i++)
									{
										readers[i].Close();
									}
								}
							}
							else
							{
								writer2.AddIndexesNoOptimize(dirs);
							}
							
							success = true;
							if (debug)
							{
								System.Console.Out.WriteLine("  success!");
							}
							
							if (0 == x)
							{
								done = true;
							}
						}
						catch (System.IO.IOException e)
						{
							success = false;
							err = e;
							if (debug)
							{
								System.Console.Out.WriteLine("  hit IOException: " + e);
								System.Console.Out.WriteLine(e.StackTrace);
							}
							
							if (1 == x)
							{
								System.Console.Out.WriteLine(e.StackTrace);
								Assert.Fail(methodName + " hit IOException after disk space was freed up");
							}
						}
						
						// Make sure all threads from
						// ConcurrentMergeScheduler are done
						_TestUtil.SyncConcurrentMerges(writer2);
						
						if (autoCommit)
						{
							
							// Whether we succeeded or failed, check that
							// all un-referenced files were in fact
							// deleted (ie, we did not create garbage).
							// Only check this when autoCommit is true:
							// when it's false, it's expected that there
							// are unreferenced files (ie they won't be
							// referenced until the "commit on close").
							// Just create a new IndexFileDeleter, have it
							// delete unreferenced files, then verify that
							// in fact no files were deleted:
							
							System.String successStr;
							if (success)
							{
								successStr = "success";
							}
							else
							{
								successStr = "IOException";
							}
							System.String message = methodName + " failed to delete unreferenced files after " + successStr + " (" + diskFree + " bytes)";
							AssertNoUnreferencedFiles(dir, message);
						}
						
						if (debug)
						{
							System.Console.Out.WriteLine("  now test readers");
						}
						
						// Finally, verify index is not corrupt, and, if
						// we succeeded, we see all docs added, and if we
						// failed, we see either all docs or no docs added
						// (transactional semantics):
						try
						{
							reader = IndexReader.Open(dir);
						}
						catch (System.IO.IOException e)
						{
							System.Console.Out.WriteLine(e.StackTrace);
							Assert.Fail(testName + ": exception when creating IndexReader: " + e);
						}
						int result = reader.DocFreq(searchTerm);
						if (success)
						{
							if (autoCommit && result != END_COUNT)
							{
								Assert.Fail(testName + ": method did not throw exception but docFreq('aaa') is " + result + " instead of expected " + END_COUNT);
							}
							else if (!autoCommit && result != START_COUNT)
							{
								Assert.Fail(testName + ": method did not throw exception but docFreq('aaa') is " + result + " instead of expected " + START_COUNT + " [autoCommit = false]");
							}
						}
						else
						{
							// On hitting exception we still may have added
							// all docs:
							if (result != START_COUNT && result != END_COUNT)
							{
								System.Console.Out.WriteLine(err.StackTrace);
								Assert.Fail(testName + ": method did throw exception but docFreq('aaa') is " + result + " instead of expected " + START_COUNT + " or " + END_COUNT);
							}
						}
						
						searcher = new IndexSearcher(reader);
						try
						{
							hits = searcher.Search(new TermQuery(searchTerm), null, END_COUNT).scoreDocs;
						}
						catch (System.IO.IOException e)
						{
							System.Console.Out.WriteLine(e.StackTrace);
							Assert.Fail(testName + ": exception when searching: " + e);
						}
						int result2 = hits.Length;
						if (success)
						{
							if (result2 != result)
							{
								Assert.Fail(testName + ": method did not throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + result);
							}
						}
						else
						{
							// On hitting exception we still may have added
							// all docs:
							if (result2 != result)
							{
								System.Console.Out.WriteLine(err.StackTrace);
								Assert.Fail(testName + ": method did throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + result);
							}
						}
						
						searcher.Close();
						reader.Close();
						if (debug)
						{
							System.Console.Out.WriteLine("  count is " + result);
						}
						
						if (done || result == END_COUNT)
						{
							break;
						}
					}
					
					if (debug)
					{
						System.Console.Out.WriteLine("  start disk = " + startDiskUsage + "; input disk = " + inputDiskUsage + "; max used = " + dir.GetMaxUsedSizeInBytes());
					}
					
					if (done)
					{
						// Javadocs state that temp free Directory space
						// required is at most 2X total input size of
						// indices so let's make sure:
						Assert.IsTrue((dir.GetMaxUsedSizeInBytes() - startDiskUsage) < 2 * (startDiskUsage + inputDiskUsage), "max free Directory space required exceeded 1X the total input index sizes during " + methodName + ": max temp usage = " + (dir.GetMaxUsedSizeInBytes() - startDiskUsage) + " bytes; " + "starting disk usage = " + startDiskUsage + " bytes; " + "input index disk usage = " + inputDiskUsage + " bytes");
					}
					
					// Make sure we don't hit disk full during close below:
					dir.SetMaxSizeInBytes(0);
					dir.SetRandomIOExceptionRate(0.0, 0);
					
					writer2.Close();
					
					// Wait for all BG threads to finish else
					// dir.close() will throw IOException because
					// there are still open files
					_TestUtil.SyncConcurrentMerges(ms);
					
					dir.Close();
					
					// Try again with 2000 more bytes of free space:
					diskFree += 2000;
				}
			}
			
			startDir.Close();
		}
		
		/*
		* Make sure IndexWriter cleans up on hitting a disk
		* full exception in addDocument.
		*/
        [Test]
		public virtual void  TestAddDocumentOnDiskFull()
		{
			
			bool debug = false;
			
			for (int pass = 0; pass < 3; pass++)
			{
				if (debug)
					System.Console.Out.WriteLine("TEST: pass=" + pass);
				bool autoCommit = pass == 0;
				bool doAbort = pass == 2;
				long diskFree = 200;
				while (true)
				{
					if (debug)
						System.Console.Out.WriteLine("TEST: cycle: diskFree=" + diskFree);
					MockRAMDirectory dir = new MockRAMDirectory();
					dir.SetMaxSizeInBytes(diskFree);
					IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true);
					
					MergeScheduler ms = writer.GetMergeScheduler();
					if (ms is ConcurrentMergeScheduler)
					// This test intentionally produces exceptions
					// in the threads that CMS launches; we don't
					// want to pollute test output with these.
						((ConcurrentMergeScheduler) ms).SetSuppressExceptions();
					
					bool hitError = false;
					try
					{
						for (int i = 0; i < 200; i++)
						{
							AddDoc(writer);
						}
					}
					catch (System.IO.IOException e)
					{
						if (debug)
						{
							System.Console.Out.WriteLine("TEST: exception on addDoc");
							System.Console.Out.WriteLine(e.StackTrace);
						}
						hitError = true;
					}
					
					if (hitError)
					{
						if (doAbort)
						{
							writer.Abort();
						}
						else
						{
							try
							{
								writer.Close();
							}
							catch (System.IO.IOException e)
							{
								if (debug)
								{
									System.Console.Out.WriteLine("TEST: exception on close");
									System.Console.Out.WriteLine(e.StackTrace);
								}
								dir.SetMaxSizeInBytes(0);
								writer.Close();
							}
						}
						
						_TestUtil.SyncConcurrentMerges(ms);
						
						AssertNoUnreferencedFiles(dir, "after disk full during addDocument with autoCommit=" + autoCommit);
						
						// Make sure reader can open the index:
						IndexReader.Open(dir).Close();
						
						dir.Close();
						
						// Now try again w/ more space:
						diskFree += 500;
					}
					else
					{
						_TestUtil.SyncConcurrentMerges(writer);
						dir.Close();
						break;
					}
				}
			}
		}
		
		public static void  AssertNoUnreferencedFiles(Directory dir, System.String message)
		{
			System.String[] startFiles = dir.ListAll();
			SegmentInfos infos = new SegmentInfos();
			infos.Read(dir);
			new IndexFileDeleter(dir, new KeepOnlyLastCommitDeletionPolicy(), infos, null, null);
			System.String[] endFiles = dir.ListAll();
			
			System.Array.Sort(startFiles);
			System.Array.Sort(endFiles);
			
			if (!SupportClass.CollectionsHelper.Equals(startFiles, endFiles))
			{
				Assert.Fail(message + ": before delete:\n    " + ArrayToString(startFiles) + "\n  after delete:\n    " + ArrayToString(endFiles));
			}
		}
		
		/// <summary> Make sure we skip wicked long terms.</summary>
        [Test]
		public virtual void  TestWickedLongTerm()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			char[] chars = new char[DocumentsWriter.CHAR_BLOCK_SIZE_ForNUnit - 1];
			SupportClass.CollectionsHelper.Fill(chars, 'x');
			Document doc = new Document();
			System.String bigTerm = new System.String(chars);
			
			// Max length term is 16383, so this contents produces
			// a too-long term:
			System.String contents = "abc xyz x" + bigTerm + " another term";
			doc.Add(new Field("content", contents, Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			// Make sure we can add another normal document
			doc = new Document();
			doc.Add(new Field("content", "abc bbb ccc", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			
			// Make sure all terms < max size were indexed
			Assert.AreEqual(2, reader.DocFreq(new Term("content", "abc")));
			Assert.AreEqual(1, reader.DocFreq(new Term("content", "bbb")));
			Assert.AreEqual(1, reader.DocFreq(new Term("content", "term")));
			Assert.AreEqual(1, reader.DocFreq(new Term("content", "another")));
			
			// Make sure position is still incremented when
			// massive term is skipped:
			TermPositions tps = reader.TermPositions(new Term("content", "another"));
			Assert.IsTrue(tps.Next());
			Assert.AreEqual(1, tps.Freq());
			Assert.AreEqual(3, tps.NextPosition());
			
			// Make sure the doc that has the massive term is in
			// the index:
			Assert.AreEqual(2, reader.NumDocs(), "document with wicked long term should is not in the index!");
			
			reader.Close();
			
			// Make sure we can add a document with exactly the
			// maximum length term, and search on that term:
			doc = new Document();
			doc.Add(new Field("content", bigTerm, Field.Store.NO, Field.Index.ANALYZED));
			StandardAnalyzer sa = new StandardAnalyzer();
			sa.SetMaxTokenLength(100000);
			writer = new IndexWriter(dir, sa, IndexWriter.MaxFieldLength.LIMITED);
			writer.AddDocument(doc);
			writer.Close();
			reader = IndexReader.Open(dir);
			Assert.AreEqual(1, reader.DocFreq(new Term("content", bigTerm)));
			reader.Close();
			
			dir.Close();
		}
		
        [Test]
		public virtual void  TestOptimizeMaxNumSegments()
		{
			
			MockRAMDirectory dir = new MockRAMDirectory();
			
			Document doc = new Document();
			doc.Add(new Field("content", "aaa", Field.Store.YES, Field.Index.ANALYZED));
			
			for (int numDocs = 38; numDocs < 500; numDocs += 38)
			{
				IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				LogDocMergePolicy ldmp = new LogDocMergePolicy(writer);
				ldmp.SetMinMergeDocs(1);
				writer.SetMergePolicy(ldmp);
				writer.SetMergeFactor(5);
				writer.SetMaxBufferedDocs(2);
				for (int j = 0; j < numDocs; j++)
					writer.AddDocument(doc);
				writer.Close();
				
				SegmentInfos sis = new SegmentInfos();
				sis.Read(dir);
				int segCount = sis.Count;
				
				writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
				writer.SetMergePolicy(ldmp);
				writer.SetMergeFactor(5);
				writer.Optimize(3);
				writer.Close();
				
				sis = new SegmentInfos();
				sis.Read(dir);
				int optSegCount = sis.Count;
				
				if (segCount < 3)
					Assert.AreEqual(segCount, optSegCount);
				else
					Assert.AreEqual(3, optSegCount);
			}
		}
		
        [Test]
		public virtual void  TestOptimizeMaxNumSegments2()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			
			Document doc = new Document();
			doc.Add(new Field("content", "aaa", Field.Store.YES, Field.Index.ANALYZED));
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			LogDocMergePolicy ldmp = new LogDocMergePolicy(writer);
			ldmp.SetMinMergeDocs(1);
			writer.SetMergePolicy(ldmp);
			writer.SetMergeFactor(4);
			writer.SetMaxBufferedDocs(2);
			
			for (int iter = 0; iter < 10; iter++)
			{
				for (int i = 0; i < 19; i++)
					writer.AddDocument(doc);
				
				((ConcurrentMergeScheduler) writer.GetMergeScheduler()).Sync();
				writer.Commit();
				
				SegmentInfos sis = new SegmentInfos();
				sis.Read(dir);
				
				int segCount = sis.Count;
				
				writer.Optimize(7);
				writer.Commit();
				
				sis = new SegmentInfos();
				((ConcurrentMergeScheduler) writer.GetMergeScheduler()).Sync();
				sis.Read(dir);
				int optSegCount = sis.Count;
				
				if (segCount < 7)
					Assert.AreEqual(segCount, optSegCount);
				else
					Assert.AreEqual(7, optSegCount);
			}
		}
		
		/// <summary> Make sure optimize doesn't use any more than 1X
		/// starting index size as its temporary free space
		/// required.
		/// </summary>
        [Test]
		public virtual void  TestOptimizeTempSpaceUsage()
		{
			
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int j = 0; j < 500; j++)
			{
				AddDocWithIndex(writer, j);
			}
			writer.Close();
			
			long startDiskUsage = 0;
			System.String[] files = dir.ListAll();
			for (int i = 0; i < files.Length; i++)
			{
				startDiskUsage += dir.FileLength(files[i]);
			}
			
			dir.ResetMaxUsedSizeInBytes();
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize();
			writer.Close();
			long maxDiskUsage = dir.GetMaxUsedSizeInBytes();
			
			Assert.IsTrue(maxDiskUsage <= 2 * startDiskUsage, "optimized used too much temporary space: starting usage was " + startDiskUsage + " bytes; max temp usage was " + maxDiskUsage + " but should have been " + (2 * startDiskUsage) + " (= 2X starting usage)");
			dir.Close();
		}
		
		internal static System.String ArrayToString(System.String[] l)
		{
			System.String s = "";
			for (int i = 0; i < l.Length; i++)
			{
				if (i > 0)
				{
					s += "\n    ";
				}
				s += l[i];
			}
			return s;
		}
		
		// Make sure we can open an index for create even when a
		// reader holds it open (this fails pre lock-less
		// commits on windows):
        [Test]
		public virtual void  TestCreateWithReader()
		{
			System.IO.FileInfo indexDir = _TestUtil.GetTempDir("lucenetestindexwriter");
			
			try
			{
				Directory dir = FSDirectory.Open(indexDir);
				
				// add one document & close writer
				IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				AddDoc(writer);
				writer.Close();
				
				// now open reader:
				IndexReader reader = IndexReader.Open(dir);
				Assert.AreEqual(reader.NumDocs(), 1, "should be one document");
				
				// now open index for create:
				writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				Assert.AreEqual(writer.DocCount(), 0, "should be zero documents");
				AddDoc(writer);
				writer.Close();
				
				Assert.AreEqual(reader.NumDocs(), 1, "should be one document");
				IndexReader reader2 = IndexReader.Open(dir);
				Assert.AreEqual(reader2.NumDocs(), 1, "should be one document");
				reader.Close();
				reader2.Close();
			}
			finally
			{
				RmDir(indexDir);
			}
		}
		
		
		// Same test as above, but use IndexWriter constructor
		// that takes File:
        [Test]
		public virtual void  TestCreateWithReader2()
		{
			System.IO.FileInfo indexDir = _TestUtil.GetTempDir("lucenetestindexwriter");
			try
			{
				// add one document & close writer
				IndexWriter writer = new IndexWriter(indexDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				AddDoc(writer);
				writer.Close();
				
				// now open reader:
				IndexReader reader = IndexReader.Open(indexDir);
				Assert.AreEqual(reader.NumDocs(), 1, "should be one document");
				
				// now open index for create:
				writer = new IndexWriter(indexDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				Assert.AreEqual(writer.DocCount(), 0, "should be zero documents");
				AddDoc(writer);
				writer.Close();
				
				Assert.AreEqual(reader.NumDocs(), 1, "should be one document");
				IndexReader reader2 = IndexReader.Open(indexDir);
				Assert.AreEqual(reader2.NumDocs(), 1, "should be one document");
				reader.Close();
				reader2.Close();
			}
			finally
			{
				RmDir(indexDir);
			}
		}
		
		// Same test as above, but use IndexWriter constructor
		// that takes String:
        [Test]
		public virtual void  TestCreateWithReader3()
		{
			System.IO.FileInfo dirName = _TestUtil.GetTempDir("lucenetestindexwriter");
			try
			{
				
				// add one document & close writer
				IndexWriter writer = new IndexWriter(dirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				AddDoc(writer);
				writer.Close();
				
				// now open reader:
				IndexReader reader = IndexReader.Open(dirName);
				Assert.AreEqual(reader.NumDocs(), 1, "should be one document");
				
				// now open index for create:
				writer = new IndexWriter(dirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				Assert.AreEqual(writer.DocCount(), 0, "should be zero documents");
				AddDoc(writer);
				writer.Close();
				
				Assert.AreEqual(reader.NumDocs(), 1, "should be one document");
				IndexReader reader2 = IndexReader.Open(dirName);
				Assert.AreEqual(reader2.NumDocs(), 1, "should be one document");
				reader.Close();
				reader2.Close();
			}
			finally
			{
				RmDir(dirName);
			}
		}
		
		// Simulate a writer that crashed while writing segments
		// file: make sure we can still open the index (ie,
		// gracefully fallback to the previous segments file),
		// and that we can add to the index:
        [Test]
		public virtual void  TestSimulatedCrashedWriter()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = null;
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			// add 100 documents
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer);
			}
			
			// close
			writer.Close();
			
			long gen = SegmentInfos.GetCurrentSegmentGeneration(dir);
			Assert.IsTrue(gen > 1, "segment generation should be > 1 but got " + gen);
			
			// Make the next segments file, with last byte
			// missing, to simulate a writer that crashed while
			// writing segments file:
			System.String fileNameIn = SegmentInfos.GetCurrentSegmentFileName(dir);
			System.String fileNameOut = IndexFileNames.FileNameFromGeneration(IndexFileNames.SEGMENTS, "", 1 + gen);
			IndexInput in_Renamed = dir.OpenInput(fileNameIn);
			IndexOutput out_Renamed = dir.CreateOutput(fileNameOut);
			long length = in_Renamed.Length();
			for (int i = 0; i < length - 1; i++)
			{
				out_Renamed.WriteByte(in_Renamed.ReadByte());
			}
			in_Renamed.Close();
			out_Renamed.Close();
			
			IndexReader reader = null;
			try
			{
				reader = IndexReader.Open(dir);
			}
			catch (System.Exception e)
			{
				Assert.Fail("reader failed to open on a crashed index");
			}
			reader.Close();
			
			try
			{
				writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			}
			catch (System.Exception e)
			{
				Assert.Fail("writer failed to open on a crashed index");
			}
			
			// add 100 documents
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer);
			}
			
			// close
			writer.Close();
		}
		
		// Simulate a corrupt index by removing last byte of
		// latest segments file and make sure we get an
		// IOException trying to open the index:
        [Test]
		public virtual void  TestSimulatedCorruptIndex1()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = null;
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			// add 100 documents
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer);
			}
			
			// close
			writer.Close();
			
			long gen = SegmentInfos.GetCurrentSegmentGeneration(dir);
			Assert.IsTrue(gen > 1, "segment generation should be > 1 but got " + gen);
			
			System.String fileNameIn = SegmentInfos.GetCurrentSegmentFileName(dir);
			System.String fileNameOut = IndexFileNames.FileNameFromGeneration(IndexFileNames.SEGMENTS, "", 1 + gen);
			IndexInput in_Renamed = dir.OpenInput(fileNameIn);
			IndexOutput out_Renamed = dir.CreateOutput(fileNameOut);
			long length = in_Renamed.Length();
			for (int i = 0; i < length - 1; i++)
			{
				out_Renamed.WriteByte(in_Renamed.ReadByte());
			}
			in_Renamed.Close();
			out_Renamed.Close();
			dir.DeleteFile(fileNameIn);
			
			IndexReader reader = null;
			try
			{
				reader = IndexReader.Open(dir);
				Assert.Fail("reader did not hit IOException on opening a corrupt index");
			}
			catch (System.Exception e)
			{
			}
			if (reader != null)
			{
				reader.Close();
			}
		}
		
        [Test]
		public virtual void  TestChangesAfterClose()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = null;
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDoc(writer);
			
			// close
			writer.Close();
			try
			{
				AddDoc(writer);
				Assert.Fail("did not hit AlreadyClosedException");
			}
			catch (AlreadyClosedException e)
			{
				// expected
			}
		}
		
		
		// Simulate a corrupt index by removing one of the cfs
		// files and make sure we get an IOException trying to
		// open the index:
        [Test]
		public virtual void  TestSimulatedCorruptIndex2()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = null;
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			// add 100 documents
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer);
			}
			
			// close
			writer.Close();
			
			long gen = SegmentInfos.GetCurrentSegmentGeneration(dir);
			Assert.IsTrue(gen > 1, "segment generation should be > 1 but got " + gen);
			
			System.String[] files = dir.ListAll();
			for (int i = 0; i < files.Length; i++)
			{
				if (files[i].EndsWith(".cfs"))
				{
					dir.DeleteFile(files[i]);
					break;
				}
			}
			
			IndexReader reader = null;
			try
			{
				reader = IndexReader.Open(dir);
				Assert.Fail("reader did not hit IOException on opening a corrupt index");
			}
			catch (System.Exception e)
			{
			}
			if (reader != null)
			{
				reader.Close();
			}
		}
		
		/*
		* Simple test for "commit on close": open writer with
		* autoCommit=false, so it will only commit on close,
		* then add a bunch of docs, making sure reader does not
		* see these docs until writer is closed.
		*/
        [Test]
		public virtual void  TestCommitOnClose()
		{
			Directory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 14; i++)
			{
				AddDoc(writer);
			}
			writer.Close();
			
			Term searchTerm = new Term("content", "aaa");
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
			Assert.AreEqual(14, hits.Length, "first number of hits");
			searcher.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 3; i++)
			{
				for (int j = 0; j < 11; j++)
				{
					AddDoc(writer);
				}
				searcher = new IndexSearcher(dir);
				hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
				Assert.AreEqual(14, hits.Length, "reader incorrectly sees changes from writer with autoCommit disabled");
				searcher.Close();
				Assert.IsTrue(reader.IsCurrent(), "reader should have still been current");
			}
			
			// Now, close the writer:
			writer.Close();
			Assert.IsFalse(reader.IsCurrent(), "reader should not be current now");
			
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
			Assert.AreEqual(47, hits.Length, "reader did not see changes after writer was closed");
			searcher.Close();
		}
		
		/*
		* Simple test for "commit on close": open writer with
		* autoCommit=false, so it will only commit on close,
		* then add a bunch of docs, making sure reader does not
		* see them until writer has closed.  Then instead of
		* closing the writer, call abort and verify reader sees
		* nothing was added.  Then verify we can open the index
		* and add docs to it.
		*/
        [Test]
		public virtual void  TestCommitOnCloseAbort()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			for (int i = 0; i < 14; i++)
			{
				AddDoc(writer);
			}
			writer.Close();
			
			Term searchTerm = new Term("content", "aaa");
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
			Assert.AreEqual(14, hits.Length, "first number of hits");
			searcher.Close();
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			for (int j = 0; j < 17; j++)
			{
				AddDoc(writer);
			}
			// Delete all docs:
			writer.DeleteDocuments(searchTerm);
			
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
			Assert.AreEqual(14, hits.Length, "reader incorrectly sees changes from writer with autoCommit disabled");
			searcher.Close();
			
			// Now, close the writer:
			writer.Abort();
			
			AssertNoUnreferencedFiles(dir, "unreferenced files remain after abort()");
			
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
			Assert.AreEqual(14, hits.Length, "saw changes after writer.abort");
			searcher.Close();
			
			// Now make sure we can re-open the index, add docs,
			// and all is good:
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			
			// On abort, writer in fact may write to the same
			// segments_N file:
			dir.SetPreventDoubleWrite(false);
			
			for (int i = 0; i < 12; i++)
			{
				for (int j = 0; j < 17; j++)
				{
					AddDoc(writer);
				}
				searcher = new IndexSearcher(dir);
				hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
				Assert.AreEqual(14, hits.Length, "reader incorrectly sees changes from writer with autoCommit disabled");
				searcher.Close();
			}
			
			writer.Close();
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
			Assert.AreEqual(218, hits.Length, "didn't see changes after close");
			searcher.Close();
			
			dir.Close();
		}
		
		/*
		* Verify that a writer with "commit on close" indeed
		* cleans up the temp segments created after opening
		* that are not referenced by the starting segments
		* file.  We check this by using MockRAMDirectory to
		* measure max temp disk space used.
		*/
        [Test]
		public virtual void  TestCommitOnCloseDiskUsage()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int j = 0; j < 30; j++)
			{
				AddDocWithIndex(writer, j);
			}
			writer.Close();
			dir.ResetMaxUsedSizeInBytes();
			
			long startDiskUsage = dir.GetMaxUsedSizeInBytes();
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			writer.SetMergeScheduler(new SerialMergeScheduler());
			for (int j = 0; j < 1470; j++)
			{
				AddDocWithIndex(writer, j);
			}
			long midDiskUsage = dir.GetMaxUsedSizeInBytes();
			dir.ResetMaxUsedSizeInBytes();
			writer.Optimize();
			writer.Close();
			
			IndexReader.Open(dir).Close();
			
			long endDiskUsage = dir.GetMaxUsedSizeInBytes();
			
			// Ending index is 50X as large as starting index; due
			// to 2X disk usage normally we allow 100X max
			// transient usage.  If something is wrong w/ deleter
			// and it doesn't delete intermediate segments then it
			// will exceed this 100X:
			// System.out.println("start " + startDiskUsage + "; mid " + midDiskUsage + ";end " + endDiskUsage);
			Assert.IsTrue(midDiskUsage < 100 * startDiskUsage, "writer used too much space while adding documents when autoCommit=false: mid=" + midDiskUsage + " start=" + startDiskUsage + " end=" + endDiskUsage);
			Assert.IsTrue(endDiskUsage < 100 * startDiskUsage, "writer used too much space after close when autoCommit=false endDiskUsage=" + endDiskUsage + " startDiskUsage=" + startDiskUsage);
		}
		
		
		/*
		* Verify that calling optimize when writer is open for
		* "commit on close" works correctly both for abort()
		* and close().
		*/
        [Test]
		public virtual void  TestCommitOnCloseOptimize()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			for (int j = 0; j < 17; j++)
			{
				AddDocWithIndex(writer, j);
			}
			writer.Close();
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize();
			
			// Open a reader before closing (commiting) the writer:
			IndexReader reader = IndexReader.Open(dir);
			
			// Reader should see index as unoptimized at this
			// point:
			Assert.IsFalse(reader.IsOptimized(), "Reader incorrectly sees that the index is optimized");
			reader.Close();
			
			// Abort the writer:
			writer.Abort();
			AssertNoUnreferencedFiles(dir, "aborted writer after optimize");
			
			// Open a reader after aborting writer:
			reader = IndexReader.Open(dir);
			
			// Reader should still see index as unoptimized:
			Assert.IsFalse(reader.IsOptimized(), "Reader incorrectly sees that the index is optimized");
			reader.Close();
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize();
			writer.Close();
			AssertNoUnreferencedFiles(dir, "aborted writer after optimize");
			
			// Open a reader after aborting writer:
			reader = IndexReader.Open(dir);
			
			// Reader should still see index as unoptimized:
			Assert.IsTrue(reader.IsOptimized(), "Reader incorrectly sees that the index is unoptimized");
			reader.Close();
		}
		
        [Test]
		public virtual void  TestIndexNoDocuments()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.Flush();
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(0, reader.MaxDoc());
			Assert.AreEqual(0, reader.NumDocs());
			reader.Close();
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Flush();
			writer.Close();
			
			reader = IndexReader.Open(dir);
			Assert.AreEqual(0, reader.MaxDoc());
			Assert.AreEqual(0, reader.NumDocs());
			reader.Close();
		}
		
        [Test]
		public virtual void  TestManyFields()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			for (int j = 0; j < 100; j++)
			{
				Document doc = new Document();
				doc.Add(new Field("a" + j, "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
				doc.Add(new Field("b" + j, "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
				doc.Add(new Field("c" + j, "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
				doc.Add(new Field("d" + j, "aaa", Field.Store.YES, Field.Index.ANALYZED));
				doc.Add(new Field("e" + j, "aaa", Field.Store.YES, Field.Index.ANALYZED));
				doc.Add(new Field("f" + j, "aaa", Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(100, reader.MaxDoc());
			Assert.AreEqual(100, reader.NumDocs());
			for (int j = 0; j < 100; j++)
			{
				Assert.AreEqual(1, reader.DocFreq(new Term("a" + j, "aaa" + j)));
				Assert.AreEqual(1, reader.DocFreq(new Term("b" + j, "aaa" + j)));
				Assert.AreEqual(1, reader.DocFreq(new Term("c" + j, "aaa" + j)));
				Assert.AreEqual(1, reader.DocFreq(new Term("d" + j, "aaa")));
				Assert.AreEqual(1, reader.DocFreq(new Term("e" + j, "aaa")));
				Assert.AreEqual(1, reader.DocFreq(new Term("f" + j, "aaa")));
			}
			reader.Close();
			dir.Close();
		}
		
        [Test]
		public virtual void  TestSmallRAMBuffer()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetRAMBufferSizeMB(0.000001);
			int lastNumFile = dir.ListAll().Length;
			for (int j = 0; j < 9; j++)
			{
				Document doc = new Document();
				doc.Add(new Field("field", "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(doc);
				int numFile = dir.ListAll().Length;
				// Verify that with a tiny RAM buffer we see new
				// segment after every doc
				Assert.IsTrue(numFile > lastNumFile);
				lastNumFile = numFile;
			}
			writer.Close();
			dir.Close();
		}
		
		// Make sure it's OK to change RAM buffer size and
		// maxBufferedDocs in a write session
        [Test]
		public virtual void  TestChangingRAMBuffer()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
			
			int lastFlushCount = - 1;
			for (int j = 1; j < 52; j++)
			{
				Document doc = new Document();
				doc.Add(new Field("field", "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(doc);
				_TestUtil.SyncConcurrentMerges(writer);
				int flushCount = writer.GetFlushCount();
				if (j == 1)
					lastFlushCount = flushCount;
				else if (j < 10)
				// No new files should be created
					Assert.AreEqual(flushCount, lastFlushCount);
				else if (10 == j)
				{
					Assert.IsTrue(flushCount > lastFlushCount);
					lastFlushCount = flushCount;
					writer.SetRAMBufferSizeMB(0.000001);
					writer.SetMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
				}
				else if (j < 20)
				{
					Assert.IsTrue(flushCount > lastFlushCount);
					lastFlushCount = flushCount;
				}
				else if (20 == j)
				{
					writer.SetRAMBufferSizeMB(16);
					writer.SetMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
					lastFlushCount = flushCount;
				}
				else if (j < 30)
				{
					Assert.AreEqual(flushCount, lastFlushCount);
				}
				else if (30 == j)
				{
					writer.SetRAMBufferSizeMB(0.000001);
					writer.SetMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
				}
				else if (j < 40)
				{
					Assert.IsTrue(flushCount > lastFlushCount);
					lastFlushCount = flushCount;
				}
				else if (40 == j)
				{
					writer.SetMaxBufferedDocs(10);
					writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
					lastFlushCount = flushCount;
				}
				else if (j < 50)
				{
					Assert.AreEqual(flushCount, lastFlushCount);
					writer.SetMaxBufferedDocs(10);
					writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
				}
				else if (50 == j)
				{
					Assert.IsTrue(flushCount > lastFlushCount);
				}
			}
			writer.Close();
			dir.Close();
		}
		
        [Test]
		public virtual void  TestChangingRAMBuffer2()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			writer.SetMaxBufferedDeleteTerms(10);
			writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
			
			for (int j = 1; j < 52; j++)
			{
				Document doc = new Document();
				doc.Add(new Field("field", "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
			
			int lastFlushCount = - 1;
			for (int j = 1; j < 52; j++)
			{
				writer.DeleteDocuments(new Term("field", "aaa" + j));
				_TestUtil.SyncConcurrentMerges(writer);
				int flushCount = writer.GetFlushCount();
				if (j == 1)
					lastFlushCount = flushCount;
				else if (j < 10)
				{
					// No new files should be created
					Assert.AreEqual(flushCount, lastFlushCount);
				}
				else if (10 == j)
				{
					Assert.IsTrue(flushCount > lastFlushCount);
					lastFlushCount = flushCount;
					writer.SetRAMBufferSizeMB(0.000001);
					writer.SetMaxBufferedDeleteTerms(1);
				}
				else if (j < 20)
				{
					Assert.IsTrue(flushCount > lastFlushCount);
					lastFlushCount = flushCount;
				}
				else if (20 == j)
				{
					writer.SetRAMBufferSizeMB(16);
					writer.SetMaxBufferedDeleteTerms(IndexWriter.DISABLE_AUTO_FLUSH);
					lastFlushCount = flushCount;
				}
				else if (j < 30)
				{
					Assert.AreEqual(flushCount, lastFlushCount);
				}
				else if (30 == j)
				{
					writer.SetRAMBufferSizeMB(0.000001);
					writer.SetMaxBufferedDeleteTerms(IndexWriter.DISABLE_AUTO_FLUSH);
					writer.SetMaxBufferedDeleteTerms(1);
				}
				else if (j < 40)
				{
					Assert.IsTrue(flushCount > lastFlushCount);
					lastFlushCount = flushCount;
				}
				else if (40 == j)
				{
					writer.SetMaxBufferedDeleteTerms(10);
					writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
					lastFlushCount = flushCount;
				}
				else if (j < 50)
				{
					Assert.AreEqual(flushCount, lastFlushCount);
					writer.SetMaxBufferedDeleteTerms(10);
					writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
				}
				else if (50 == j)
				{
					Assert.IsTrue(flushCount > lastFlushCount);
				}
			}
			writer.Close();
			dir.Close();
		}
		
        [Test]
		public virtual void  TestDiverseDocs()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetRAMBufferSizeMB(0.5);
			System.Random rand = NewRandom();
			for (int i = 0; i < 3; i++)
			{
				// First, docs where every term is unique (heavy on
				// Posting instances)
				for (int j = 0; j < 100; j++)
				{
					Document doc = new Document();
					for (int k = 0; k < 100; k++)
					{
						doc.Add(new Field("field", System.Convert.ToString(rand.Next()), Field.Store.YES, Field.Index.ANALYZED));
					}
					writer.AddDocument(doc);
				}
				
				// Next, many single term docs where only one term
				// occurs (heavy on byte blocks)
				for (int j = 0; j < 100; j++)
				{
					Document doc = new Document();
					doc.Add(new Field("field", "aaa aaa aaa aaa aaa aaa aaa aaa aaa aaa", Field.Store.YES, Field.Index.ANALYZED));
					writer.AddDocument(doc);
				}
				
				// Next, many single term docs where only one term
				// occurs but the terms are very long (heavy on
				// char[] arrays)
				for (int j = 0; j < 100; j++)
				{
					System.Text.StringBuilder b = new System.Text.StringBuilder();
					System.String x = System.Convert.ToString(j) + ".";
					for (int k = 0; k < 1000; k++)
						b.Append(x);
					System.String longTerm = b.ToString();
					
					Document doc = new Document();
					doc.Add(new Field("field", longTerm, Field.Store.YES, Field.Index.ANALYZED));
					writer.AddDocument(doc);
				}
			}
			writer.Close();
			
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(new TermQuery(new Term("field", "aaa")), null, 1000).scoreDocs;
			Assert.AreEqual(300, hits.Length);
			searcher.Close();
			
			dir.Close();
		}
		
        [Test]
		public virtual void  TestEnablingNorms()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			// Enable norms for only 1 doc, pre flush
			for (int j = 0; j < 10; j++)
			{
				Document doc = new Document();
				Field f = new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED);
				if (j != 8)
				{
					f.SetOmitNorms(true);
				}
				doc.Add(f);
				writer.AddDocument(doc);
			}
			writer.Close();
			
			Term searchTerm = new Term("field", "aaa");
			
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
			Assert.AreEqual(10, hits.Length);
			searcher.Close();
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			// Enable norms for only 1 doc, post flush
			for (int j = 0; j < 27; j++)
			{
				Document doc = new Document();
				Field f = new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED);
				if (j != 26)
				{
					f.SetOmitNorms(true);
				}
				doc.Add(f);
				writer.AddDocument(doc);
			}
			writer.Close();
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
			Assert.AreEqual(27, hits.Length);
			searcher.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			reader.Close();
			
			dir.Close();
		}
		
        [Test]
		public virtual void  TestHighFreqTerm()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, new IndexWriter.MaxFieldLength(100000000));
			writer.SetRAMBufferSizeMB(0.01);
			// Massive doc that has 128 K a's
			System.Text.StringBuilder b = new System.Text.StringBuilder(1024 * 1024);
			for (int i = 0; i < 4096; i++)
			{
				b.Append(" a a a a a a a a");
				b.Append(" a a a a a a a a");
				b.Append(" a a a a a a a a");
				b.Append(" a a a a a a a a");
			}
			Document doc = new Document();
			doc.Add(new Field("field", b.ToString(), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			writer.AddDocument(doc);
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(1, reader.MaxDoc());
			Assert.AreEqual(1, reader.NumDocs());
			Term t = new Term("field", "a");
			Assert.AreEqual(1, reader.DocFreq(t));
			TermDocs td = reader.TermDocs(t);
			td.Next();
			Assert.AreEqual(128 * 1024, td.Freq());
			reader.Close();
			dir.Close();
		}
		
		// Make sure that a Directory implementation that does
		// not use LockFactory at all (ie overrides makeLock and
		// implements its own private locking) works OK.  This
		// was raised on java-dev as loss of backwards
		// compatibility.
        [Test]
		public virtual void  TestNullLockFactory()
		{
			
			
			Directory dir = new MyRAMDirectory(this);
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer);
			}
			writer.Close();
			Term searchTerm = new Term("content", "aaa");
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
			Assert.AreEqual(100, hits.Length, "did not get right number of hits");
			writer.Close();
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.Close();
			
			dir.Close();
		}
		
        [Test]
		public virtual void  TestFlushWithNoMerging()
		{
			Directory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			Document doc = new Document();
			doc.Add(new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			for (int i = 0; i < 19; i++)
				writer.AddDocument(doc);
			writer.Flush(false, true, true);
			writer.Close();
			SegmentInfos sis = new SegmentInfos();
			sis.Read(dir);
			// Since we flushed w/o allowing merging we should now
			// have 10 segments
			// {{}} assert sis.size() == 10;
		}
		
		// Make sure we can flush segment w/ norms, then add
		// empty doc (no norms) and flush
        [Test]
		public virtual void  TestEmptyDocAfterFlushingRealDoc()
		{
			Directory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			writer.AddDocument(doc);
			writer.Flush();
			writer.AddDocument(new Document());
			writer.Close();
			_TestUtil.CheckIndex(dir);
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(2, reader.NumDocs());
		}
		
		// Test calling optimize(false) whereby optimize is kicked
		// off but we don't wait for it to finish (but
		// writer.close()) does wait
        [Test]
		public virtual void  TestBackgroundOptimize()
		{
			
			Directory dir = new MockRAMDirectory();
			for (int pass = 0; pass < 2; pass++)
			{
				IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				writer.SetMergeScheduler(new ConcurrentMergeScheduler());
				Document doc = new Document();
				doc.Add(new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
				writer.SetMaxBufferedDocs(2);
				writer.SetMergeFactor(101);
				for (int i = 0; i < 200; i++)
					writer.AddDocument(doc);
				writer.Optimize(false);
				
				if (0 == pass)
				{
					writer.Close();
					IndexReader reader = IndexReader.Open(dir);
					Assert.IsTrue(reader.IsOptimized());
					reader.Close();
				}
				else
				{
					// Get another segment to flush so we can verify it is
					// NOT included in the optimization
					writer.AddDocument(doc);
					writer.AddDocument(doc);
					writer.Close();
					
					IndexReader reader = IndexReader.Open(dir);
					Assert.IsTrue(!reader.IsOptimized());
					reader.Close();
					
					SegmentInfos infos = new SegmentInfos();
					infos.Read(dir);
					Assert.AreEqual(2, infos.Count);
				}
			}
			
			dir.Close();
		}
		
		private void  RmDir(System.IO.FileInfo dir)
		{
			System.IO.FileInfo[] files = SupportClass.FileSupport.GetFiles(dir);
			if (files != null)
			{
				for (int i = 0; i < files.Length; i++)
				{
					bool tmpBool;
					if (System.IO.File.Exists(files[i].FullName))
					{
						System.IO.File.Delete(files[i].FullName);
						tmpBool = true;
					}
					else if (System.IO.Directory.Exists(files[i].FullName))
					{
						System.IO.Directory.Delete(files[i].FullName);
						tmpBool = true;
					}
					else
						tmpBool = false;
					bool generatedAux = tmpBool;
				}
			}
			bool tmpBool2;
			if (System.IO.File.Exists(dir.FullName))
			{
				System.IO.File.Delete(dir.FullName);
				tmpBool2 = true;
			}
			else if (System.IO.Directory.Exists(dir.FullName))
			{
				System.IO.Directory.Delete(dir.FullName);
				tmpBool2 = true;
			}
			else
				tmpBool2 = false;
			bool generatedAux2 = tmpBool2;
		}
		
		/// <summary> Test that no NullPointerException will be raised,
		/// when adding one document with a single, empty field
		/// and term vectors enabled.
		/// </summary>
		/// <throws>  IOException </throws>
		/// <summary> 
		/// </summary>
        [Test]
		public virtual void  TestBadSegment()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter ir = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			Document document = new Document();
			document.Add(new Field("tvtest", "", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES));
			ir.AddDocument(document);
			ir.Close();
			dir.Close();
		}
		
		// LUCENE-1008
        [Test]
		public virtual void  TestNoTermVectorAfterTermVector()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter iw = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Document document = new Document();
			document.Add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES));
			iw.AddDocument(document);
			document = new Document();
			document.Add(new Field("tvtest", "x y z", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO));
			iw.AddDocument(document);
			// Make first segment
			iw.Flush();
			
			document.Add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES));
			iw.AddDocument(document);
			// Make 2nd segment
			iw.Flush();
			
			iw.Optimize();
			iw.Close();
			dir.Close();
		}
		
		// LUCENE-1010
        [Test]
		public virtual void  TestNoTermVectorAfterTermVectorMerge()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter iw = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Document document = new Document();
			document.Add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES));
			iw.AddDocument(document);
			iw.Flush();
			
			document = new Document();
			document.Add(new Field("tvtest", "x y z", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO));
			iw.AddDocument(document);
			// Make first segment
			iw.Flush();
			
			iw.Optimize();
			
			document.Add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES));
			iw.AddDocument(document);
			// Make 2nd segment
			iw.Flush();
			iw.Optimize();
			
			iw.Close();
			dir.Close();
		}
		
		// LUCENE-1036
        [Test]
		public virtual void  TestMaxThreadPriority()
		{
			int pri = (System.Int32) SupportClass.ThreadClass.Current().Priority;
			try
			{
				MockRAMDirectory dir = new MockRAMDirectory();
				IndexWriter iw = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				Document document = new Document();
				document.Add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES));
				iw.SetMaxBufferedDocs(2);
				iw.SetMergeFactor(2);
				SupportClass.ThreadClass.Current().Priority = (System.Threading.ThreadPriority) System.Threading.ThreadPriority.Highest;
				for (int i = 0; i < 4; i++)
					iw.AddDocument(document);
				iw.Close();
			}
			finally
			{
				SupportClass.ThreadClass.Current().Priority = (System.Threading.ThreadPriority) pri;
			}
		}
		
		// Just intercepts all merges & verifies that we are never
		// merging a segment with >= 20 (maxMergeDocs) docs
		private class MyMergeScheduler:MergeScheduler
		{
			public MyMergeScheduler(TestIndexWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override /*virtual*/ void  Merge(IndexWriter writer)
			{
				lock (this)
				{
					
					while (true)
					{
						MergePolicy.OneMerge merge = writer.GetNextMerge();
						if (merge == null)
							break;
						for (int i = 0; i < merge.segments_ForNUnit.Count; i++)
							; // {{}} assert merge.segments.Info(i).docCount < 20;
						writer.Merge(merge);
					}
				}
			}
			
			public override /*virtual*/ void  Close()
			{
			}
		}
		
		// LUCENE-1013
        [Test]
		public virtual void  TestSetMaxMergeDocs()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter iw = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			iw.SetMergeScheduler(new MyMergeScheduler(this));
			iw.SetMaxMergeDocs(20);
			iw.SetMaxBufferedDocs(2);
			iw.SetMergeFactor(2);
			Document document = new Document();
			document.Add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.YES));
			for (int i = 0; i < 177; i++)
				iw.AddDocument(document);
			iw.Close();
		}
		
		// LUCENE-1072
        [Test]
		public virtual void  TestExceptionFromTokenStream()
		{
			RAMDirectory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new AnonymousClassAnalyzer(this), true, IndexWriter.MaxFieldLength.LIMITED);
			
			Document doc = new Document();
			System.String contents = "aa bb cc dd ee ff gg hh ii jj kk";
			doc.Add(new Field("content", contents, Field.Store.NO, Field.Index.ANALYZED));
			try
			{
				writer.AddDocument(doc);
				Assert.Fail("did not hit expected exception");
			}
			catch (System.Exception e)
			{
			}
			
			// Make sure we can add another normal document
			doc = new Document();
			doc.Add(new Field("content", "aa bb cc dd", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			// Make sure we can add another normal document
			doc = new Document();
			doc.Add(new Field("content", "aa bb cc dd", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			writer.Close();
			IndexReader reader = IndexReader.Open(dir);
			Term t = new Term("content", "aa");
			Assert.AreEqual(reader.DocFreq(t), 3);
			
			// Make sure the doc that hit the exception was marked
			// as deleted:
			TermDocs tdocs = reader.TermDocs(t);
			int count = 0;
			while (tdocs.Next())
			{
				count++;
			}
			Assert.AreEqual(2, count);
			
			Assert.AreEqual(reader.DocFreq(new Term("content", "gg")), 0);
			reader.Close();
			dir.Close();
		}
		
		private class FailOnlyOnFlush:MockRAMDirectory.Failure
		{
			internal bool doFail = false;
			internal int count;
			
			public virtual void  SetDoFail()
			{
				this.doFail = true;
			}
			public virtual void  ClearDoFail()
			{
				this.doFail = false;
			}
			
			public override void  Eval(MockRAMDirectory dir)
			{
				if (doFail)
				{
					System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace();
					bool sawAppend = false;
					bool sawFlush = false;
					for (int i = 0; i < trace.FrameCount; i++)
					{
						System.Diagnostics.StackFrame sf = trace.GetFrame(i);
                        string className = sf.GetMethod().DeclaringType.Namespace + "." + sf.GetMethod().DeclaringType.Name;
                        if ("Lucene.Net.Index.FreqProxTermsWriter".Equals(className) && "AppendPostings".Equals(sf.GetMethod().Name))
							sawAppend = true;
						if ("DoFlush".Equals(sf.GetMethod().Name))
							sawFlush = true;
					}
					
					if (sawAppend && sawFlush && count++ >= 30)
					{
						doFail = false;
						throw new System.IO.IOException("now failing during flush");
					}
				}
			}
		}
		
		// LUCENE-1072: make sure an errant exception on flushing
		// one segment only takes out those docs in that one flush
        [Test]
		public virtual void  TestDocumentsWriterAbort()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			FailOnlyOnFlush failure = new FailOnlyOnFlush();
			failure.SetDoFail();
			dir.FailOn(failure);
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			Document doc = new Document();
			System.String contents = "aa bb cc dd ee ff gg hh ii jj kk";
			doc.Add(new Field("content", contents, Field.Store.NO, Field.Index.ANALYZED));
			bool hitError = false;
			for (int i = 0; i < 200; i++)
			{
				try
				{
					writer.AddDocument(doc);
				}
				catch (System.IO.IOException ioe)
				{
					// only one flush should fail:
					Assert.IsFalse(hitError);
					hitError = true;
				}
			}
			Assert.IsTrue(hitError);
			writer.Close();
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(198, reader.DocFreq(new Term("content", "aa")));
			reader.Close();
		}
		
		private class CrashingFilter:TokenFilter
		{
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal System.String fieldName;
			internal int count;
			
			public CrashingFilter(TestIndexWriter enclosingInstance, System.String fieldName, TokenStream input):base(input)
			{
				InitBlock(enclosingInstance);
				this.fieldName = fieldName;
			}
			
			public override bool IncrementToken()
			{
				if (this.fieldName.Equals("crash") && count++ >= 4)
					throw new System.IO.IOException("I'm experiencing problems");
				return input.IncrementToken();
			}
			
			public override void  Reset()
			{
				base.Reset();
				count = 0;
			}
		}
		
        [Test]
		public virtual void  TestDocumentsWriterExceptions()
		{
			Analyzer analyzer = new AnonymousClassAnalyzer1(this);
			
			for (int i = 0; i < 2; i++)
			{
				MockRAMDirectory dir = new MockRAMDirectory();
				IndexWriter writer = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);
				//writer.setInfoStream(System.out);
				Document doc = new Document();
				doc.Add(new Field("contents", "here are some contents", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
				writer.AddDocument(doc);
				writer.AddDocument(doc);
				doc.Add(new Field("crash", "this should crash after 4 terms", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
				doc.Add(new Field("other", "this will not get indexed", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
				try
				{
					writer.AddDocument(doc);
					Assert.Fail("did not hit expected exception");
				}
				catch (System.IO.IOException ioe)
				{
				}
				
				if (0 == i)
				{
					doc = new Document();
					doc.Add(new Field("contents", "here are some contents", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
					writer.AddDocument(doc);
					writer.AddDocument(doc);
				}
				writer.Close();
				
				IndexReader reader = IndexReader.Open(dir);
				int expected = 3 + (1 - i) * 2;
				Assert.AreEqual(expected, reader.DocFreq(new Term("contents", "here")));
				Assert.AreEqual(expected, reader.MaxDoc());
				int numDel = 0;
				for (int j = 0; j < reader.MaxDoc(); j++)
				{
					if (reader.IsDeleted(j))
						numDel++;
					else
					{
						reader.Document(j);
						reader.GetTermFreqVectors(j);
					}
				}
				reader.Close();
				
				Assert.AreEqual(1, numDel);
				
				writer = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);
				writer.SetMaxBufferedDocs(10);
				doc = new Document();
				doc.Add(new Field("contents", "here are some contents", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
				for (int j = 0; j < 17; j++)
					writer.AddDocument(doc);
				writer.Optimize();
				writer.Close();
				
				reader = IndexReader.Open(dir);
				expected = 19 + (1 - i) * 2;
				Assert.AreEqual(expected, reader.DocFreq(new Term("contents", "here")));
				Assert.AreEqual(expected, reader.MaxDoc());
				numDel = 0;
				for (int j = 0; j < reader.MaxDoc(); j++)
				{
					if (reader.IsDeleted(j))
						numDel++;
					else
					{
						reader.Document(j);
						reader.GetTermFreqVectors(j);
					}
				}
				reader.Close();
				Assert.AreEqual(0, numDel);
				
				dir.Close();
			}
		}
		
        [Test]
		public virtual void  TestDocumentsWriterExceptionThreads()
		{
			Analyzer analyzer = new AnonymousClassAnalyzer2(this);
			
			int NUM_THREAD = 3;
			int NUM_ITER = 100;
			
			for (int i = 0; i < 2; i++)
			{
				MockRAMDirectory dir = new MockRAMDirectory();
				
				{
					IndexWriter writer = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);
					
					int finalI = i;
					
					SupportClass.ThreadClass[] threads = new SupportClass.ThreadClass[NUM_THREAD];
					for (int t = 0; t < NUM_THREAD; t++)
					{
						threads[t] = new AnonymousClassThread(NUM_ITER, writer, finalI, this);
						threads[t].Start();
					}
					
					for (int t = 0; t < NUM_THREAD; t++)
						threads[t].Join();
					
					writer.Close();
				}
				
				IndexReader reader = IndexReader.Open(dir);
				int expected = (3 + (1 - i) * 2) * NUM_THREAD * NUM_ITER;
				Assert.AreEqual(expected, reader.DocFreq(new Term("contents", "here")));
				Assert.AreEqual(expected, reader.MaxDoc());
				int numDel = 0;
				for (int j = 0; j < reader.MaxDoc(); j++)
				{
					if (reader.IsDeleted(j))
						numDel++;
					else
					{
						reader.Document(j);
						reader.GetTermFreqVectors(j);
					}
				}
				reader.Close();
				
				Assert.AreEqual(NUM_THREAD * NUM_ITER, numDel);
				
				IndexWriter writer2 = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);
				writer2.SetMaxBufferedDocs(10);
				Document doc = new Document();
				doc.Add(new Field("contents", "here are some contents", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
				for (int j = 0; j < 17; j++)
					writer2.AddDocument(doc);
				writer2.Optimize();
				writer2.Close();
				
				reader = IndexReader.Open(dir);
				expected += 17 - NUM_THREAD * NUM_ITER;
				Assert.AreEqual(expected, reader.DocFreq(new Term("contents", "here")));
				Assert.AreEqual(expected, reader.MaxDoc());
				numDel = 0;
				for (int j = 0; j < reader.MaxDoc(); j++)
				{
					if (reader.IsDeleted(j))
						numDel++;
					else
					{
						reader.Document(j);
						reader.GetTermFreqVectors(j);
					}
				}
				reader.Close();
				Assert.AreEqual(0, numDel);
				
				dir.Close();
			}
		}
		
        [Test]
		public virtual void  TestVariableSchema()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			int delID = 0;
			for (int i = 0; i < 20; i++)
			{
				IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
				writer.SetMaxBufferedDocs(2);
				writer.SetMergeFactor(2);
				writer.SetUseCompoundFile(false);
				Document doc = new Document();
				System.String contents = "aa bb cc dd ee ff gg hh ii jj kk";
				
				if (i == 7)
				{
					// Add empty docs here
					doc.Add(new Field("content3", "", Field.Store.NO, Field.Index.ANALYZED));
				}
				else
				{
					Field.Store storeVal;
					if (i % 2 == 0)
					{
						doc.Add(new Field("content4", contents, Field.Store.YES, Field.Index.ANALYZED));
						storeVal = Field.Store.YES;
					}
					else
						storeVal = Field.Store.NO;
					doc.Add(new Field("content1", contents, storeVal, Field.Index.ANALYZED));
					doc.Add(new Field("content3", "", Field.Store.YES, Field.Index.ANALYZED));
					doc.Add(new Field("content5", "", storeVal, Field.Index.ANALYZED));
				}
				
				for (int j = 0; j < 4; j++)
					writer.AddDocument(doc);
				
				writer.Close();
				IndexReader reader = IndexReader.Open(dir);
				reader.DeleteDocument(delID++);
				reader.Close();
				
				if (0 == i % 4)
				{
					writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
					writer.SetUseCompoundFile(false);
					writer.Optimize();
					writer.Close();
				}
			}
		}
		
        [Test]
		public virtual void  TestNoWaitClose()
		{
			RAMDirectory directory = new MockRAMDirectory();
			
			Document doc = new Document();
			Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
			doc.Add(idField);
			
			for (int pass = 0; pass < 3; pass++)
			{
				bool autoCommit = pass % 2 == 0;
				IndexWriter writer = new IndexWriter(directory, autoCommit, new WhitespaceAnalyzer(), true);
				
				//System.out.println("TEST: pass=" + pass + " ac=" + autoCommit + " cms=" + (pass >= 2));
				for (int iter = 0; iter < 10; iter++)
				{
					//System.out.println("TEST: iter=" + iter);
					MergeScheduler ms;
					if (pass >= 2)
						ms = new ConcurrentMergeScheduler();
					else
						ms = new SerialMergeScheduler();
					
					writer.SetMergeScheduler(ms);
					writer.SetMaxBufferedDocs(2);
					writer.SetMergeFactor(100);
					
					for (int j = 0; j < 199; j++)
					{
						idField.SetValue(System.Convert.ToString(iter * 201 + j));
						writer.AddDocument(doc);
					}
					
					int delID = iter * 199;
					for (int j = 0; j < 20; j++)
					{
						writer.DeleteDocuments(new Term("id", System.Convert.ToString(delID)));
						delID += 5;
					}
					
					// Force a bunch of merge threads to kick off so we
					// stress out aborting them on close:
					writer.SetMergeFactor(2);
					
					IndexWriter finalWriter = writer;
					System.Collections.ArrayList failure = new System.Collections.ArrayList();
					SupportClass.ThreadClass t1 = new AnonymousClassThread1(finalWriter, doc, failure, this);
					
					if (failure.Count > 0)
					{
						throw (System.Exception) failure[0];
					}
					
					t1.Start();
					
					writer.Close(false);
					t1.Join();
					
					// Make sure reader can read
					IndexReader reader = IndexReader.Open(directory);
					reader.Close();
					
					// Reopen
					writer = new IndexWriter(directory, autoCommit, new WhitespaceAnalyzer(), false);
				}
				writer.Close();
			}
			
			directory.Close();
		}
		
		// Used by test cases below
		private class IndexerThread:SupportClass.ThreadClass
		{
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			internal bool diskFull;
			internal System.Exception error;
			internal AlreadyClosedException ace;
			internal IndexWriter writer;
			internal bool noErrors;
			internal volatile int addCount;
			
			public IndexerThread(TestIndexWriter enclosingInstance, IndexWriter writer, bool noErrors)
			{
				InitBlock(enclosingInstance);
				this.writer = writer;
				this.noErrors = noErrors;
			}
			
			override public void  Run()
			{
				
				Document doc = new Document();
				doc.Add(new Field("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
				
				int idUpto = 0;
				int fullCount = 0;
				long stopTime = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) + 500;
				
				while ((DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) < stopTime)
				{
					try
					{
						writer.UpdateDocument(new Term("id", "" + (idUpto++)), doc);
						addCount++;
					}
					catch (System.IO.IOException ioe)
					{
						//System.out.println(Thread.currentThread().getName() + ": hit exc");
						//ioe.printStackTrace(System.out);
						if (ioe.Message.StartsWith("fake disk full at") || ioe.Message.Equals("now failing on purpose"))
						{
							diskFull = true;
							try
							{
								System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 1));
							}
							catch (System.Threading.ThreadInterruptedException ie)
							{
								SupportClass.ThreadClass.Current().Interrupt();
								throw new System.SystemException("", ie);
							}
							if (fullCount++ >= 5)
								break;
						}
						else
						{
							if (noErrors)
							{
								System.Console.Out.WriteLine(SupportClass.ThreadClass.Current().Name + ": ERROR: unexpected IOException:");
								System.Console.Out.WriteLine(ioe.StackTrace);
								error = ioe;
							}
							break;
						}
					}
					catch (System.Exception t)
					{
						//t.printStackTrace(System.out);
						if (noErrors)
						{
							System.Console.Out.WriteLine(SupportClass.ThreadClass.Current().Name + ": ERROR: unexpected Throwable:");
							System.Console.Out.WriteLine(t.StackTrace);
							error = t;
						}
						break;
					}
				}
			}
		}
		
		// LUCENE-1130: make sure we can close() even while
		// threads are trying to add documents.  Strictly
		// speaking, this isn't valid us of Lucene's APIs, but we
		// still want to be robust to this case:
        [Test]
		public virtual void  TestCloseWithThreads()
		{
			int NUM_THREADS = 3;
			
			for (int iter = 0; iter < 20; iter++)
			{
				MockRAMDirectory dir = new MockRAMDirectory();
				IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
				ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
				
				// We expect AlreadyClosedException
				cms.SetSuppressExceptions();
				
				writer.SetMergeScheduler(cms);
				writer.SetMaxBufferedDocs(10);
				writer.SetMergeFactor(4);
				
				IndexerThread[] threads = new IndexerThread[NUM_THREADS];
				
				for (int i = 0; i < NUM_THREADS; i++)
					threads[i] = new IndexerThread(this, writer, false);
				
				for (int i = 0; i < NUM_THREADS; i++)
					threads[i].Start();
				
				bool done = false;
				while (!done)
				{
					System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 100));
					for (int i = 0; i < NUM_THREADS; i++)
					// only stop when at least one thread has added a doc
						if (threads[i].addCount > 0)
						{
							done = true;
							break;
						}
				}
				
				writer.Close(false);
				
				// Make sure threads that are adding docs are not hung:
				for (int i = 0; i < NUM_THREADS; i++)
				{
					// Without fix for LUCENE-1130: one of the
					// threads will hang
					threads[i].Join();
					if (threads[i].IsAlive)
						Assert.Fail("thread seems to be hung");
				}
				
				// Quick test to make sure index is not corrupt:
				IndexReader reader = IndexReader.Open(dir);
				TermDocs tdocs = reader.TermDocs(new Term("field", "aaa"));
				int count = 0;
				while (tdocs.Next())
				{
					count++;
				}
				Assert.IsTrue(count > 0);
				reader.Close();
				
				dir.Close();
			}
		}
		
		// LUCENE-1130: make sure immeidate disk full on creating
		// an IndexWriter (hit during DW.ThreadState.init()) is
		// OK:
        [Test]
		public virtual void  TestImmediateDiskFull()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			dir.SetMaxSizeInBytes(dir.GetRecomputedActualSizeInBytes());
			writer.SetMaxBufferedDocs(2);
			Document doc = new Document();
			doc.Add(new Field("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			try
			{
				writer.AddDocument(doc);
				Assert.Fail("did not hit disk full");
			}
			catch (System.IO.IOException ioe)
			{
			}
			// Without fix for LUCENE-1130: this call will hang:
			try
			{
				writer.AddDocument(doc);
				Assert.Fail("did not hit disk full");
			}
			catch (System.IO.IOException ioe)
			{
			}
			try
			{
				writer.Close(false);
				Assert.Fail("did not hit disk full");
			}
			catch (System.IO.IOException ioe)
			{
			}
		}
		
		// LUCENE-1130: make sure immediate disk full on creating
		// an IndexWriter (hit during DW.ThreadState.init()), with
		// multiple threads, is OK:
        [Test]
		public virtual void  TestImmediateDiskFullWithThreads()
		{
			
			int NUM_THREADS = 3;
			
			for (int iter = 0; iter < 10; iter++)
			{
				MockRAMDirectory dir = new MockRAMDirectory();
				IndexWriter writer = new IndexWriter(dir, true, new WhitespaceAnalyzer());
				ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
				// We expect disk full exceptions in the merge threads
				cms.SetSuppressExceptions();
				writer.SetMergeScheduler(cms);
				writer.SetMaxBufferedDocs(2);
				writer.SetMergeFactor(4);
				dir.SetMaxSizeInBytes(4 * 1024 + 20 * iter);
				
				IndexerThread[] threads = new IndexerThread[NUM_THREADS];
				
				for (int i = 0; i < NUM_THREADS; i++)
					threads[i] = new IndexerThread(this, writer, true);
				
				for (int i = 0; i < NUM_THREADS; i++)
					threads[i].Start();
				
				for (int i = 0; i < NUM_THREADS; i++)
				{
					// Without fix for LUCENE-1130: one of the
					// threads will hang
					threads[i].Join();
					Assert.IsTrue(threads[i].error == null, "hit unexpected Throwable");
				}
				
				try
				{
					writer.Close(false);
				}
				catch (System.IO.IOException ioe)
				{
				}
				
				dir.Close();
			}
		}
		
		// Throws IOException during FieldsWriter.flushDocument and during DocumentsWriter.abort
		private class FailOnlyOnAbortOrFlush:MockRAMDirectory.Failure
		{
			private bool onlyOnce;
			public FailOnlyOnAbortOrFlush(bool onlyOnce)
			{
				this.onlyOnce = onlyOnce;
			}
			public override void  Eval(MockRAMDirectory dir)
			{
				if (doFail)
				{
					System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace();
					for (int i = 0; i < trace.FrameCount; i++)
					{
						System.Diagnostics.StackFrame sf = trace.GetFrame(i);
						if ("Abort".Equals(sf.GetMethod().Name) || "FlushDocument".Equals(sf.GetMethod().Name))
						{
							if (onlyOnce)
								doFail = false;
							//System.out.println(Thread.currentThread().getName() + ": now fail");
							//new Throwable().printStackTrace(System.out);
							throw new System.IO.IOException("now failing on purpose");
						}
					}
				}
			}
		}
		
		// Runs test, with one thread, using the specific failure
		// to trigger an IOException
		public virtual void  _testSingleThreadFailure(MockRAMDirectory.Failure failure)
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, true, new WhitespaceAnalyzer());
			writer.SetMaxBufferedDocs(2);
			Document doc = new Document();
			doc.Add(new Field("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			
			for (int i = 0; i < 6; i++)
				writer.AddDocument(doc);
			
			dir.FailOn(failure);
			failure.SetDoFail();
			try
			{
				writer.AddDocument(doc);
				writer.AddDocument(doc);
				Assert.Fail("did not hit exception");
			}
			catch (System.IO.IOException ioe)
			{
			}
			failure.ClearDoFail();
			writer.AddDocument(doc);
			writer.Close(false);
		}
		
		// Runs test, with multiple threads, using the specific
		// failure to trigger an IOException
		public virtual void  _testMultipleThreadsFailure(MockRAMDirectory.Failure failure)
		{
			
			int NUM_THREADS = 3;
			
			for (int iter = 0; iter < 5; iter++)
			{
				MockRAMDirectory dir = new MockRAMDirectory();
				IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
				ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
				// We expect disk full exceptions in the merge threads
				cms.SetSuppressExceptions();
				writer.SetMergeScheduler(cms);
				writer.SetMaxBufferedDocs(2);
				writer.SetMergeFactor(4);
				
				IndexerThread[] threads = new IndexerThread[NUM_THREADS];
				
				for (int i = 0; i < NUM_THREADS; i++)
					threads[i] = new IndexerThread(this, writer, true);
				
				for (int i = 0; i < NUM_THREADS; i++)
					threads[i].Start();
				
				System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 10));
				
				dir.FailOn(failure);
				failure.SetDoFail();
				
				for (int i = 0; i < NUM_THREADS; i++)
				{
					threads[i].Join();
					Assert.IsTrue(threads[i].error == null, "hit unexpected Throwable");
				}
				
				bool success = false;
				try
				{
					writer.Close(false);
					success = true;
				}
				catch (System.IO.IOException ioe)
				{
					failure.ClearDoFail();
					writer.Close(false);
				}
				
				if (success)
				{
					IndexReader reader = IndexReader.Open(dir);
					for (int j = 0; j < reader.MaxDoc(); j++)
					{
						if (!reader.IsDeleted(j))
						{
							reader.Document(j);
							reader.GetTermFreqVectors(j);
						}
					}
					reader.Close();
				}
				
				dir.Close();
			}
		}
		
		// LUCENE-1130: make sure initial IOException, and then 2nd
		// IOException during abort(), is OK:
        [Test]
		public virtual void  TestIOExceptionDuringAbort()
		{
			_testSingleThreadFailure(new FailOnlyOnAbortOrFlush(false));
		}
		
		// LUCENE-1130: make sure initial IOException, and then 2nd
		// IOException during abort(), is OK:
        [Test]
		public virtual void  TestIOExceptionDuringAbortOnlyOnce()
		{
			_testSingleThreadFailure(new FailOnlyOnAbortOrFlush(true));
		}
		
		// LUCENE-1130: make sure initial IOException, and then 2nd
		// IOException during abort(), with multiple threads, is OK:
        [Test]
		public virtual void  TestIOExceptionDuringAbortWithThreads()
		{
			_testMultipleThreadsFailure(new FailOnlyOnAbortOrFlush(false));
		}
		
		// LUCENE-1130: make sure initial IOException, and then 2nd
		// IOException during abort(), with multiple threads, is OK:
        [Test]
		public virtual void  TestIOExceptionDuringAbortWithThreadsOnlyOnce()
		{
			_testMultipleThreadsFailure(new FailOnlyOnAbortOrFlush(true));
		}
		
		// Throws IOException during DocumentsWriter.closeDocStore
		private class FailOnlyInCloseDocStore:MockRAMDirectory.Failure
		{
			private bool onlyOnce;
			public FailOnlyInCloseDocStore(bool onlyOnce)
			{
				this.onlyOnce = onlyOnce;
			}
			public override void  Eval(MockRAMDirectory dir)
			{
				if (doFail)
				{
					System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace();
					for (int i = 0; i < trace.FrameCount; i++)
					{
						System.Diagnostics.StackFrame sf = trace.GetFrame(i);
						if ("CloseDocStore".Equals(sf.GetMethod().Name))
						{
							if (onlyOnce)
								doFail = false;
							throw new System.IO.IOException("now failing on purpose");
						}
					}
				}
			}
		}
		
		// LUCENE-1130: test IOException in closeDocStore
        [Test]
		public virtual void  TestIOExceptionDuringCloseDocStore()
		{
			_testSingleThreadFailure(new FailOnlyInCloseDocStore(false));
		}
		
		// LUCENE-1130: test IOException in closeDocStore
        [Test]
		public virtual void  TestIOExceptionDuringCloseDocStoreOnlyOnce()
		{
			_testSingleThreadFailure(new FailOnlyInCloseDocStore(true));
		}
		
		// LUCENE-1130: test IOException in closeDocStore, with threads
        [Test]
		public virtual void  TestIOExceptionDuringCloseDocStoreWithThreads()
		{
			_testMultipleThreadsFailure(new FailOnlyInCloseDocStore(false));
		}
		
		// LUCENE-1130: test IOException in closeDocStore, with threads
        [Test]
		public virtual void  TestIOExceptionDuringCloseDocStoreWithThreadsOnlyOnce()
		{
			_testMultipleThreadsFailure(new FailOnlyInCloseDocStore(true));
		}
		
		// Throws IOException during DocumentsWriter.writeSegment
		private class FailOnlyInWriteSegment:MockRAMDirectory.Failure
		{
			private bool onlyOnce;
			public FailOnlyInWriteSegment(bool onlyOnce)
			{
				this.onlyOnce = onlyOnce;
			}
			public override void  Eval(MockRAMDirectory dir)
			{
				if (doFail)
				{
					System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace();
					for (int i = 0; i < trace.FrameCount; i++)
					{
						System.Diagnostics.StackFrame sf = trace.GetFrame(i);
                        string className = sf.GetMethod().DeclaringType.Namespace + "." + sf.GetMethod().DeclaringType.Name;
						if ("Flush".Equals(sf.GetMethod().Name) && "Lucene.Net.Index.DocFieldProcessor".Equals(className))
						{
							if (onlyOnce)
								doFail = false;
							throw new System.IO.IOException("now failing on purpose");
						}
					}
				}
			}
		}
		
		// LUCENE-1130: test IOException in writeSegment
        [Test]
		public virtual void  TestIOExceptionDuringWriteSegment()
		{
			_testSingleThreadFailure(new FailOnlyInWriteSegment(false));
		}
		
		// LUCENE-1130: test IOException in writeSegment
        [Test]
		public virtual void  TestIOExceptionDuringWriteSegmentOnlyOnce()
		{
			_testSingleThreadFailure(new FailOnlyInWriteSegment(true));
		}
		
		// LUCENE-1130: test IOException in writeSegment, with threads
        [Test]
		public virtual void  TestIOExceptionDuringWriteSegmentWithThreads()
		{
			_testMultipleThreadsFailure(new FailOnlyInWriteSegment(false));
		}
		
		// LUCENE-1130: test IOException in writeSegment, with threads
        [Test]
		public virtual void  TestIOExceptionDuringWriteSegmentWithThreadsOnlyOnce()
		{
			_testMultipleThreadsFailure(new FailOnlyInWriteSegment(true));
		}
		
		// LUCENE-1084: test unlimited field length
        [Test]
		public virtual void  TestUnlimitedMaxFieldLength()
		{
			Directory dir = new MockRAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			
			Document doc = new Document();
			System.Text.StringBuilder b = new System.Text.StringBuilder();
			for (int i = 0; i < 10000; i++)
				b.Append(" a");
			b.Append(" x");
			doc.Add(new Field("field", b.ToString(), Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			Term t = new Term("field", "x");
			Assert.AreEqual(1, reader.DocFreq(t));
			reader.Close();
			dir.Close();
		}
		
		// LUCENE-1044: Simulate checksum error in segments_N
        [Test]
		public virtual void  TestSegmentsChecksumError()
		{
			Directory dir = new MockRAMDirectory();
			
			IndexWriter writer = null;
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			// add 100 documents
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer);
			}
			
			// close
			writer.Close();
			
			long gen = SegmentInfos.GetCurrentSegmentGeneration(dir);
			Assert.IsTrue(gen > 1, "segment generation should be > 1 but got " + gen);
			
			System.String segmentsFileName = SegmentInfos.GetCurrentSegmentFileName(dir);
			IndexInput in_Renamed = dir.OpenInput(segmentsFileName);
			IndexOutput out_Renamed = dir.CreateOutput(IndexFileNames.FileNameFromGeneration(IndexFileNames.SEGMENTS, "", 1 + gen));
			out_Renamed.CopyBytes(in_Renamed, in_Renamed.Length() - 1);
			byte b = in_Renamed.ReadByte();
			out_Renamed.WriteByte((byte) (1 + b));
			out_Renamed.Close();
			in_Renamed.Close();
			
			IndexReader reader = null;
			try
			{
				reader = IndexReader.Open(dir);
			}
			catch (System.IO.IOException e)
			{
				System.Console.Out.WriteLine(e.StackTrace);
				Assert.Fail("segmentInfos failed to retry fallback to correct segments_N file");
			}
			reader.Close();
		}
		
		// LUCENE-1044: test writer.commit() when ac=false
        [Test]
		public virtual void  TestForceCommit()
		{
			Directory dir = new MockRAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			writer.SetMergeFactor(5);
			
			for (int i = 0; i < 23; i++)
				AddDoc(writer);
			
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(0, reader.NumDocs());
			writer.Commit();
			IndexReader reader2 = reader.Reopen();
			Assert.AreEqual(0, reader.NumDocs());
			Assert.AreEqual(23, reader2.NumDocs());
			reader.Close();
			
			for (int i = 0; i < 17; i++)
				AddDoc(writer);
			Assert.AreEqual(23, reader2.NumDocs());
			reader2.Close();
			reader = IndexReader.Open(dir);
			Assert.AreEqual(23, reader.NumDocs());
			reader.Close();
			writer.Commit();
			
			reader = IndexReader.Open(dir);
			Assert.AreEqual(40, reader.NumDocs());
			reader.Close();
			writer.Close();
			dir.Close();
		}
		
		// Throws IOException during MockRAMDirectory.sync
		private class FailOnlyInSync:MockRAMDirectory.Failure
		{
			internal bool didFail;
			public override void  Eval(MockRAMDirectory dir)
			{
				if (doFail)
				{
					System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace();
					for (int i = 0; i < trace.FrameCount; i++)
					{
						System.Diagnostics.StackFrame sf = trace.GetFrame(i);
                        string className = sf.GetMethod().DeclaringType.Namespace + "." + sf.GetMethod().DeclaringType.Name;
						if (doFail && "Lucene.Net.Store.MockRAMDirectory".Equals(className) && "Sync".Equals(sf.GetMethod().Name))
						{
							didFail = true;
							throw new System.IO.IOException("now failing on purpose during sync");
						}
					}
				}
			}
		}
		
		// LUCENE-1044: test exception during sync
        [Test]
		public virtual void  TestExceptionDuringSync()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			FailOnlyInSync failure = new FailOnlyInSync();
			dir.FailOn(failure);
			
			IndexWriter writer = new IndexWriter(dir, true, new WhitespaceAnalyzer());
			failure.SetDoFail();
			
			ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
			// We expect sync exceptions in the merge threads
			cms.SetSuppressExceptions();
			writer.SetMergeScheduler(cms);
			writer.SetMaxBufferedDocs(2);
			writer.SetMergeFactor(5);
			
			for (int i = 0; i < 23; i++)
				AddDoc(writer);
			
			cms.Sync();
			Assert.IsTrue(failure.didFail);
			failure.ClearDoFail();
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(23, reader.NumDocs());
			reader.Close();
			dir.Close();
		}
		
		// LUCENE-1168
        [Test]
		public virtual void  TestTermVectorCorruption()
		{
			
			Directory dir = new MockRAMDirectory();
			for (int iter = 0; iter < 4; iter++)
			{
				bool autoCommit = 1 == iter / 2;
				IndexWriter writer = new IndexWriter(dir, autoCommit, new StandardAnalyzer());
				writer.SetMaxBufferedDocs(2);
				writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
				writer.SetMergeScheduler(new SerialMergeScheduler());
				writer.SetMergePolicy(new LogDocMergePolicy(writer));
				
				Document document = new Document();
				
				Field storedField = new Field("stored", "stored", Field.Store.YES, Field.Index.NO);
				document.Add(storedField);
				writer.AddDocument(document);
				writer.AddDocument(document);
				
				document = new Document();
				document.Add(storedField);
				Field termVectorField = new Field("termVector", "termVector", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
				
				document.Add(termVectorField);
				writer.AddDocument(document);
				writer.Optimize();
				writer.Close();
				
				IndexReader reader = IndexReader.Open(dir);
				for (int i = 0; i < reader.NumDocs(); i++)
				{
					reader.Document(i);
					reader.GetTermFreqVectors(i);
				}
				reader.Close();
				
				writer = new IndexWriter(dir, autoCommit, new StandardAnalyzer());
				writer.SetMaxBufferedDocs(2);
				writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
				writer.SetMergeScheduler(new SerialMergeScheduler());
				writer.SetMergePolicy(new LogDocMergePolicy(writer));
				
				Directory[] indexDirs = new Directory[]{new MockRAMDirectory(dir)};
				writer.AddIndexes(indexDirs);
				writer.Close();
			}
			dir.Close();
		}
		
		// LUCENE-1168
        [Test]
		public virtual void  TestTermVectorCorruption2()
		{
			Directory dir = new MockRAMDirectory();
			for (int iter = 0; iter < 4; iter++)
			{
				bool autoCommit = 1 == iter / 2;
				IndexWriter writer = new IndexWriter(dir, autoCommit, new StandardAnalyzer());
				writer.SetMaxBufferedDocs(2);
				writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
				writer.SetMergeScheduler(new SerialMergeScheduler());
				writer.SetMergePolicy(new LogDocMergePolicy(writer));
				
				Document document = new Document();
				
				Field storedField = new Field("stored", "stored", Field.Store.YES, Field.Index.NO);
				document.Add(storedField);
				writer.AddDocument(document);
				writer.AddDocument(document);
				
				document = new Document();
				document.Add(storedField);
				Field termVectorField = new Field("termVector", "termVector", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
				document.Add(termVectorField);
				writer.AddDocument(document);
				writer.Optimize();
				writer.Close();
				
				IndexReader reader = IndexReader.Open(dir);
				Assert.IsTrue(reader.GetTermFreqVectors(0) == null);
				Assert.IsTrue(reader.GetTermFreqVectors(1) == null);
				Assert.IsTrue(reader.GetTermFreqVectors(2) != null);
				reader.Close();
			}
			dir.Close();
		}
		
		// LUCENE-1168
        [Test]
		public virtual void  TestTermVectorCorruption3()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
			writer.SetMergeScheduler(new SerialMergeScheduler());
			writer.SetMergePolicy(new LogDocMergePolicy(writer));
			
			Document document = new Document();
			
			document = new Document();
			Field storedField = new Field("stored", "stored", Field.Store.YES, Field.Index.NO);
			document.Add(storedField);
			Field termVectorField = new Field("termVector", "termVector", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			document.Add(termVectorField);
			for (int i = 0; i < 10; i++)
				writer.AddDocument(document);
			writer.Close();
			
			writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
			writer.SetMergeScheduler(new SerialMergeScheduler());
			writer.SetMergePolicy(new LogDocMergePolicy(writer));
			for (int i = 0; i < 6; i++)
				writer.AddDocument(document);
			
			writer.Optimize();
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			for (int i = 0; i < 10; i++)
			{
				reader.GetTermFreqVectors(i);
				reader.Document(i);
			}
			reader.Close();
			dir.Close();
		}
		
		// LUCENE-1084: test user-specified field length
        [Test]
		public virtual void  TestUserSpecifiedMaxFieldLength()
		{
			Directory dir = new MockRAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), new IndexWriter.MaxFieldLength(100000));
			
			Document doc = new Document();
			System.Text.StringBuilder b = new System.Text.StringBuilder();
			for (int i = 0; i < 10000; i++)
				b.Append(" a");
			b.Append(" x");
			doc.Add(new Field("field", b.ToString(), Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			Term t = new Term("field", "x");
			Assert.AreEqual(1, reader.DocFreq(t));
			reader.Close();
			dir.Close();
		}
		
		// LUCENE-325: test expungeDeletes, when 2 singular merges
		// are required
        [Test]
		public virtual void  TestExpungeDeletes()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
			
			Document document = new Document();
			
			document = new Document();
			Field storedField = new Field("stored", "stored", Field.Store.YES, Field.Index.NO);
			document.Add(storedField);
			Field termVectorField = new Field("termVector", "termVector", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			document.Add(termVectorField);
			for (int i = 0; i < 10; i++)
				writer.AddDocument(document);
			writer.Close();
			
			IndexReader ir = IndexReader.Open(dir);
			Assert.AreEqual(10, ir.MaxDoc());
			Assert.AreEqual(10, ir.NumDocs());
			ir.DeleteDocument(0);
			ir.DeleteDocument(7);
			Assert.AreEqual(8, ir.NumDocs());
			ir.Close();
			
			writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Assert.AreEqual(8, writer.NumDocs());
			Assert.AreEqual(10, writer.MaxDoc());
			writer.ExpungeDeletes();
			Assert.AreEqual(8, writer.NumDocs());
			writer.Close();
			ir = IndexReader.Open(dir);
			Assert.AreEqual(8, ir.MaxDoc());
			Assert.AreEqual(8, ir.NumDocs());
			ir.Close();
			dir.Close();
		}
		
		// LUCENE-325: test expungeDeletes, when many adjacent merges are required
        [Test]
		public virtual void  TestExpungeDeletes2()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			writer.SetMergeFactor(50);
			writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
			
			Document document = new Document();
			
			document = new Document();
			Field storedField = new Field("stored", "stored", Field.Store.YES, Field.Index.NO);
			document.Add(storedField);
			Field termVectorField = new Field("termVector", "termVector", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			document.Add(termVectorField);
			for (int i = 0; i < 98; i++)
				writer.AddDocument(document);
			writer.Close();
			
			IndexReader ir = IndexReader.Open(dir);
			Assert.AreEqual(98, ir.MaxDoc());
			Assert.AreEqual(98, ir.NumDocs());
			for (int i = 0; i < 98; i += 2)
				ir.DeleteDocument(i);
			Assert.AreEqual(49, ir.NumDocs());
			ir.Close();
			
			writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMergeFactor(3);
			Assert.AreEqual(49, writer.NumDocs());
			writer.ExpungeDeletes();
			writer.Close();
			ir = IndexReader.Open(dir);
			Assert.AreEqual(49, ir.MaxDoc());
			Assert.AreEqual(49, ir.NumDocs());
			ir.Close();
			dir.Close();
		}
		
		// LUCENE-325: test expungeDeletes without waiting, when
		// many adjacent merges are required
        [Test]
		public virtual void  TestExpungeDeletes3()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			writer.SetMergeFactor(50);
			writer.SetRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
			
			Document document = new Document();
			
			document = new Document();
			Field storedField = new Field("stored", "stored", Field.Store.YES, Field.Index.NO);
			document.Add(storedField);
			Field termVectorField = new Field("termVector", "termVector", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			document.Add(termVectorField);
			for (int i = 0; i < 98; i++)
				writer.AddDocument(document);
			writer.Close();
			
			IndexReader ir = IndexReader.Open(dir);
			Assert.AreEqual(98, ir.MaxDoc());
			Assert.AreEqual(98, ir.NumDocs());
			for (int i = 0; i < 98; i += 2)
				ir.DeleteDocument(i);
			Assert.AreEqual(49, ir.NumDocs());
			ir.Close();
			
			writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			// Force many merges to happen
			writer.SetMergeFactor(3);
			writer.ExpungeDeletes(false);
			writer.Close();
			ir = IndexReader.Open(dir);
			Assert.AreEqual(49, ir.MaxDoc());
			Assert.AreEqual(49, ir.NumDocs());
			ir.Close();
			dir.Close();
		}
		
		// LUCENE-1179
        [Test]
		public virtual void  TestEmptyFieldName()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer());
			Document doc = new Document();
			doc.Add(new Field("", "a b c", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.Close();
		}
		
		// LUCENE-1198
		public class MockIndexWriter:IndexWriter
		{
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public MockIndexWriter(TestIndexWriter enclosingInstance, Directory dir, Analyzer a, bool create, MaxFieldLength mfl):base(dir, a, create, mfl)
			{
				InitBlock(enclosingInstance);
			}
			
			internal bool doFail;
			
			public override bool TestPoint(System.String name)
			{
				if (doFail && name.Equals("DocumentsWriter.ThreadState.init start"))
					throw new System.SystemException("intentionally failing");
				return true;
			}
		}
		
        [Test]
		public virtual void  TestExceptionDocumentsWriterInit()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			MockIndexWriter w = new MockIndexWriter(this, dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "a field", Field.Store.YES, Field.Index.ANALYZED));
			w.AddDocument(doc);
			w.doFail = true;
			try
			{
				w.AddDocument(doc);

                if (SupportClass.BuildType.Debug)
                    Assert.Fail("did not hit exception");
                else
                    Assert.Ignore("This test is not executed in release mode");
            }
			catch (System.SystemException re)
			{
				// expected
			}
			w.Close();
			_TestUtil.CheckIndex(dir);
			dir.Close();
		}
		
		// LUCENE-1208
        [Test]
		public virtual void  TestExceptionJustBeforeFlush()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			MockIndexWriter w = new MockIndexWriter(this, dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
			w.SetMaxBufferedDocs(2);
			Document doc = new Document();
			doc.Add(new Field("field", "a field", Field.Store.YES, Field.Index.ANALYZED));
			w.AddDocument(doc);
			
			Analyzer analyzer = new AnonymousClassAnalyzer3(this);
			
			Document crashDoc = new Document();
			crashDoc.Add(new Field("crash", "do it on token 4", Field.Store.YES, Field.Index.ANALYZED));
			try
			{
				w.AddDocument(crashDoc, analyzer);
				Assert.Fail("did not hit expected exception");
			}
			catch (System.IO.IOException ioe)
			{
				// expected
			}
			w.AddDocument(doc);
			w.Close();
			dir.Close();
		}
		
		public class MockIndexWriter2:IndexWriter
		{
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public MockIndexWriter2(TestIndexWriter enclosingInstance, Directory dir, Analyzer a, bool create, MaxFieldLength mfl):base(dir, a, create, mfl)
			{
				InitBlock(enclosingInstance);
			}
			
			internal bool doFail;
			internal bool failed;
			
			public override bool TestPoint(System.String name)
			{
				if (doFail && name.Equals("startMergeInit"))
				{
					failed = true;
					throw new System.SystemException("intentionally failing");
				}
				return true;
			}
		}
		
		// LUCENE-1210
        [Test]
		public virtual void  TestExceptionOnMergeInit()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			MockIndexWriter2 w = new MockIndexWriter2(this, dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
			w.SetMaxBufferedDocs(2);
			w.SetMergeFactor(2);
			w.doFail = true;
			w.SetMergeScheduler(new ConcurrentMergeScheduler());
			Document doc = new Document();
			doc.Add(new Field("field", "a field", Field.Store.YES, Field.Index.ANALYZED));
			for (int i = 0; i < 10; i++)
				try
				{
					w.AddDocument(doc);
				}
				catch (System.SystemException re)
				{
					break;
				}
			
			((ConcurrentMergeScheduler) w.GetMergeScheduler()).Sync();
            if (SupportClass.BuildType.Debug)
                Assert.IsTrue(w.failed);
            else
                Assert.Ignore("This test is not executed in release mode");
			w.Close();
			dir.Close();
		}
		
		public class MockIndexWriter3:IndexWriter
		{
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public MockIndexWriter3(TestIndexWriter enclosingInstance, Directory dir, Analyzer a, bool create, IndexWriter.MaxFieldLength mfl):base(dir, a, create, mfl)
			{
				InitBlock(enclosingInstance);
			}
			
			internal bool wasCalled;
			
			protected override void  DoAfterFlush()
			{
				wasCalled = true;
			}
		}
		
		// LUCENE-1222
        [Test]
		public virtual void  TestDoAfterFlush()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			MockIndexWriter3 w = new MockIndexWriter3(this, dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "a field", Field.Store.YES, Field.Index.ANALYZED));
			w.AddDocument(doc);
			w.Commit();
			Assert.IsTrue(w.wasCalled);
			w.wasCalled = true;
			w.DeleteDocuments(new Term("field", "field"));
			w.Commit();
			Assert.IsTrue(w.wasCalled);
			w.Close();
			
			IndexReader ir = IndexReader.Open(dir);
			Assert.AreEqual(1, ir.MaxDoc());
			Assert.AreEqual(0, ir.NumDocs());
			ir.Close();
			
			dir.Close();
		}
		
		private class FailOnlyInCommit:MockRAMDirectory.Failure
		{
			
			internal bool fail1, fail2;
			
			public override void  Eval(MockRAMDirectory dir)
			{
				System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace();
				bool isCommit = false;
				bool isDelete = false;
				for (int i = 0; i < trace.FrameCount; i++)
				{
					System.Diagnostics.StackFrame sf = trace.GetFrame(i);
                    string className = sf.GetMethod().DeclaringType.Namespace + "." + sf.GetMethod().DeclaringType.Name;
					if ("Lucene.Net.Index.SegmentInfos".Equals(className) && "PrepareCommit".Equals(sf.GetMethod().Name))
						isCommit = true;
					if ("Lucene.Net.Store.MockRAMDirectory".Equals(className) && "DeleteFile".Equals(sf.GetMethod().Name))
						isDelete = true;
				}
				
				if (isCommit)
				{
					if (!isDelete)
					{
						fail1 = true;
						throw new System.SystemException("now fail first");
					}
					else
					{
						fail2 = true;
						throw new System.IO.IOException("now fail during delete");
					}
				}
			}
		}
		
		// LUCENE-1214
        [Test]
		public virtual void  TestExceptionsDuringCommit()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			FailOnlyInCommit failure = new FailOnlyInCommit();
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "a field", Field.Store.YES, Field.Index.ANALYZED));
			w.AddDocument(doc);
			dir.FailOn(failure);
			try
			{
				w.Close();
				Assert.Fail();
			}
			catch (System.IO.IOException ioe)
			{
				Assert.Fail("expected only RuntimeException");
			}
			catch (System.SystemException re)
			{
				// Expected
			}
			Assert.IsTrue(failure.fail1 && failure.fail2);
			w.Abort();
			dir.Close();
		}
		
		internal System.String[] utf8Data = new System.String[]{"ab\udc17cd", "ab\ufffdcd", "\udc17abcd", "\ufffdabcd", "\udc17", "\ufffd", "ab\udc17\udc17cd", "ab\ufffd\ufffdcd", "\udc17\udc17abcd", "\ufffd\ufffdabcd", "\udc17\udc17", "\ufffd\ufffd", "ab\ud917cd", "ab\ufffdcd", "\ud917abcd", "\ufffdabcd", "\ud917", "\ufffd", "ab\ud917\ud917cd", "ab\ufffd\ufffdcd", "\ud917\ud917abcd", "\ufffd\ufffdabcd", "\ud917\ud917", "\ufffd\ufffd", "ab\udc17\ud917cd", "ab\ufffd\ufffdcd", "\udc17\ud917abcd", "\ufffd\ufffdabcd", "\udc17\ud917", "\ufffd\ufffd", "ab\udc17\ud917\udc17\ud917cd", "ab\ufffd\ud917\udc17\ufffdcd", "\udc17\ud917\udc17\ud917abcd", "\ufffd\ud917\udc17\ufffdabcd", "\udc17\ud917\udc17\ud917", "\ufffd\ud917\udc17\ufffd"};
		
		// LUCENE-510
        [Test]
		public virtual void  TestInvalidUTF16()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			
			int count = utf8Data.Length / 2;
			for (int i = 0; i < count; i++)
				doc.Add(new Field("f" + i, utf8Data[2 * i], Field.Store.YES, Field.Index.ANALYZED));
			w.AddDocument(doc);
			w.Close();
			
			IndexReader ir = IndexReader.Open(dir);
			Document doc2 = ir.Document(0);
			for (int i = 0; i < count; i++)
			{
				Assert.AreEqual(1, ir.DocFreq(new Term("f" + i, utf8Data[2 * i + 1])), "field " + i + " was not indexed correctly");
				Assert.AreEqual(utf8Data[2 * i + 1], doc2.GetField("f" + i).StringValue(), "field " + i + " is incorrect");
			}
			ir.Close();
			dir.Close();
		}
		
		// LUCENE-510
        [Test]
		public virtual void  TestAllUnicodeChars()
		{
			
			UnicodeUtil.UTF8Result utf8 = new UnicodeUtil.UTF8Result();
			UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();
			char[] chars = new char[2];
			for (int ch = 0; ch < 0x0010FFFF; ch++)
			{
				
				if (ch == 0xd800)
				// Skip invalid code points
					ch = 0xe000;
				
				int len = 0;
				if (ch <= 0xffff)
				{
					chars[len++] = (char) ch;
				}
				else
				{
					chars[len++] = (char) (((ch - 0x0010000) >> 10) + UnicodeUtil.UNI_SUR_HIGH_START);
					chars[len++] = (char) (((ch - 0x0010000) & 0x3FFL) + UnicodeUtil.UNI_SUR_LOW_START);
				}
				
				UnicodeUtil.UTF16toUTF8(chars, 0, len, utf8);
				
				System.String s1 = new System.String(chars, 0, len);
				System.String s2 = System.Text.Encoding.UTF8.GetString(utf8.result, 0, utf8.length);
				Assert.AreEqual(s1, s2, "codepoint " + ch);
				
				UnicodeUtil.UTF8toUTF16(utf8.result, 0, utf8.length, utf16);
				Assert.AreEqual(s1, new String(utf16.result, 0, utf16.length), "codepoint " + ch);
				
				byte[] b = System.Text.Encoding.GetEncoding("UTF-8").GetBytes(s1);
				Assert.AreEqual(utf8.length, b.Length);
				for (int j = 0; j < utf8.length; j++)
					Assert.AreEqual(utf8.result[j], b[j]);
			}
		}
		
		internal System.Random r;
		
		private int NextInt(int lim)
		{
			return r.Next(lim);
		}
		
		private int NextInt(int start, int end)
		{
			return start + NextInt(end - start);
		}
		
		private bool FillUnicode(char[] buffer, char[] expected, int offset, int count)
		{
			int len = offset + count;
			bool hasIllegal = false;
			
			if (offset > 0 && buffer[offset] >= 0xdc00 && buffer[offset] < 0xe000)
			// Don't start in the middle of a valid surrogate pair
				offset--;
			
			for (int i = offset; i < len; i++)
			{
				int t = NextInt(6);
				if (0 == t && i < len - 1)
				{
					// Make a surrogate pair
					// High surrogate
					expected[i] = buffer[i++] = (char) NextInt(0xd800, 0xdc00);
					// Low surrogate
					expected[i] = buffer[i] = (char) NextInt(0xdc00, 0xe000);
				}
				else if (t <= 1)
					expected[i] = buffer[i] = (char) NextInt(0x80);
				else if (2 == t)
					expected[i] = buffer[i] = (char) NextInt(0x80, 0x800);
				else if (3 == t)
					expected[i] = buffer[i] = (char) NextInt(0x800, 0xd800);
				else if (4 == t)
					expected[i] = buffer[i] = (char) NextInt(0xe000, 0xffff);
				else if (5 == t && i < len - 1)
				{
					// Illegal unpaired surrogate
					if (NextInt(10) == 7)
					{
						if (r.NextDouble() > 0.5)
							buffer[i] = (char) NextInt(0xd800, 0xdc00);
						else
							buffer[i] = (char) NextInt(0xdc00, 0xe000);
						expected[i++] = (char) (0xfffd);
						expected[i] = buffer[i] = (char) NextInt(0x800, 0xd800);
						hasIllegal = true;
					}
					else
						expected[i] = buffer[i] = (char) NextInt(0x800, 0xd800);
				}
				else
				{
					expected[i] = buffer[i] = ' ';
				}
			}
			
			return hasIllegal;
		}
		
		// LUCENE-510
        [Test]
		public virtual void  TestRandomUnicodeStrings()
		{
			r = NewRandom();
			
			char[] buffer = new char[20];
			char[] expected = new char[20];
			
			UnicodeUtil.UTF8Result utf8 = new UnicodeUtil.UTF8Result();
			UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();
			
			for (int iter = 0; iter < 100000; iter++)
			{
				bool hasIllegal = FillUnicode(buffer, expected, 0, 20);
				
				UnicodeUtil.UTF16toUTF8(buffer, 0, 20, utf8);
				if (!hasIllegal)
				{
					byte[] b = System.Text.Encoding.GetEncoding("UTF-8").GetBytes(new System.String(buffer, 0, 20));
					Assert.AreEqual(b.Length, utf8.length);
					for (int i = 0; i < b.Length; i++)
						Assert.AreEqual(b[i], utf8.result[i]);
				}
				
				UnicodeUtil.UTF8toUTF16(utf8.result, 0, utf8.length, utf16);
				Assert.AreEqual(utf16.length, 20);
				for (int i = 0; i < 20; i++)
					Assert.AreEqual(expected[i], utf16.result[i]);
			}
		}
		
		// LUCENE-510
        [Test]
		public virtual void  TestIncrementalUnicodeStrings()
		{
			r = NewRandom();
			char[] buffer = new char[20];
			char[] expected = new char[20];
			
			UnicodeUtil.UTF8Result utf8 = new UnicodeUtil.UTF8Result();
			UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();
			UnicodeUtil.UTF16Result utf16a = new UnicodeUtil.UTF16Result();
			
			bool hasIllegal = false;
			byte[] last = new byte[60];
			
			for (int iter = 0; iter < 100000; iter++)
			{
				
				int prefix;
				
				if (iter == 0 || hasIllegal)
					prefix = 0;
				else
					prefix = NextInt(20);
				
				hasIllegal = FillUnicode(buffer, expected, prefix, 20 - prefix);
				
				UnicodeUtil.UTF16toUTF8(buffer, 0, 20, utf8);
				if (!hasIllegal)
				{
					byte[] b = System.Text.Encoding.GetEncoding("UTF-8").GetBytes(new System.String(buffer, 0, 20));
					Assert.AreEqual(b.Length, utf8.length);
					for (int i = 0; i < b.Length; i++)
						Assert.AreEqual(b[i], utf8.result[i]);
				}
				
				int bytePrefix = 20;
				if (iter == 0 || hasIllegal)
					bytePrefix = 0;
				else
					for (int i = 0; i < 20; i++)
						if (last[i] != utf8.result[i])
						{
							bytePrefix = i;
							break;
						}
				System.Array.Copy(utf8.result, 0, last, 0, utf8.length);
				
				UnicodeUtil.UTF8toUTF16(utf8.result, bytePrefix, utf8.length - bytePrefix, utf16);
				Assert.AreEqual(20, utf16.length);
				for (int i = 0; i < 20; i++)
					Assert.AreEqual(expected[i], utf16.result[i]);
				
				UnicodeUtil.UTF8toUTF16(utf8.result, 0, utf8.length, utf16a);
				Assert.AreEqual(20, utf16a.length);
				for (int i = 0; i < 20; i++)
					Assert.AreEqual(expected[i], utf16a.result[i]);
			}
		}
		
		// LUCENE-1255
        [Test]
		public virtual void  TestNegativePositions()
		{
			TokenStream tokens = new AnonymousClassTokenStream(this);
			
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", tokens));
			w.AddDocument(doc);
			w.Commit();
			
			IndexSearcher s = new IndexSearcher(dir);
			PhraseQuery pq = new PhraseQuery();
			pq.Add(new Term("field", "a"));
			pq.Add(new Term("field", "b"));
			pq.Add(new Term("field", "c"));
			ScoreDoc[] hits = s.Search(pq, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			Query q = new SpanTermQuery(new Term("field", "a"));
			hits = s.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			TermPositions tps = s.GetIndexReader().TermPositions(new Term("field", "a"));
			Assert.IsTrue(tps.Next());
			Assert.AreEqual(1, tps.Freq());
			Assert.AreEqual(0, tps.NextPosition());
			w.Close();
			
			Assert.IsTrue(_TestUtil.CheckIndex(dir));
			s.Close();
			dir.Close();
		}
		
		// LUCENE-1274: test writer.prepareCommit()
        [Test]
		public virtual void  TestPrepareCommit()
		{
			Directory dir = new MockRAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			writer.SetMergeFactor(5);
			
			for (int i = 0; i < 23; i++)
				AddDoc(writer);
			
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(0, reader.NumDocs());
			
			writer.PrepareCommit();
			
			IndexReader reader2 = IndexReader.Open(dir);
			Assert.AreEqual(0, reader2.NumDocs());
			
			writer.Commit();
			
			IndexReader reader3 = reader.Reopen();
			Assert.AreEqual(0, reader.NumDocs());
			Assert.AreEqual(0, reader2.NumDocs());
			Assert.AreEqual(23, reader3.NumDocs());
			reader.Close();
			reader2.Close();
			
			for (int i = 0; i < 17; i++)
				AddDoc(writer);
			
			Assert.AreEqual(23, reader3.NumDocs());
			reader3.Close();
			reader = IndexReader.Open(dir);
			Assert.AreEqual(23, reader.NumDocs());
			reader.Close();
			
			writer.PrepareCommit();
			
			reader = IndexReader.Open(dir);
			Assert.AreEqual(23, reader.NumDocs());
			reader.Close();
			
			writer.Commit();
			reader = IndexReader.Open(dir);
			Assert.AreEqual(40, reader.NumDocs());
			reader.Close();
			writer.Close();
			dir.Close();
		}
		
		// LUCENE-1274: test writer.prepareCommit()
        [Test]
		public virtual void  TestPrepareCommitRollback()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			dir.SetPreventDoubleWrite(false);
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			
			writer.SetMaxBufferedDocs(2);
			writer.SetMergeFactor(5);
			
			for (int i = 0; i < 23; i++)
				AddDoc(writer);
			
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(0, reader.NumDocs());
			
			writer.PrepareCommit();
			
			IndexReader reader2 = IndexReader.Open(dir);
			Assert.AreEqual(0, reader2.NumDocs());
			
			writer.Rollback();
			
			IndexReader reader3 = reader.Reopen();
			Assert.AreEqual(0, reader.NumDocs());
			Assert.AreEqual(0, reader2.NumDocs());
			Assert.AreEqual(0, reader3.NumDocs());
			reader.Close();
			reader2.Close();
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 17; i++)
				AddDoc(writer);
			
			Assert.AreEqual(0, reader3.NumDocs());
			reader3.Close();
			reader = IndexReader.Open(dir);
			Assert.AreEqual(0, reader.NumDocs());
			reader.Close();
			
			writer.PrepareCommit();
			
			reader = IndexReader.Open(dir);
			Assert.AreEqual(0, reader.NumDocs());
			reader.Close();
			
			writer.Commit();
			reader = IndexReader.Open(dir);
			Assert.AreEqual(17, reader.NumDocs());
			reader.Close();
			writer.Close();
			dir.Close();
		}
		
		// LUCENE-1274
        [Test]
		public virtual void  TestPrepareCommitNoChanges()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.PrepareCommit();
			writer.Commit();
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(0, reader.NumDocs());
			reader.Close();
			dir.Close();
		}
		
		private abstract class RunAddIndexesThreads
		{
			private class AnonymousClassThread2:SupportClass.ThreadClass
			{
				public AnonymousClassThread2(int numIter, RunAddIndexesThreads enclosingInstance)
				{
					InitBlock(numIter, enclosingInstance);
				}
				private void  InitBlock(int numIter, RunAddIndexesThreads enclosingInstance)
				{
					this.numIter = numIter;
					this.enclosingInstance = enclosingInstance;
				}
				private int numIter;
				private RunAddIndexesThreads enclosingInstance;
				public RunAddIndexesThreads Enclosing_Instance
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
						
						Directory[] dirs = new Directory[Enclosing_Instance.NUM_COPY];
						for (int k = 0; k < Enclosing_Instance.NUM_COPY; k++)
							dirs[k] = new MockRAMDirectory(Enclosing_Instance.dir);
						
						int j = 0;
						
						while (true)
						{
							// System.out.println(Thread.currentThread().getName() + ": iter j=" + j);
							if (numIter > 0 && j == numIter)
								break;
							Enclosing_Instance.DoBody(j++, dirs);
						}
					}
					catch (System.Exception t)
					{
						Enclosing_Instance.Handle(t);
					}
				}
			}
			private void  InitBlock()
			{
				threads = new SupportClass.ThreadClass[NUM_THREADS];
			}
			
			internal Directory dir, dir2;
			internal const int NUM_INIT_DOCS = 17;
			internal IndexWriter writer2;
			internal System.Collections.IList failures = new System.Collections.ArrayList();
			internal volatile bool didClose;
			internal IndexReader[] readers;
			internal int NUM_COPY;
			internal const int NUM_THREADS = 5;
			internal SupportClass.ThreadClass[] threads;
			internal ConcurrentMergeScheduler cms;
			
			public RunAddIndexesThreads(int numCopy)
			{
				NUM_COPY = numCopy;
				dir = new MockRAMDirectory();
				IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
				writer.SetMaxBufferedDocs(2);
				for (int i = 0; i < NUM_INIT_DOCS; i++)
					AddDoc(writer);
				writer.Close();
				
				dir2 = new MockRAMDirectory();
				writer2 = new IndexWriter(dir2, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
				cms = (ConcurrentMergeScheduler) writer2.GetMergeScheduler();
				
				readers = new IndexReader[NUM_COPY];
				for (int i = 0; i < NUM_COPY; i++)
					readers[i] = IndexReader.Open(dir);
			}
			
			internal virtual void  LaunchThreads(int numIter)
			{
                threads = new SupportClass.ThreadClass[NUM_THREADS]; //{{DIGY}} Should this be created somewhere else?
				for (int i = 0; i < NUM_THREADS; i++)
				{
					threads[i] = new AnonymousClassThread2(numIter, this);
				}
				
				for (int i = 0; i < NUM_THREADS; i++)
					threads[i].Start();
			}
			
			internal virtual void  JoinThreads()
			{
				for (int i = 0; i < NUM_THREADS; i++)
					threads[i].Join();
			}
			
			internal virtual void  Close(bool doWait)
			{
				didClose = true;
				writer2.Close(doWait);
			}
			
			internal virtual void  CloseDir()
			{
				for (int i = 0; i < NUM_COPY; i++)
					readers[i].Close();
				dir2.Close();
			}
			
			public /*internal*/ abstract void  DoBody(int j, Directory[] dirs);
			internal abstract void  Handle(System.Exception t);
		}
		
		private class CommitAndAddIndexes:RunAddIndexesThreads
		{
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public CommitAndAddIndexes(TestIndexWriter enclosingInstance, int numCopy):base(numCopy)
			{
				InitBlock(enclosingInstance);
			}
			
			internal override void  Handle(System.Exception t)
			{
				System.Console.Out.WriteLine(t.StackTrace);
				lock (failures.SyncRoot)
				{
					failures.Add(t);
				}
			}
			
			public /*internal*/ override /*virtual*/ void  DoBody(int j, Directory[] dirs)
			{
				switch (j % 4)
				{
					
					case 0: 
						writer2.AddIndexes(dirs);
						break;
					
					case 1: 
						writer2.AddIndexesNoOptimize(dirs);
						break;
					
					case 2: 
						writer2.AddIndexes(readers);
						break;
					
					case 3: 
						writer2.Commit();
						break;
					}
			}
		}
		
		// LUCENE-1335: test simultaneous addIndexes & commits
		// from multiple threads
        [Test]
		public virtual void  TestAddIndexesWithThreads()
		{
			
			int NUM_ITER = 12;
			int NUM_COPY = 3;
			CommitAndAddIndexes c = new CommitAndAddIndexes(this, NUM_COPY);
			c.LaunchThreads(NUM_ITER);
			
			for (int i = 0; i < 100; i++)
				AddDoc(c.writer2);
			
			c.JoinThreads();
			
			Assert.AreEqual(100 + NUM_COPY * (3 * NUM_ITER / 4) * Lucene.Net.Index.TestIndexWriter.CommitAndAddIndexes.NUM_THREADS * Lucene.Net.Index.TestIndexWriter.CommitAndAddIndexes.NUM_INIT_DOCS, c.writer2.NumDocs());
			
			c.Close(true);
			
			Assert.IsTrue(c.failures.Count == 0);
			
			_TestUtil.CheckIndex(c.dir2);
			
			IndexReader reader = IndexReader.Open(c.dir2);
			Assert.AreEqual(100 + NUM_COPY * (3 * NUM_ITER / 4) * Lucene.Net.Index.TestIndexWriter.CommitAndAddIndexes.NUM_THREADS * Lucene.Net.Index.TestIndexWriter.CommitAndAddIndexes.NUM_INIT_DOCS, reader.NumDocs());
			reader.Close();
			
			c.CloseDir();
		}
		
		private class CommitAndAddIndexes2:CommitAndAddIndexes
		{
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public new TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public CommitAndAddIndexes2(TestIndexWriter enclosingInstance, int numCopy):base(enclosingInstance, numCopy)
			{
				InitBlock(enclosingInstance);
			}
			
			internal override void  Handle(System.Exception t)
			{
				if (!(t is AlreadyClosedException) && !(t is System.NullReferenceException))
				{
					System.Console.Out.WriteLine(t.StackTrace);
					lock (failures.SyncRoot)
					{
						failures.Add(t);
					}
				}
			}
		}
		
		// LUCENE-1335: test simultaneous addIndexes & close
        [Test]
		public virtual void  TestAddIndexesWithClose()
		{
			int NUM_COPY = 3;
			CommitAndAddIndexes2 c = new CommitAndAddIndexes2(this, NUM_COPY);
			//c.writer2.setInfoStream(System.out);
			c.LaunchThreads(- 1);
			
			// Close w/o first stopping/joining the threads
			c.Close(true);
			//c.writer2.close();
			
			c.JoinThreads();
			
			_TestUtil.CheckIndex(c.dir2);
			
			c.CloseDir();
			
			Assert.IsTrue(c.failures.Count == 0);
		}
		
		private class CommitAndAddIndexes3:RunAddIndexesThreads
		{
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public CommitAndAddIndexes3(TestIndexWriter enclosingInstance, int numCopy):base(numCopy)
			{
				InitBlock(enclosingInstance);
			}
			
			public /*internal*/ override /*virtual*/ void  DoBody(int j, Directory[] dirs)
			{
				switch (j % 5)
				{
					
					case 0: 
						writer2.AddIndexes(dirs);
						break;
					
					case 1: 
						writer2.AddIndexesNoOptimize(dirs);
						break;
					
					case 2: 
						writer2.AddIndexes(readers);
						break;
					
					case 3: 
						writer2.Optimize();
						goto case 4;
					
					case 4: 
						writer2.Commit();
						break;
					}
			}
			
			internal override void  Handle(System.Exception t)
			{
				bool report = true;
				
				if (t is AlreadyClosedException || t is MergePolicy.MergeAbortedException || t is System.NullReferenceException)
				{
					report = !didClose;
				}
				else if (t is System.IO.IOException)
				{
					System.Exception t2 = t.InnerException;
					if (t2 is MergePolicy.MergeAbortedException)
					{
						report = !didClose;
					}
				}
				if (report)
				{
					System.Console.Out.WriteLine(t.StackTrace);
					lock (failures.SyncRoot)
					{
						failures.Add(t);
					}
				}
			}
		}
		
		// LUCENE-1335: test simultaneous addIndexes & close
        [Test]
		public virtual void  TestAddIndexesWithCloseNoWait()
		{
			
			int NUM_COPY = 50;
			CommitAndAddIndexes3 c = new CommitAndAddIndexes3(this, NUM_COPY);
			c.LaunchThreads(- 1);
			
			System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 500));
			
			// Close w/o first stopping/joining the threads
			c.Close(false);
			
			c.JoinThreads();
			
			_TestUtil.CheckIndex(c.dir2);
			
			c.CloseDir();
			
			Assert.IsTrue(c.failures.Count == 0);
		}
		
		// LUCENE-1335: test simultaneous addIndexes & close
        [Test]
		public virtual void  TestAddIndexesWithRollback()
		{
			
			int NUM_COPY = 50;
			CommitAndAddIndexes3 c = new CommitAndAddIndexes3(this, NUM_COPY);
			c.LaunchThreads(- 1);
			
			System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 500));
			
			// Close w/o first stopping/joining the threads
			c.didClose = true;
			c.writer2.Rollback();
			
			c.JoinThreads();
			
			_TestUtil.CheckIndex(c.dir2);
			
			c.CloseDir();
			
			Assert.IsTrue(c.failures.Count == 0);
		}
		
		// LUCENE-1347
		public class MockIndexWriter4:IndexWriter
		{
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public MockIndexWriter4(TestIndexWriter enclosingInstance, Directory dir, Analyzer a, bool create, MaxFieldLength mfl):base(dir, a, create, mfl)
			{
				InitBlock(enclosingInstance);
			}
			
			internal bool doFail;
			
			public override bool TestPoint(System.String name)
			{
				if (doFail && name.Equals("rollback before checkpoint"))
					throw new System.SystemException("intentionally failing");
				return true;
			}
		}
		
		// LUCENE-1347
        [Test]
		public virtual void  TestRollbackExceptionHang()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			MockIndexWriter4 w = new MockIndexWriter4(this, dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			AddDoc(w);
			w.doFail = true;
			try
			{
				w.Rollback();
                if (SupportClass.BuildType.Debug)
                    Assert.Fail("did not hit intentional RuntimeException");
                else
                    Assert.Ignore("This test is not executed in release mode");
				
			}
			catch (System.SystemException re)
			{
				// expected
			}
			
			w.doFail = false;
			w.Rollback();
		}
		
		
		// LUCENE-1219
        [Test]
		public virtual void  TestBinaryFieldOffsetLength()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
			byte[] b = new byte[50];
			for (int i = 0; i < 50; i++)
				b[i] = (byte) (i + 77);
			
			Document doc = new Document();
			Field f = new Field("binary", b, 10, 17, Field.Store.YES);
			byte[] bx = f.GetBinaryValue();
			Assert.IsTrue(bx != null);
			Assert.AreEqual(50, bx.Length);
			Assert.AreEqual(10, f.GetBinaryOffset());
			Assert.AreEqual(17, f.GetBinaryLength());
			doc.Add(f);
			w.AddDocument(doc);
			w.Close();
			
			IndexReader ir = IndexReader.Open(dir);
			doc = ir.Document(0);
			f = doc.GetField("binary");
			b = f.GetBinaryValue();
			Assert.IsTrue(b != null);
			Assert.AreEqual(17, b.Length, 17);
			Assert.AreEqual(87, b[0]);
			ir.Close();
			dir.Close();
		}
		
		// LUCENE-1374
        [Test]
		public virtual void  TestMergeCompressedFields()
		{
			System.IO.FileInfo indexDir = new System.IO.FileInfo(System.IO.Path.Combine(SupportClass.AppSettings.Get("tempDir", ""), "mergecompressedfields"));
			Directory dir = FSDirectory.Open(indexDir);
			try
			{
				for (int i = 0; i < 5; i++)
				{
					// Must make a new writer & doc each time, w/
					// different fields, so bulk merge of stored fields
					// cannot run:
					IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), i == 0, IndexWriter.MaxFieldLength.UNLIMITED);
					try
					{
						w.SetMergeFactor(5);
						w.SetMergeScheduler(new SerialMergeScheduler());
						Document doc = new Document();
						doc.Add(new Field("test1", "this is some data that will be compressed this this this", Field.Store.COMPRESS, Field.Index.NO));
						doc.Add(new Field("test2", new byte[20], Field.Store.COMPRESS));
						doc.Add(new Field("field" + i, "random field", Field.Store.NO, Field.Index.ANALYZED));
						w.AddDocument(doc);
					}
					finally
					{
						w.Close();
					}
				}
				
				byte[] cmp = new byte[20];
				
				IndexReader r = IndexReader.Open(dir);
				try
				{
					for (int i = 0; i < 5; i++)
					{
						Document doc = r.Document(i);
						Assert.AreEqual(doc.GetField("test1").StringValue(), "this is some data that will be compressed this this this");
						byte[] b = doc.GetField("test2").BinaryValue();
                        Assert.AreEqual(b.Length, cmp.Length);
                        for (int j = 0; j < b.Length; j++)
                            Assert.AreEqual(b[j], cmp[j]);
					}
				}
				finally
				{
					r.Close();
				}
			}
			finally
			{
				dir.Close();
				_TestUtil.RmDir(indexDir);
			}
		}
		
		// LUCENE-1382
        [Test]
		public virtual void  TestCommitUserData()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			w.SetMaxBufferedDocs(2);
			for (int j = 0; j < 17; j++)
				AddDoc(w);
			w.Close();
			
			Assert.AreEqual(0, IndexReader.GetCommitUserData(dir).Count);
			
			IndexReader r = IndexReader.Open(dir);
			// commit(Map) never called for this index
			Assert.AreEqual(0, r.GetCommitUserData().Count);
			r.Close();
			
			w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			w.SetMaxBufferedDocs(2);
			for (int j = 0; j < 17; j++)
				AddDoc(w);
            System.Collections.Generic.IDictionary<string, string> data = new System.Collections.Generic.Dictionary<string,string>();
			data["label"] = "test1";
			w.Commit(data);
			w.Close();
			
			Assert.AreEqual("test1", IndexReader.GetCommitUserData(dir)["label"]);
			
			r = IndexReader.Open(dir);
			Assert.AreEqual("test1", r.GetCommitUserData()["label"]);
			r.Close();
			
			w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			w.Optimize();
			w.Close();
			
			Assert.AreEqual("test1", IndexReader.GetCommitUserData(dir)["label"]);
			
			dir.Close();
		}
		
        [Test]
		public virtual void  TestOptimizeExceptions()
		{
			RAMDirectory startDir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(startDir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			w.SetMaxBufferedDocs(2);
			w.SetMergeFactor(100);
			for (int i = 0; i < 27; i++)
				AddDoc(w);
			w.Close();
			
			for (int i = 0; i < 200; i++)
			{
				MockRAMDirectory dir = new MockRAMDirectory(startDir);
				w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
				((ConcurrentMergeScheduler) w.GetMergeScheduler()).SetSuppressExceptions();
				dir.SetRandomIOExceptionRate(0.5, 100);
				try
				{
					w.Optimize();
				}
				catch (System.IO.IOException ioe)
				{
					if (ioe.InnerException == null)
						Assert.Fail("optimize threw IOException without root cause");
				}
				w.Close();
				dir.Close();
			}
		}
		
		// LUCENE-1429
        [Test]
		public virtual void  TestOutOfMemoryErrorCausesCloseToFail()
		{
			
			System.Collections.IList thrown = new System.Collections.ArrayList();
			
			IndexWriter writer = new AnonymousClassIndexWriter(thrown, this, new MockRAMDirectory(), new StandardAnalyzer());
			
			// need to set an info stream so message is called
			writer.SetInfoStream(new System.IO.StreamWriter(new System.IO.MemoryStream()));
			try
			{
				writer.Close();
				Assert.Fail("OutOfMemoryError expected");
			}
			catch (System.OutOfMemoryException expected)
			{
			}
			
			// throws IllegalStateEx w/o bug fix
			writer.Close();
		}
		
		// LUCENE-1442
        [Test]
		public virtual void  TestDoubleOffsetCounting()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			Field f = new Field("field", "abcd", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			doc.Add(f);
			doc.Add(f);
			Field f2 = new Field("field", "", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			doc.Add(f2);
			doc.Add(f);
			w.AddDocument(doc);
			w.Close();
			
			IndexReader r = IndexReader.Open(dir);
			TermVectorOffsetInfo[] termOffsets = ((TermPositionVector) r.GetTermFreqVector(0, "field")).GetOffsets(0);
			
			// Token "" occurred once
			Assert.AreEqual(1, termOffsets.Length);
			Assert.AreEqual(8, termOffsets[0].GetStartOffset());
			Assert.AreEqual(8, termOffsets[0].GetEndOffset());
			
			// Token "abcd" occurred three times
			termOffsets = ((TermPositionVector) r.GetTermFreqVector(0, "field")).GetOffsets(1);
			Assert.AreEqual(3, termOffsets.Length);
			Assert.AreEqual(0, termOffsets[0].GetStartOffset());
			Assert.AreEqual(4, termOffsets[0].GetEndOffset());
			Assert.AreEqual(4, termOffsets[1].GetStartOffset());
			Assert.AreEqual(8, termOffsets[1].GetEndOffset());
			Assert.AreEqual(8, termOffsets[2].GetStartOffset());
			Assert.AreEqual(12, termOffsets[2].GetEndOffset());
			r.Close();
			dir.Close();
		}
		
		// LUCENE-1442
        [Test]
		public virtual void  TestDoubleOffsetCounting2()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new SimpleAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			Field f = new Field("field", "abcd", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			doc.Add(f);
			doc.Add(f);
			w.AddDocument(doc);
			w.Close();
			
			IndexReader r = IndexReader.Open(dir);
			TermVectorOffsetInfo[] termOffsets = ((TermPositionVector) r.GetTermFreqVector(0, "field")).GetOffsets(0);
			Assert.AreEqual(2, termOffsets.Length);
			Assert.AreEqual(0, termOffsets[0].GetStartOffset());
			Assert.AreEqual(4, termOffsets[0].GetEndOffset());
			Assert.AreEqual(5, termOffsets[1].GetStartOffset());
			Assert.AreEqual(9, termOffsets[1].GetEndOffset());
			r.Close();
			dir.Close();
		}
		
		// LUCENE-1448
        [Test]
		public virtual void  TestEndOffsetPositionCharAnalyzer()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			Field f = new Field("field", "abcd   ", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			doc.Add(f);
			doc.Add(f);
			w.AddDocument(doc);
			w.Close();
			
			IndexReader r = IndexReader.Open(dir);
			TermVectorOffsetInfo[] termOffsets = ((TermPositionVector) r.GetTermFreqVector(0, "field")).GetOffsets(0);
			Assert.AreEqual(2, termOffsets.Length);
			Assert.AreEqual(0, termOffsets[0].GetStartOffset());
			Assert.AreEqual(4, termOffsets[0].GetEndOffset());
			Assert.AreEqual(8, termOffsets[1].GetStartOffset());
			Assert.AreEqual(12, termOffsets[1].GetEndOffset());
			r.Close();
			dir.Close();
		}
		
		// LUCENE-1448
        [Test]
		public virtual void  TestEndOffsetPositionWithCachingTokenFilter()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			Analyzer analyzer = new WhitespaceAnalyzer();
			IndexWriter w = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			TokenStream stream = new CachingTokenFilter(analyzer.TokenStream("field", new System.IO.StringReader("abcd   ")));
			Field f = new Field("field", stream, Field.TermVector.WITH_POSITIONS_OFFSETS);
			doc.Add(f);
			doc.Add(f);
			w.AddDocument(doc);
			w.Close();
			
			IndexReader r = IndexReader.Open(dir);
			TermVectorOffsetInfo[] termOffsets = ((TermPositionVector) r.GetTermFreqVector(0, "field")).GetOffsets(0);
			Assert.AreEqual(2, termOffsets.Length);
			Assert.AreEqual(0, termOffsets[0].GetStartOffset());
			Assert.AreEqual(4, termOffsets[0].GetEndOffset());
			Assert.AreEqual(8, termOffsets[1].GetStartOffset());
			Assert.AreEqual(12, termOffsets[1].GetEndOffset());
			r.Close();
			dir.Close();
		}
		
		// LUCENE-1448
        [Test]
		public virtual void  TestEndOffsetPositionWithTeeSinkTokenFilter()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			Analyzer analyzer = new WhitespaceAnalyzer();
			IndexWriter w = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			TeeSinkTokenFilter tee = new TeeSinkTokenFilter(analyzer.TokenStream("field", new System.IO.StringReader("abcd   ")));
			TokenStream sink = tee.NewSinkTokenStream();
			Field f1 = new Field("field", tee, Field.TermVector.WITH_POSITIONS_OFFSETS);
			Field f2 = new Field("field", sink, Field.TermVector.WITH_POSITIONS_OFFSETS);
			doc.Add(f1);
			doc.Add(f2);
			w.AddDocument(doc);
			w.Close();
			
			IndexReader r = IndexReader.Open(dir);
			TermVectorOffsetInfo[] termOffsets = ((TermPositionVector) r.GetTermFreqVector(0, "field")).GetOffsets(0);
			Assert.AreEqual(2, termOffsets.Length);
			Assert.AreEqual(0, termOffsets[0].GetStartOffset());
			Assert.AreEqual(4, termOffsets[0].GetEndOffset());
			Assert.AreEqual(8, termOffsets[1].GetStartOffset());
			Assert.AreEqual(12, termOffsets[1].GetEndOffset());
			r.Close();
			dir.Close();
		}
		
		// LUCENE-1448
        [Test]
		public virtual void  TestEndOffsetPositionStopFilter()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new StopAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			Field f = new Field("field", "abcd the", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			doc.Add(f);
			doc.Add(f);
			w.AddDocument(doc);
			w.Close();
			
			IndexReader r = IndexReader.Open(dir);
			TermVectorOffsetInfo[] termOffsets = ((TermPositionVector) r.GetTermFreqVector(0, "field")).GetOffsets(0);
			Assert.AreEqual(2, termOffsets.Length);
			Assert.AreEqual(0, termOffsets[0].GetStartOffset());
			Assert.AreEqual(4, termOffsets[0].GetEndOffset());
			Assert.AreEqual(9, termOffsets[1].GetStartOffset());
			Assert.AreEqual(13, termOffsets[1].GetEndOffset());
			r.Close();
			dir.Close();
		}
		
		// LUCENE-1448
        [Test]
		public virtual void  TestEndOffsetPositionStandard()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			Field f = new Field("field", "abcd the  ", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			Field f2 = new Field("field", "crunch man", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			doc.Add(f);
			doc.Add(f2);
			w.AddDocument(doc);
			w.Close();
			
			IndexReader r = IndexReader.Open(dir);
			TermPositionVector tpv = ((TermPositionVector) r.GetTermFreqVector(0, "field"));
			TermVectorOffsetInfo[] termOffsets = tpv.GetOffsets(0);
			Assert.AreEqual(1, termOffsets.Length);
			Assert.AreEqual(0, termOffsets[0].GetStartOffset());
			Assert.AreEqual(4, termOffsets[0].GetEndOffset());
			termOffsets = tpv.GetOffsets(1);
			Assert.AreEqual(11, termOffsets[0].GetStartOffset());
			Assert.AreEqual(17, termOffsets[0].GetEndOffset());
			termOffsets = tpv.GetOffsets(2);
			Assert.AreEqual(18, termOffsets[0].GetStartOffset());
			Assert.AreEqual(21, termOffsets[0].GetEndOffset());
			r.Close();
			dir.Close();
		}
		
		// LUCENE-1448
        [Test]
		public virtual void  TestEndOffsetPositionStandardEmptyField()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			Field f = new Field("field", "", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			Field f2 = new Field("field", "crunch man", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			doc.Add(f);
			doc.Add(f2);
			w.AddDocument(doc);
			w.Close();
			
			IndexReader r = IndexReader.Open(dir);
			TermPositionVector tpv = ((TermPositionVector) r.GetTermFreqVector(0, "field"));
			TermVectorOffsetInfo[] termOffsets = tpv.GetOffsets(0);
			Assert.AreEqual(1, termOffsets.Length);
			Assert.AreEqual(0, termOffsets[0].GetStartOffset());
			Assert.AreEqual(6, termOffsets[0].GetEndOffset());
			termOffsets = tpv.GetOffsets(1);
			Assert.AreEqual(7, termOffsets[0].GetStartOffset());
			Assert.AreEqual(10, termOffsets[0].GetEndOffset());
			r.Close();
			dir.Close();
		}
		
		// LUCENE-1448
        [Test]
		public virtual void  TestEndOffsetPositionStandardEmptyField2()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			
			Field f = new Field("field", "abcd", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			doc.Add(f);
			doc.Add(new Field("field", "", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			
			Field f2 = new Field("field", "crunch", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
			doc.Add(f2);
			
			w.AddDocument(doc);
			w.Close();
			
			IndexReader r = IndexReader.Open(dir);
			TermPositionVector tpv = ((TermPositionVector) r.GetTermFreqVector(0, "field"));
			TermVectorOffsetInfo[] termOffsets = tpv.GetOffsets(0);
			Assert.AreEqual(1, termOffsets.Length);
			Assert.AreEqual(0, termOffsets[0].GetStartOffset());
			Assert.AreEqual(4, termOffsets[0].GetEndOffset());
			termOffsets = tpv.GetOffsets(1);
			Assert.AreEqual(5, termOffsets[0].GetStartOffset());
			Assert.AreEqual(11, termOffsets[0].GetEndOffset());
			r.Close();
			dir.Close();
		}
		
		
		// LUCENE-1468 -- make sure opening an IndexWriter with
		// create=true does not remove non-index files
		
        [Test]
		public virtual void  TestOtherFiles()
		{
			System.IO.FileInfo indexDir = new System.IO.FileInfo(System.IO.Path.Combine(SupportClass.AppSettings.Get("tempDir", ""), "otherfiles"));
			Directory dir = FSDirectory.Open(indexDir);
			try
			{
				// Create my own random file:
				
				IndexOutput out_Renamed = dir.CreateOutput("myrandomfile");
				out_Renamed.WriteByte((byte) 42);
				out_Renamed.Close();
				
				new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED).Close();
				
				Assert.IsTrue(dir.FileExists("myrandomfile"));
				
				// Make sure this does not copy myrandomfile:
				Directory dir2 = new RAMDirectory(dir);
				Assert.IsTrue(!dir2.FileExists("myrandomfile"));
			}
			finally
			{
				dir.Close();
				_TestUtil.RmDir(indexDir);
			}
		}
		
        [Test]
		public virtual void  TestDeadlock()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, true, new WhitespaceAnalyzer());
			writer.SetMaxBufferedDocs(2);
			Document doc = new Document();
			doc.Add(new Field("content", "aaa bbb ccc ddd eee fff ggg hhh iii", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			writer.AddDocument(doc);
			writer.AddDocument(doc);
			writer.AddDocument(doc);
			writer.Commit();
			// index has 2 segments
			
			MockRAMDirectory dir2 = new MockRAMDirectory();
			IndexWriter writer2 = new IndexWriter(dir2, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer2.AddDocument(doc);
			writer2.Close();
			
			IndexReader r1 = IndexReader.Open(dir2);
			IndexReader r2 = (IndexReader) r1.Clone();
			writer.AddIndexes(new IndexReader[]{r1, r2});
			writer.Close();
			
			IndexReader r3 = IndexReader.Open(dir);
			Assert.AreEqual(5, r3.NumDocs());
			r3.Close();
			
			r1.Close();
			r2.Close();
			
			dir2.Close();
			dir.Close();
		}
		
		private class IndexerThreadInterrupt:SupportClass.ThreadClass
		{
			public IndexerThreadInterrupt(TestIndexWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestIndexWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexWriter enclosingInstance;
			public TestIndexWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal volatile bool failed;
			internal volatile bool finish;
			override public void  Run()
			{
                bool endLoop = false;
				RAMDirectory dir = new RAMDirectory();
				IndexWriter w = null;
				while (!finish)
				{
					try
					{
						//IndexWriter.unlock(dir);
						w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
						((ConcurrentMergeScheduler) w.GetMergeScheduler()).SetSuppressExceptions();
						//w.setInfoStream(System.out);
						w.SetMaxBufferedDocs(2);
						w.SetMergeFactor(2);
						Document doc = new Document();
						doc.Add(new Field("field", "some text contents", Field.Store.YES, Field.Index.ANALYZED));
						for (int i = 0; i < 100; i++)
						{
							w.AddDocument(doc);
							w.Commit();
						}
					}
					catch (System.SystemException re)
					{
						System.Exception e = re.InnerException;
                        if (re is System.Threading.ThreadInterruptedException || e is System.Threading.ThreadInterruptedException)
						{
                            // {{Aroush-2.9}} in Java, this is: java.lang.Thread.interrupted()
                            //{There is no way in .Net to check interrupted state. So comment it out

                            //// Make sure IW restored interrupted bit
                            //if ((Instance.ThreadState & (System.Threading.ThreadState.Stopped | System.Threading.ThreadState.Unstarted)) != System.Threading.ThreadState.Running)  // {{Aroush-2.9}} in Java, this is: java.lang.Thread.interrupted()
                            //{
                            //    System.Console.Out.WriteLine("FAILED; InterruptedException hit but thread.interrupted() was false");
                            //    System.Console.Out.WriteLine(e.StackTrace);
                            //    failed = true;
                            //    break;
                            //}
						}
						else
						{
							System.Console.Out.WriteLine("FAILED; unexpected exception");
                            if (e != null)
                            {
                                System.Console.Out.WriteLine(e.StackTrace);
                            }
                            else
                            {
                                System.Console.Out.WriteLine(re.StackTrace);
                            }
							failed = true;
							break;
						}
					}
					catch (System.Exception t)
					{
						System.Console.Out.WriteLine("FAILED; unexpected exception");
						System.Console.Out.WriteLine(t.StackTrace);
						failed = true;
						break;
					}
					finally
					{
						try
						{
							// Clear interrupt if pending
							lock (this)
							{
								Interrupt();
								if (w != null)
								{
									w.Close();
								}
							}
						}
						catch (System.Exception t)
						{
							System.Console.Out.WriteLine("FAILED; unexpected exception during close");
							System.Console.Out.WriteLine(t.StackTrace);
							failed = true;
                            endLoop = true;
						}
					}

                    if (endLoop) break;
				}
				
				if (!failed)
				{
					try
					{
						_TestUtil.CheckIndex(dir);
					}
					catch (System.Exception e)
					{
						failed = true;
						System.Console.Out.WriteLine("CheckIndex FAILED: unexpected exception");
						System.Console.Out.WriteLine(e.StackTrace);
					}
					try
					{
						IndexReader r = IndexReader.Open(dir);
						//System.out.println("doc count=" + r.numDocs());
						r.Close();
					}
					catch (System.Exception e)
					{
						failed = true;
						System.Console.Out.WriteLine("IndexReader.open FAILED: unexpected exception");
						System.Console.Out.WriteLine(e.StackTrace);
					}
				}
			}
		}
		
        [Test]
		public virtual void  TestThreadInterruptDeadlock()
		{
			IndexerThreadInterrupt t = new IndexerThreadInterrupt(this);
			t.IsBackground = true;
			t.Start();
			for (int i = 0; i < 100; i++)
			{
				System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 1));
				lock (t)
				{
					t.Interrupt();
				}
			}
			t.finish = true;
			lock (t)
			{
				t.Interrupt();
			}
			t.Join();
			Assert.IsFalse(t.failed);
		}
		
		
        [Test]
		public virtual void  TestIndexStoreCombos()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
			byte[] b = new byte[50];
			for (int i = 0; i < 50; i++)
				b[i] = (byte) (i + 77);
			
			Document doc = new Document();
			Field f = new Field("binary", b, 10, 17, Field.Store.YES);
			f.SetTokenStream(new WhitespaceTokenizer(new System.IO.StringReader("doc1field1")));
			Field f2 = new Field("string", "value", Field.Store.YES, Field.Index.ANALYZED);
			f2.SetTokenStream(new WhitespaceTokenizer(new System.IO.StringReader("doc1field2")));
			doc.Add(f);
			doc.Add(f2);
			w.AddDocument(doc);
			
			// add 2 docs to test in-memory merging
			f.SetTokenStream(new WhitespaceTokenizer(new System.IO.StringReader("doc2field1")));
			f2.SetTokenStream(new WhitespaceTokenizer(new System.IO.StringReader("doc2field2")));
			w.AddDocument(doc);
			
			// force segment flush so we can force a segment merge with doc3 later.
			w.Commit();
			
			f.SetTokenStream(new WhitespaceTokenizer(new System.IO.StringReader("doc3field1")));
			f2.SetTokenStream(new WhitespaceTokenizer(new System.IO.StringReader("doc3field2")));
			
			w.AddDocument(doc);
			w.Commit();
			w.Optimize(); // force segment merge.
			
			IndexReader ir = IndexReader.Open(dir);
			doc = ir.Document(0);
			f = doc.GetField("binary");
			b = f.GetBinaryValue();
			Assert.IsTrue(b != null);
			Assert.AreEqual(17, b.Length, 17);
			Assert.AreEqual(87, b[0]);
			
			Assert.IsTrue(ir.Document(0).GetFieldable("binary").IsBinary());
			Assert.IsTrue(ir.Document(1).GetFieldable("binary").IsBinary());
			Assert.IsTrue(ir.Document(2).GetFieldable("binary").IsBinary());
			
			Assert.AreEqual("value", ir.Document(0).Get("string"));
			Assert.AreEqual("value", ir.Document(1).Get("string"));
			Assert.AreEqual("value", ir.Document(2).Get("string"));
			
			
			// test that the terms were indexed.
			Assert.IsTrue(ir.TermDocs(new Term("binary", "doc1field1")).Next());
			Assert.IsTrue(ir.TermDocs(new Term("binary", "doc2field1")).Next());
			Assert.IsTrue(ir.TermDocs(new Term("binary", "doc3field1")).Next());
			Assert.IsTrue(ir.TermDocs(new Term("string", "doc1field2")).Next());
			Assert.IsTrue(ir.TermDocs(new Term("string", "doc2field2")).Next());
			Assert.IsTrue(ir.TermDocs(new Term("string", "doc3field2")).Next());
			
			ir.Close();
			dir.Close();
		}
		
		// LUCENE-1727: make sure doc fields are stored in order
        [Test]
		public virtual void  TestStoredFieldsOrder()
		{
			Directory d = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(d, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			doc.Add(new Field("zzz", "a b c", Field.Store.YES, Field.Index.NO));
			doc.Add(new Field("aaa", "a b c", Field.Store.YES, Field.Index.NO));
			doc.Add(new Field("zzz", "1 2 3", Field.Store.YES, Field.Index.NO));
			w.AddDocument(doc);
			IndexReader r = w.GetReader();
			doc = r.Document(0);
			System.Collections.IEnumerator it = doc.GetFields().GetEnumerator();
			Assert.IsTrue(it.MoveNext());
			Field f = (Field) it.Current;
			Assert.AreEqual(f.Name(), "zzz");
			Assert.AreEqual(f.StringValue(), "a b c");
			
			Assert.IsTrue(it.MoveNext());
			f = (Field) it.Current;
			Assert.AreEqual(f.Name(), "aaa");
			Assert.AreEqual(f.StringValue(), "a b c");
			
			Assert.IsTrue(it.MoveNext());
			f = (Field) it.Current;
			Assert.AreEqual(f.Name(), "zzz");
			Assert.AreEqual(f.StringValue(), "1 2 3");
			Assert.IsFalse(it.MoveNext());
			r.Close();
			w.Close();
			d.Close();
		}

        [Test]
        public void TestEmbeddedFFFF()
        {

            Directory d = new MockRAMDirectory();
            IndexWriter w = new IndexWriter(d, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
            Document doc = new Document();
            doc.Add(new Field("field", "a a\uffffb", Field.Store.NO, Field.Index.ANALYZED));
            w.AddDocument(doc);
            doc = new Document();
            doc.Add(new Field("field", "a", Field.Store.NO, Field.Index.ANALYZED));
            w.AddDocument(doc);
            w.Close();

            _TestUtil.CheckIndex(d);
            d.Close();
        }

        [Test]
        public void TestNoDocsIndex()
        {
            Directory dir = new MockRAMDirectory();
            IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
            writer.SetUseCompoundFile(false);
            //ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
            //writer.SetInfoStream(new PrintStream(bos));
            writer.AddDocument(new Document());
            writer.Close();

            _TestUtil.CheckIndex(dir);
            dir.Close();
        }

        class LUCENE_2095_Thread : SupportClass.ThreadClass
        {
            IndexWriter w = null;
            Directory dir = null;
            long endTime = 0;
            System.Collections.IList failed = null;
            int finalI = 0;

            public LUCENE_2095_Thread(IndexWriter w, long endTime, Directory dir, System.Collections.IList failed, int finalI)
            {
                this.w = w;
                this.dir = dir;
                this.endTime = endTime;
                this.failed = failed;
                this.finalI = finalI;
            }

            override public void Run()
            {
                try
                {
                    Document doc = new Document();
                    IndexReader r = IndexReader.Open(dir);
                    Field f = new Field("f", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
                    doc.Add(f);
                    int count = 0;
                    while ((DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) < endTime && failed.Count == 0)
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            String s = finalI + "_" + (count++).ToString();
                            f.SetValue(s);
                            w.AddDocument(doc);
                            w.Commit();
                            IndexReader r2 = r.Reopen();
                            Assert.IsTrue(r2 != r);
                            r.Close();
                            r = r2;
                            Assert.AreEqual(1, r.DocFreq(new Term("f", s)), "term=f:" + s);
                        }
                    }
                    r.Close();
                }
                catch (Exception t)
                {
                    lock (failed)
                    {
                        failed.Add(this);
                    }
                    throw t;
                }
            }
        }

        // LUCENE-2095: make sure with multiple threads commit
        // doesn't return until all changes are in fact in the
        // index
        [Test]
        public void TestCommitThreadSafety()
        {
            int NUM_THREADS = 5;
            double RUN_SEC = 0.5;
            Directory dir = new MockRAMDirectory();
            IndexWriter w = new IndexWriter(dir, new SimpleAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
            w.Commit();
            System.Collections.IList failed = new System.Collections.ArrayList();
            LUCENE_2095_Thread[] threads = new LUCENE_2095_Thread[NUM_THREADS];
            long endTime = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) + ((long)(RUN_SEC * 1000));
            for (int i = 0; i < NUM_THREADS; i++)
            {
                int finalI = i;

                threads[i] = new LUCENE_2095_Thread(w, endTime, dir, failed, finalI);
                threads[i].Start();
            }
            for (int i = 0; i < NUM_THREADS; i++)
            {
                threads[i].Join();
            }
            w.Close();
            dir.Close();
            Assert.AreEqual(0, failed.Count);
        }
	}
}