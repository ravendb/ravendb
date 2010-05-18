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
using SimpleAnalyzer = Lucene.Net.Analysis.SimpleAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Directory = Lucene.Net.Store.Directory;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using English = Lucene.Net.Util.English;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestThreadedOptimize:LuceneTestCase
	{
		private class AnonymousClassThread:SupportClass.ThreadClass
		{
			public AnonymousClassThread(Lucene.Net.Index.IndexWriter writerFinal, int iFinal, int iterFinal, TestThreadedOptimize enclosingInstance)
			{
				InitBlock(writerFinal, iFinal, iterFinal, enclosingInstance);
			}
			private void  InitBlock(Lucene.Net.Index.IndexWriter writerFinal, int iFinal, int iterFinal, TestThreadedOptimize enclosingInstance)
			{
				this.writerFinal = writerFinal;
				this.iFinal = iFinal;
				this.iterFinal = iterFinal;
				this.enclosingInstance = enclosingInstance;
			}
			private Lucene.Net.Index.IndexWriter writerFinal;
			private int iFinal;
			private int iterFinal;
			private TestThreadedOptimize enclosingInstance;
			public TestThreadedOptimize Enclosing_Instance
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
					for (int j = 0; j < Lucene.Net.Index.TestThreadedOptimize.NUM_ITER2; j++)
					{
						writerFinal.Optimize(false);
						for (int k = 0; k < 17 * (1 + iFinal); k++)
						{
							Document d = new Document();
							d.Add(new Field("id", iterFinal + "_" + iFinal + "_" + j + "_" + k, Field.Store.YES, Field.Index.NOT_ANALYZED));
							d.Add(new Field("contents", English.IntToEnglish(iFinal + k), Field.Store.NO, Field.Index.ANALYZED));
							writerFinal.AddDocument(d);
						}
						for (int k = 0; k < 9 * (1 + iFinal); k++)
							writerFinal.DeleteDocuments(new Term("id", iterFinal + "_" + iFinal + "_" + j + "_" + k));
						writerFinal.Optimize();
					}
				}
				catch (System.Exception t)
				{
					Enclosing_Instance.setFailed();
					System.Console.Out.WriteLine(SupportClass.ThreadClass.Current().Name + ": hit exception");
					System.Console.Out.WriteLine(t.StackTrace);
				}
			}
		}
		
		private static readonly Analyzer ANALYZER = new SimpleAnalyzer();
		
		private const int NUM_THREADS = 3;
		//private final static int NUM_THREADS = 5;
		
		private const int NUM_ITER = 1;
		//private final static int NUM_ITER = 10;
		
		private const int NUM_ITER2 = 1;
		//private final static int NUM_ITER2 = 5;
		
		private bool failed;
		
		private void  setFailed()
		{
			failed = true;
		}
		
		public virtual void  runTest(Directory directory, bool autoCommit, MergeScheduler merger)
		{
			
			IndexWriter writer = new IndexWriter(directory, autoCommit, ANALYZER, true);
			writer.SetMaxBufferedDocs(2);
			if (merger != null)
				writer.SetMergeScheduler(merger);
			
			for (int iter = 0; iter < NUM_ITER; iter++)
			{
				int iterFinal = iter;
				
				writer.SetMergeFactor(1000);
				
				for (int i = 0; i < 200; i++)
				{
					Document d = new Document();
					d.Add(new Field("id", System.Convert.ToString(i), Field.Store.YES, Field.Index.NOT_ANALYZED));
					d.Add(new Field("contents", English.IntToEnglish(i), Field.Store.NO, Field.Index.ANALYZED));
					writer.AddDocument(d);
				}
				
				writer.SetMergeFactor(4);
				//writer.setInfoStream(System.out);
				
				int docCount = writer.DocCount();
				
				SupportClass.ThreadClass[] threads = new SupportClass.ThreadClass[NUM_THREADS];
				
				for (int i = 0; i < NUM_THREADS; i++)
				{
					int iFinal = i;
					IndexWriter writerFinal = writer;
					threads[i] = new AnonymousClassThread(writerFinal, iFinal, iterFinal, this);
				}
				
				for (int i = 0; i < NUM_THREADS; i++)
					threads[i].Start();
				
				for (int i = 0; i < NUM_THREADS; i++)
					threads[i].Join();
				
				Assert.IsTrue(!failed);
				
				int expectedDocCount = (int) ((1 + iter) * (200 + 8 * NUM_ITER2 * (NUM_THREADS / 2.0) * (1 + NUM_THREADS)));
				
				// System.out.println("TEST: now index=" + writer.segString());
				
				Assert.AreEqual(expectedDocCount, writer.DocCount());
				
				if (!autoCommit)
				{
					writer.Close();
					writer = new IndexWriter(directory, autoCommit, ANALYZER, false);
					writer.SetMaxBufferedDocs(2);
				}
				
				IndexReader reader = IndexReader.Open(directory);
				Assert.IsTrue(reader.IsOptimized());
				Assert.AreEqual(expectedDocCount, reader.NumDocs());
				reader.Close();
			}
			writer.Close();
		}
		
		/*
		Run above stress test against RAMDirectory and then
		FSDirectory.
		*/
		[Test]
		public virtual void  TestThreadedOptimize_Renamed()
		{
			Directory directory = new MockRAMDirectory();
			runTest(directory, false, new SerialMergeScheduler());
			runTest(directory, true, new SerialMergeScheduler());
			runTest(directory, false, new ConcurrentMergeScheduler());
			runTest(directory, true, new ConcurrentMergeScheduler());
			directory.Close();
			
			System.String tempDir = SupportClass.AppSettings.Get("tempDir", "");
			if (tempDir == null)
				throw new System.IO.IOException("tempDir undefined, cannot run test");
			
			System.String dirName = tempDir + "/luceneTestThreadedOptimize";
			directory = FSDirectory.Open(new System.IO.FileInfo(dirName));
			runTest(directory, false, new SerialMergeScheduler());
			runTest(directory, true, new SerialMergeScheduler());
			runTest(directory, false, new ConcurrentMergeScheduler());
			runTest(directory, true, new ConcurrentMergeScheduler());
			directory.Close();
			_TestUtil.RmDir(dirName);
		}
	}
}