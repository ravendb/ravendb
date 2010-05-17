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
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using HeavyAtomicInt = Lucene.Net.Index.TestIndexWriterReader.HeavyAtomicInt;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestNRTReaderWithThreads:LuceneTestCase
	{
		internal System.Random random = new System.Random();
		internal HeavyAtomicInt seq = new HeavyAtomicInt(1);
		
        [Test]
		public virtual void  TestIndexing()
		{
			Directory mainDir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(mainDir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetUseCompoundFile(false);
			IndexReader reader = writer.GetReader(); // start pooling readers
			reader.Close();
			writer.SetMergeFactor(2);
			writer.SetMaxBufferedDocs(10);
			RunThread[] indexThreads = new RunThread[4];
			for (int x = 0; x < indexThreads.Length; x++)
			{
				indexThreads[x] = new RunThread(this, x % 2, writer);
				indexThreads[x].Name = "Thread " + x;
				indexThreads[x].Start();
			}
			long startTime = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
			long duration = 5 * 1000;
			while (((DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) - startTime) < duration)
			{
				System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 100));
			}
			int delCount = 0;
			int addCount = 0;
			for (int x = 0; x < indexThreads.Length; x++)
			{
				indexThreads[x].run_Renamed_Field = false;
				Assert.IsTrue(indexThreads[x].ex == null);
				addCount += indexThreads[x].addCount;
				delCount += indexThreads[x].delCount;
			}
			for (int x = 0; x < indexThreads.Length; x++)
			{
				indexThreads[x].Join();
			}
			//System.out.println("addCount:"+addCount);
			//System.out.println("delCount:"+delCount);
			writer.Close();
			mainDir.Close();
		}
		
		public class RunThread:SupportClass.ThreadClass
		{
			private void  InitBlock(TestNRTReaderWithThreads enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestNRTReaderWithThreads enclosingInstance;
			public TestNRTReaderWithThreads Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal IndexWriter writer;
			internal bool run_Renamed_Field = true;
			internal System.Exception ex;
			internal int delCount = 0;
			internal int addCount = 0;
			internal int type;
			
			public RunThread(TestNRTReaderWithThreads enclosingInstance, int type, IndexWriter writer)
			{
				InitBlock(enclosingInstance);
				this.type = type;
				this.writer = writer;
			}
			
			override public void  Run()
			{
				try
				{
					while (run_Renamed_Field)
					{
						//int n = random.nextInt(2);
						if (type == 0)
						{
							int i = Enclosing_Instance.seq.AddAndGet(1);
							Document doc = TestIndexWriterReader.CreateDocument(i, "index1", 10);
							writer.AddDocument(doc);
							addCount++;
						}
						else if (type == 1)
						{
							// we may or may not delete because the term may not exist,
							// however we're opening and closing the reader rapidly
							IndexReader reader = writer.GetReader();
							int id = Enclosing_Instance.random.Next(Enclosing_Instance.seq.IntValue());
							Term term = new Term("id", System.Convert.ToString(id));
							int count = TestIndexWriterReader.Count(term, reader);
							writer.DeleteDocuments(term);
							reader.Close();
							delCount += count;
						}
					}
				}
				catch (System.Exception ex)
				{
					System.Console.Out.WriteLine(ex.StackTrace);
					this.ex = ex;
					run_Renamed_Field = false;
				}
			}
		}
	}
}