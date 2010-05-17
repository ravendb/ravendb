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
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Directory = Lucene.Net.Store.Directory;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	/// <summary> Tests for the "IndexModifier" class, including accesses from two threads at the
	/// same time.
	/// 
	/// </summary>
	/// <deprecated>
	/// </deprecated>
    [TestFixture]
	public class TestIndexModifier:LuceneTestCase
	{
		
		private int docCount = 0;
		
		private Term allDocTerm = new Term("all", "x");
		
		[Test]
		public virtual void  TestIndex()
		{
			Directory ramDir = new RAMDirectory();
			IndexModifier i = new IndexModifier(ramDir, new StandardAnalyzer(), true);
			i.AddDocument(GetDoc());
			Assert.AreEqual(1, i.DocCount());
			i.Flush();
			i.AddDocument(GetDoc(), new SimpleAnalyzer());
			Assert.AreEqual(2, i.DocCount());
			i.Optimize();
			Assert.AreEqual(2, i.DocCount());
			i.Flush();
			i.DeleteDocument(0);
			Assert.AreEqual(1, i.DocCount());
			i.Flush();
			Assert.AreEqual(1, i.DocCount());
			i.AddDocument(GetDoc());
			i.AddDocument(GetDoc());
			i.Flush();
			// depend on merge policy - Assert.AreEqual(3, i.docCount());
			i.DeleteDocuments(allDocTerm);
			Assert.AreEqual(0, i.DocCount());
			i.Optimize();
			Assert.AreEqual(0, i.DocCount());
			
			//  Lucene defaults:
			Assert.IsNull(i.GetInfoStream());
			Assert.IsTrue(i.GetUseCompoundFile());
			Assert.AreEqual(IndexWriter.DISABLE_AUTO_FLUSH, i.GetMaxBufferedDocs());
			Assert.AreEqual(10000, i.GetMaxFieldLength());
			Assert.AreEqual(10, i.GetMergeFactor());
			// test setting properties:
			i.SetMaxBufferedDocs(100);
			i.SetMergeFactor(25);
			i.SetMaxFieldLength(250000);
			i.AddDocument(GetDoc());
			i.SetUseCompoundFile(false);
			i.Flush();
			Assert.AreEqual(100, i.GetMaxBufferedDocs());
			Assert.AreEqual(25, i.GetMergeFactor());
			Assert.AreEqual(250000, i.GetMaxFieldLength());
			Assert.IsFalse(i.GetUseCompoundFile());
			
			// test setting properties when internally the reader is opened:
			i.DeleteDocuments(allDocTerm);
			i.SetMaxBufferedDocs(100);
			i.SetMergeFactor(25);
			i.SetMaxFieldLength(250000);
			i.AddDocument(GetDoc());
			i.SetUseCompoundFile(false);
			i.Optimize();
			Assert.AreEqual(100, i.GetMaxBufferedDocs());
			Assert.AreEqual(25, i.GetMergeFactor());
			Assert.AreEqual(250000, i.GetMaxFieldLength());
			Assert.IsFalse(i.GetUseCompoundFile());
			
			i.Close();
			try
			{
				i.DocCount();
				Assert.Fail();
			}
			catch (System.SystemException e)
			{
				// expected exception
			}
		}
		
		[Test]
		public virtual void  TestExtendedIndex()
		{
			Directory ramDir = new RAMDirectory();
			PowerIndex powerIndex = new PowerIndex(this, ramDir, new StandardAnalyzer(), true);
			powerIndex.AddDocument(GetDoc());
			powerIndex.AddDocument(GetDoc());
			powerIndex.AddDocument(GetDoc());
			powerIndex.AddDocument(GetDoc());
			powerIndex.AddDocument(GetDoc());
			powerIndex.Flush();
			Assert.AreEqual(5, powerIndex.DocFreq(allDocTerm));
			powerIndex.Close();
		}
		
		private Document GetDoc()
		{
			Document doc = new Document();
			doc.Add(new Field("body", System.Convert.ToString(docCount), Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.Add(new Field("all", "x", Field.Store.YES, Field.Index.NOT_ANALYZED));
			docCount++;
			return doc;
		}
		
		[Test]
		public virtual void  TestIndexWithThreads()
		{
			TestIndexInternal(0);
			TestIndexInternal(10);
			TestIndexInternal(50);
		}
		
		private void  TestIndexInternal(int maxWait)
		{
			bool create = true;
			//Directory rd = new RAMDirectory();
			// work on disk to make sure potential lock problems are tested:
			System.String tempDir = System.IO.Path.GetTempPath();
			if (tempDir == null)
				throw new System.IO.IOException("java.io.tmpdir undefined, cannot run test");
			System.IO.FileInfo indexDir = new System.IO.FileInfo(System.IO.Path.Combine(tempDir, "lucenetestindex"));
			Directory rd = FSDirectory.Open(indexDir);
			IndexThread.id = 0;
			IndexThread.idStack.Clear();
			IndexModifier index = new IndexModifier(rd, new StandardAnalyzer(), create);
			IndexThread thread1 = new IndexThread(index, maxWait, 1);
			thread1.Start();
			IndexThread thread2 = new IndexThread(index, maxWait, 2);
			thread2.Start();
			while (thread1.IsAlive || thread2.IsAlive)
			{
				System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 100));
			}
			index.Optimize();
			int added = thread1.added + thread2.added;
			int deleted = thread1.deleted + thread2.deleted;
			Assert.AreEqual(added - deleted, index.DocCount());
			index.Close();
			
			try
			{
				index.Close();
				Assert.Fail();
			}
			catch (System.SystemException e)
			{
				// expected exception
			}
			RmDir(indexDir);
		}
		
		private void  RmDir(System.IO.FileInfo dir)
		{
			System.IO.FileInfo[] files = SupportClass.FileSupport.GetFiles(dir);
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
		
		private class PowerIndex:IndexModifier
		{
			private void  InitBlock(TestIndexModifier enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestIndexModifier enclosingInstance;
			public TestIndexModifier Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public PowerIndex(TestIndexModifier enclosingInstance, Directory dir, Analyzer analyzer, bool create):base(dir, analyzer, create)
			{
				InitBlock(enclosingInstance);
			}
			public virtual int DocFreq(Term term)
			{
				lock (directory)
				{
					AssureOpen();
					CreateIndexReader();
					return indexReader.DocFreq(term);
				}
			}
		}
	}
	
	class IndexThread:SupportClass.ThreadClass
	{
		
		private const int TEST_SECONDS = 3; // how many seconds to run each test 
		
		internal static int id = 0;
		internal static System.Collections.ArrayList idStack = new System.Collections.ArrayList();
		
		internal int added = 0;
		internal int deleted = 0;
		
		private int maxWait = 10;
		private IndexModifier index;
		private int threadNumber;
		private System.Random random;
		
		internal IndexThread(IndexModifier index, int maxWait, int threadNumber)
		{
			this.index = index;
			this.maxWait = maxWait;
			this.threadNumber = threadNumber;
			// TODO: test case is not reproducible despite pseudo-random numbers:
			random = new System.Random((System.Int32) (101 + threadNumber)); // constant seed for better reproducability
		}
		
		override public void  Run()
		{
			
			long endTime = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) + 1000 * TEST_SECONDS;
			try
			{
				while ((DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) < endTime)
				{
					int rand = random.Next(101);
					if (rand < 5)
					{
						index.Optimize();
					}
					else if (rand < 60)
					{
						Document doc = GetDocument();
						index.AddDocument(doc);
						idStack.Add(doc.Get("id"));
						added++;
					}
					else
					{
						// we just delete the last document added and remove it
						// from the id stack so that it won't be removed twice:
						System.String delId = null;
						try
						{
                            delId = idStack[idStack.Count - 1] as System.String;
                            idStack.RemoveAt(idStack.Count - 1);
						}
						catch (System.ArgumentOutOfRangeException e)
						{
							continue;
						}
						Term delTerm = new Term("id", System.Int32.Parse(delId).ToString());
						int delCount = index.DeleteDocuments(delTerm);
						if (delCount != 1)
						{
							throw new System.SystemException("Internal error: " + threadNumber + " deleted " + delCount + " documents, term=" + delTerm);
						}
						deleted++;
					}
					if (maxWait > 0)
					{
						rand = random.Next(maxWait);
						//System.out.println("waiting " + rand + "ms");
						try
						{
							System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * rand));
						}
						catch (System.Threading.ThreadInterruptedException ie)
						{
							SupportClass.ThreadClass.Current().Interrupt();
							throw new System.SystemException("", ie);
						}
					}
				}
			}
			catch (System.IO.IOException e)
			{
				throw new System.SystemException("", e);
			}
		}
		
		private Document GetDocument()
		{
			Document doc = new Document();
			lock (GetType())
			{
				doc.Add(new Field("id", System.Convert.ToString(id), Field.Store.YES, Field.Index.NOT_ANALYZED));
				id++;
			}
			// add random stuff:
			doc.Add(new Field("content", System.Convert.ToString(random.Next(1000)), Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("content", System.Convert.ToString(random.Next(1000)), Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("all", "x", Field.Store.YES, Field.Index.ANALYZED));
			return doc;
		}
	}
}