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
using System.IO;

using NUnit.Framework;

using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using English = Lucene.Net.Util.English;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Store
{
	
	/// <summary> JUnit testcase to test RAMDirectory. RAMDirectory itself is used in many testcases,
	/// but not one of them uses an different constructor other than the default constructor.
	/// 
	/// 
	/// </summary>
	/// <version>  $Id: RAMDirectory.java 150537 2004-09-28 22:45:26 +0200 (Di, 28 Sep 2004) cutting $
	/// </version>
    [TestFixture]
	public class TestRAMDirectory:LuceneTestCase
	{
		private class AnonymousClassThread:SupportClass.ThreadClass
		{
			public AnonymousClassThread(int num, Lucene.Net.Index.IndexWriter writer, Lucene.Net.Store.MockRAMDirectory ramDir, TestRAMDirectory enclosingInstance)
			{
				InitBlock(num, writer, ramDir, enclosingInstance);
			}
			private void  InitBlock(int num, Lucene.Net.Index.IndexWriter writer, Lucene.Net.Store.MockRAMDirectory ramDir, TestRAMDirectory enclosingInstance)
			{
				this.num = num;
				this.writer = writer;
				this.ramDir = ramDir;
				this.enclosingInstance = enclosingInstance;
			}
			private int num;
			private Lucene.Net.Index.IndexWriter writer;
			private Lucene.Net.Store.MockRAMDirectory ramDir;
			private TestRAMDirectory enclosingInstance;
			public TestRAMDirectory Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			override public void  Run()
			{
				for (int j = 1; j < Enclosing_Instance.docsPerThread; j++)
				{
					Document doc = new Document();
					doc.Add(new Field("sizeContent", English.IntToEnglish(num * Enclosing_Instance.docsPerThread + j).Trim(), Field.Store.YES, Field.Index.NOT_ANALYZED));
					try
					{
						writer.AddDocument(doc);
					}
					catch (System.IO.IOException e)
					{
						throw new System.SystemException("", e);
					}
				}
			}
		}
		
		private System.IO.FileInfo indexDir = null;
		
		// add enough document so that the index will be larger than RAMDirectory.READ_BUFFER_SIZE
		private int docsToAdd = 500;
		
		// setup the index
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			System.String tempDir = System.IO.Path.GetTempPath();
			if (tempDir == null)
				throw new System.IO.IOException("java.io.tmpdir undefined, cannot run test");
			indexDir = new System.IO.FileInfo(Path.Combine(tempDir, "RAMDirIndex"));
			
			IndexWriter writer = new IndexWriter(indexDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			// add some documents
			Document doc = null;
			for (int i = 0; i < docsToAdd; i++)
			{
				doc = new Document();
				doc.Add(new Field("content", English.IntToEnglish(i).Trim(), Field.Store.YES, Field.Index.NOT_ANALYZED));
				writer.AddDocument(doc);
			}
			Assert.AreEqual(docsToAdd, writer.DocCount());
			writer.Close();
		}
		
        [Test]
		public virtual void  TestRAMDirectory_Renamed()
		{
			
			Directory dir = FSDirectory.Open(indexDir);
			MockRAMDirectory ramDir = new MockRAMDirectory(dir);
			
			// close the underlaying directory
			dir.Close();
			
			// Check size
			Assert.AreEqual(ramDir.SizeInBytes(), ramDir.GetRecomputedSizeInBytes());
			
			// open reader to test document count
			IndexReader reader = IndexReader.Open(ramDir);
			Assert.AreEqual(docsToAdd, reader.NumDocs());
			
			// open search zo check if all doc's are there
			IndexSearcher searcher = new IndexSearcher(reader);
			
			// search for all documents
			for (int i = 0; i < docsToAdd; i++)
			{
				Document doc = searcher.Doc(i);
				Assert.IsTrue(doc.GetField("content") != null);
			}
			
			// cleanup
			reader.Close();
			searcher.Close();
		}
		
        [Test]
		public virtual void  TestRAMDirectoryFile()
		{
			
			MockRAMDirectory ramDir = new MockRAMDirectory(indexDir);
			
			// Check size
			Assert.AreEqual(ramDir.SizeInBytes(), ramDir.GetRecomputedSizeInBytes());
			
			// open reader to test document count
			IndexReader reader = IndexReader.Open(ramDir);
			Assert.AreEqual(docsToAdd, reader.NumDocs());
			
			// open search zo check if all doc's are there
			IndexSearcher searcher = new IndexSearcher(reader);
			
			// search for all documents
			for (int i = 0; i < docsToAdd; i++)
			{
				Document doc = searcher.Doc(i);
				Assert.IsTrue(doc.GetField("content") != null);
			}
			
			// cleanup
			reader.Close();
			searcher.Close();
		}
		
        [Test]
		public virtual void  TestRAMDirectoryString()
		{
			
			MockRAMDirectory ramDir = new MockRAMDirectory(indexDir.FullName);
			
			// Check size
			Assert.AreEqual(ramDir.SizeInBytes(), ramDir.GetRecomputedSizeInBytes());
			
			// open reader to test document count
			IndexReader reader = IndexReader.Open(ramDir);
			Assert.AreEqual(docsToAdd, reader.NumDocs());
			
			// open search zo check if all doc's are there
			IndexSearcher searcher = new IndexSearcher(reader);
			
			// search for all documents
			for (int i = 0; i < docsToAdd; i++)
			{
				Document doc = searcher.Doc(i);
				Assert.IsTrue(doc.GetField("content") != null);
			}
			
			// cleanup
			reader.Close();
			searcher.Close();
		}
		
		private int numThreads = 10;
		private int docsPerThread = 40;
		
        [Test]
		public virtual void  TestRAMDirectorySize()
		{
			
			MockRAMDirectory ramDir = new MockRAMDirectory(indexDir.FullName);
			IndexWriter writer = new IndexWriter(ramDir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize();
			
			Assert.AreEqual(ramDir.SizeInBytes(), ramDir.GetRecomputedSizeInBytes());
			
			SupportClass.ThreadClass[] threads = new SupportClass.ThreadClass[numThreads];
			for (int i = 0; i < numThreads; i++)
			{
				int num = i;
				threads[i] = new AnonymousClassThread(num, writer, ramDir, this);
			}
			for (int i = 0; i < numThreads; i++)
				threads[i].Start();
			for (int i = 0; i < numThreads; i++)
				threads[i].Join();
			
			writer.Optimize();
			Assert.AreEqual(ramDir.SizeInBytes(), ramDir.GetRecomputedSizeInBytes());
			
			writer.Close();
		}
		
		
        [Test]
		public virtual void  TestSerializable()
		{
			Directory dir = new RAMDirectory();
			System.IO.MemoryStream bos = new System.IO.MemoryStream(1024);
			Assert.AreEqual(0, bos.Length, "initially empty");
			System.IO.BinaryWriter out_Renamed = new System.IO.BinaryWriter(bos);
			long headerSize = bos.Length;
	        System.Runtime.Serialization.Formatters.Binary.BinaryFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
	        formatter.Serialize(out_Renamed.BaseStream, dir);
			out_Renamed.Flush();  // In Java, this is Close(), but we can't do this in .NET, and the Close() is moved to after the validation check
			Assert.IsTrue(headerSize < bos.Length, "contains more then just header");
            out_Renamed.Close();
		}
		
		[TearDown]
		public override void  TearDown()
		{
			base.TearDown();
			// cleanup 
            if(System.IO.Directory.Exists(indexDir.FullName))
            {
                System.IO.Directory.Delete(indexDir.FullName, true);
            }
		}
		
		// LUCENE-1196
        [Test]
		public virtual void  TestIllegalEOF()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexOutput o = dir.CreateOutput("out");
			byte[] b = new byte[1024];
			o.WriteBytes(b, 0, 1024);
			o.Close();
			IndexInput i = dir.OpenInput("out");
			i.Seek(1024);
			i.Close();
			dir.Close();
		}
	}
}