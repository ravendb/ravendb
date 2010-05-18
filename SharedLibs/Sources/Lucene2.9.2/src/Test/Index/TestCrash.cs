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
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using NoLockFactory = Lucene.Net.Store.NoLockFactory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	[TestFixture]
	public class TestCrash:LuceneTestCase
	{
		
		private IndexWriter InitIndex()
		{
			return InitIndex(new MockRAMDirectory());
		}
		
		private IndexWriter InitIndex(MockRAMDirectory dir)
		{
			dir.SetLockFactory(NoLockFactory.GetNoLockFactory());
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer());
			//writer.setMaxBufferedDocs(2);
			writer.SetMaxBufferedDocs(10);
			((ConcurrentMergeScheduler) writer.GetMergeScheduler()).SetSuppressExceptions();
			
			Document doc = new Document();
			doc.Add(new Field("content", "aaa", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("id", "0", Field.Store.YES, Field.Index.ANALYZED));
			for (int i = 0; i < 157; i++)
				writer.AddDocument(doc);
			
			return writer;
		}
		
		private void  Crash(IndexWriter writer)
		{
			MockRAMDirectory dir = (MockRAMDirectory) writer.GetDirectory();
			ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) writer.GetMergeScheduler();
			dir.Crash();
			cms.Sync();
			dir.ClearCrash();
		}
		
		[Test]
		public virtual void  TestCrashWhileIndexing()
		{
			IndexWriter writer = InitIndex();
			MockRAMDirectory dir = (MockRAMDirectory) writer.GetDirectory();
			Crash(writer);
			IndexReader reader = IndexReader.Open(dir);
			Assert.IsTrue(reader.NumDocs() < 157);
		}
		
		[Test]
		public virtual void  TestWriterAfterCrash()
		{
			IndexWriter writer = InitIndex();
			MockRAMDirectory dir = (MockRAMDirectory) writer.GetDirectory();
			dir.SetPreventDoubleWrite(false);
			Crash(writer);
			writer = InitIndex(dir);
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			Assert.IsTrue(reader.NumDocs() < 314);
		}
		
		[Test]
		public virtual void  TestCrashAfterReopen()
		{
			IndexWriter writer = InitIndex();
			MockRAMDirectory dir = (MockRAMDirectory) writer.GetDirectory();
			writer.Close();
			writer = InitIndex(dir);
			Assert.AreEqual(314, writer.DocCount());
			Crash(writer);
			
			/*
			System.out.println("\n\nTEST: open reader");
			String[] l = dir.list();
			Arrays.sort(l);
			for(int i=0;i<l.length;i++)
			System.out.println("file " + i + " = " + l[i] + " " +
			dir.fileLength(l[i]) + " bytes");
			*/
			
			IndexReader reader = IndexReader.Open(dir);
			Assert.IsTrue(reader.NumDocs() >= 157);
		}
		
		[Test]
		public virtual void  TestCrashAfterClose()
		{
			
			IndexWriter writer = InitIndex();
			MockRAMDirectory dir = (MockRAMDirectory) writer.GetDirectory();
			
			writer.Close();
			dir.Crash();
			
			/*
			String[] l = dir.list();
			Arrays.sort(l);
			for(int i=0;i<l.length;i++)
			System.out.println("file " + i + " = " + l[i] + " " + dir.fileLength(l[i]) + " bytes");
			*/
			
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(157, reader.NumDocs());
		}
		
		[Test]
		public virtual void  TestCrashAfterCloseNoWait()
		{
			
			IndexWriter writer = InitIndex();
			MockRAMDirectory dir = (MockRAMDirectory) writer.GetDirectory();
			
			writer.Close(false);
			
			dir.Crash();
			
			/*
			String[] l = dir.list();
			Arrays.sort(l);
			for(int i=0;i<l.length;i++)
			System.out.println("file " + i + " = " + l[i] + " " + dir.fileLength(l[i]) + " bytes");
			*/
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(157, reader.NumDocs());
		}
		
		[Test]
		public virtual void  TestCrashReaderDeletes()
		{
			
			IndexWriter writer = InitIndex();
			MockRAMDirectory dir = (MockRAMDirectory) writer.GetDirectory();
			
			writer.Close(false);
			IndexReader reader = IndexReader.Open(dir);
			reader.DeleteDocument(3);
			
			dir.Crash();
			
			/*
			String[] l = dir.list();
			Arrays.sort(l);
			for(int i=0;i<l.length;i++)
			System.out.println("file " + i + " = " + l[i] + " " + dir.fileLength(l[i]) + " bytes");
			*/
			reader = IndexReader.Open(dir);
			Assert.AreEqual(157, reader.NumDocs());
		}
		
		[Test]
		public virtual void  TestCrashReaderDeletesAfterClose()
		{
			
			IndexWriter writer = InitIndex();
			MockRAMDirectory dir = (MockRAMDirectory) writer.GetDirectory();
			
			writer.Close(false);
			IndexReader reader = IndexReader.Open(dir);
			reader.DeleteDocument(3);
			reader.Close();
			
			dir.Crash();
			
			/*
			String[] l = dir.list();
			Arrays.sort(l);
			for(int i=0;i<l.length;i++)
			System.out.println("file " + i + " = " + l[i] + " " + dir.fileLength(l[i]) + " bytes");
			*/
			reader = IndexReader.Open(dir);
			Assert.AreEqual(156, reader.NumDocs());
		}
	}
}