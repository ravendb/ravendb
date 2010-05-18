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
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestIndexWriterMergePolicy:LuceneTestCase
	{
		
		// Test the normal case
		[Test]
		public virtual void  TestNormalCase()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			writer.SetMergeFactor(10);
			writer.SetMergePolicy(new LogDocMergePolicy(writer));
			
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer);
				CheckInvariants(writer);
			}
			
			writer.Close();
		}
		
		// Test to see if there is over merge
		[Test]
		public virtual void  TestNoOverMerge()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			writer.SetMergeFactor(10);
			writer.SetMergePolicy(new LogDocMergePolicy(writer));
			
			bool noOverMerge = false;
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer);
				CheckInvariants(writer);
				if (writer.GetNumBufferedDocuments() + writer.GetSegmentCount() >= 18)
				{
					noOverMerge = true;
				}
			}
			Assert.IsTrue(noOverMerge);
			
			writer.Close();
		}
		
		// Test the case where flush is forced after every addDoc
		[Test]
		public virtual void  TestForceFlush()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			writer.SetMergeFactor(10);
			LogDocMergePolicy mp = new LogDocMergePolicy(writer);
			mp.SetMinMergeDocs(100);
			writer.SetMergePolicy(mp);
			
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer);
				writer.Close();
				
				writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
				writer.SetMaxBufferedDocs(10);
				writer.SetMergePolicy(mp);
				mp.SetMinMergeDocs(100);
				writer.SetMergeFactor(10);
				CheckInvariants(writer);
			}
			
			writer.Close();
		}
		
		// Test the case where mergeFactor changes
		[Test]
		public virtual void  TestMergeFactorChange()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			writer.SetMergeFactor(100);
			writer.SetMergePolicy(new LogDocMergePolicy(writer));
			
			for (int i = 0; i < 250; i++)
			{
				AddDoc(writer);
				CheckInvariants(writer);
			}
			
			writer.SetMergeFactor(5);
			
			// merge policy only fixes segments on levels where merges
			// have been triggered, so check invariants after all adds
			for (int i = 0; i < 10; i++)
			{
				AddDoc(writer);
			}
			CheckInvariants(writer);
			
			writer.Close();
		}
		
		// Test the case where both mergeFactor and maxBufferedDocs change
		[Test]
		public virtual void  TestMaxBufferedDocsChange()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, true, new WhitespaceAnalyzer(), true);
			writer.SetMaxBufferedDocs(101);
			writer.SetMergeFactor(101);
			writer.SetMergePolicy(new LogDocMergePolicy(writer));
			
			// leftmost* segment has 1 doc
			// rightmost* segment has 100 docs
			for (int i = 1; i <= 100; i++)
			{
				for (int j = 0; j < i; j++)
				{
					AddDoc(writer);
					CheckInvariants(writer);
				}
				writer.Close();
				
				writer = new IndexWriter(dir, true, new WhitespaceAnalyzer(), false);
				writer.SetMaxBufferedDocs(101);
				writer.SetMergeFactor(101);
				writer.SetMergePolicy(new LogDocMergePolicy(writer));
			}
			
			writer.SetMaxBufferedDocs(10);
			writer.SetMergeFactor(10);
			
			// merge policy only fixes segments on levels where merges
			// have been triggered, so check invariants after all adds
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer);
			}
			CheckInvariants(writer);
			
			for (int i = 100; i < 1000; i++)
			{
				AddDoc(writer);
			}
			CheckInvariants(writer);
			
			writer.Close();
		}
		
		// Test the case where a merge results in no doc at all
		[Test]
		public virtual void  TestMergeDocCount0()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, true, new WhitespaceAnalyzer(), true);
			writer.SetMergePolicy(new LogDocMergePolicy(writer));
			writer.SetMaxBufferedDocs(10);
			writer.SetMergeFactor(100);
			
			for (int i = 0; i < 250; i++)
			{
				AddDoc(writer);
				CheckInvariants(writer);
			}
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			reader.DeleteDocuments(new Term("content", "aaa"));
			reader.Close();
			
			writer = new IndexWriter(dir, true, new WhitespaceAnalyzer(), false);
			writer.SetMergePolicy(new LogDocMergePolicy(writer));
			writer.SetMaxBufferedDocs(10);
			writer.SetMergeFactor(5);
			
			// merge factor is changed, so check invariants after all adds
			for (int i = 0; i < 10; i++)
			{
				AddDoc(writer);
			}
			CheckInvariants(writer);
			Assert.AreEqual(10, writer.DocCount());
			
			writer.Close();
		}
		
		private void  AddDoc(IndexWriter writer)
		{
			Document doc = new Document();
			doc.Add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
		}
		
		private void  CheckInvariants(IndexWriter writer)
		{
			_TestUtil.SyncConcurrentMerges(writer);
			int maxBufferedDocs = writer.GetMaxBufferedDocs();
			int mergeFactor = writer.GetMergeFactor();
			int maxMergeDocs = writer.GetMaxMergeDocs();
			
			int ramSegmentCount = writer.GetNumBufferedDocuments();
			Assert.IsTrue(ramSegmentCount < maxBufferedDocs);
			
			int lowerBound = - 1;
			int upperBound = maxBufferedDocs;
			int numSegments = 0;
			
			int segmentCount = writer.GetSegmentCount();
			for (int i = segmentCount - 1; i >= 0; i--)
			{
				int docCount = writer.GetDocCount(i);
				Assert.IsTrue(docCount > lowerBound);
				
				if (docCount <= upperBound)
				{
					numSegments++;
				}
				else
				{
					if (upperBound * mergeFactor <= maxMergeDocs)
					{
						Assert.IsTrue(numSegments < mergeFactor);
					}
					
					do 
					{
						lowerBound = upperBound;
						upperBound *= mergeFactor;
					}
					while (docCount > upperBound);
					numSegments = 1;
				}
			}
			if (upperBound * mergeFactor <= maxMergeDocs)
			{
				Assert.IsTrue(numSegments < mergeFactor);
			}
			
			System.String[] files = writer.GetDirectory().ListAll();
			int segmentCfsCount = 0;
			for (int i = 0; i < files.Length; i++)
			{
				if (files[i].EndsWith(".cfs"))
				{
					segmentCfsCount++;
				}
			}
			Assert.AreEqual(segmentCount, segmentCfsCount);
		}
		
		/*
		private void printSegmentDocCounts(IndexWriter writer) {
		int segmentCount = writer.getSegmentCount();
		System.out.println("" + segmentCount + " segments total");
		for (int i = 0; i < segmentCount; i++) {
		System.out.println("  segment " + i + " has " + writer.getDocCount(i)
		+ " docs");
		}
		}
		*/
	}
}