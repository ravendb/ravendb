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
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using PhraseQuery = Lucene.Net.Search.PhraseQuery;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	[TestFixture]
	public class TestAddIndexesNoOptimize:LuceneTestCase
	{
		[Test]
		public virtual void  TestSimpleCase()
		{
			// main directory
			Directory dir = new RAMDirectory();
			// two auxiliary directories
			Directory aux = new RAMDirectory();
			Directory aux2 = new RAMDirectory();
			
			IndexWriter writer = null;
			
			writer = NewWriter(dir, true);
			// add 100 documents
			AddDocs(writer, 100);
			Assert.AreEqual(100, writer.DocCount());
			writer.Close();
			
			writer = NewWriter(aux, true);
			writer.SetUseCompoundFile(false); // use one without a compound file
			// add 40 documents in separate files
			AddDocs(writer, 40);
			Assert.AreEqual(40, writer.DocCount());
			writer.Close();
			
			writer = NewWriter(aux2, true);
			// add 40 documents in compound files
			AddDocs2(writer, 50);
			Assert.AreEqual(50, writer.DocCount());
			writer.Close();
			
			// test doc count before segments are merged
			writer = NewWriter(dir, false);
			Assert.AreEqual(100, writer.DocCount());
			writer.AddIndexesNoOptimize(new Directory[]{aux, aux2});
			Assert.AreEqual(190, writer.DocCount());
			writer.Close();
			
			// make sure the old index is correct
			VerifyNumDocs(aux, 40);
			
			// make sure the new index is correct
			VerifyNumDocs(dir, 190);
			
			// now add another set in.
			Directory aux3 = new RAMDirectory();
			writer = NewWriter(aux3, true);
			// add 40 documents
			AddDocs(writer, 40);
			Assert.AreEqual(40, writer.DocCount());
			writer.Close();
			
			// test doc count before segments are merged/index is optimized
			writer = NewWriter(dir, false);
			Assert.AreEqual(190, writer.DocCount());
			writer.AddIndexesNoOptimize(new Directory[]{aux3});
			Assert.AreEqual(230, writer.DocCount());
			writer.Close();
			
			// make sure the new index is correct
			VerifyNumDocs(dir, 230);
			
			VerifyTermDocs(dir, new Term("content", "aaa"), 180);
			
			VerifyTermDocs(dir, new Term("content", "bbb"), 50);
			
			// now optimize it.
			writer = NewWriter(dir, false);
			writer.Optimize();
			writer.Close();
			
			// make sure the new index is correct
			VerifyNumDocs(dir, 230);
			
			VerifyTermDocs(dir, new Term("content", "aaa"), 180);
			
			VerifyTermDocs(dir, new Term("content", "bbb"), 50);
			
			// now add a single document
			Directory aux4 = new RAMDirectory();
			writer = NewWriter(aux4, true);
			AddDocs2(writer, 1);
			writer.Close();
			
			writer = NewWriter(dir, false);
			Assert.AreEqual(230, writer.DocCount());
			writer.AddIndexesNoOptimize(new Directory[]{aux4});
			Assert.AreEqual(231, writer.DocCount());
			writer.Close();
			
			VerifyNumDocs(dir, 231);
			
			VerifyTermDocs(dir, new Term("content", "bbb"), 51);
		}
		
		[Test]
		public virtual void  TestWithPendingDeletes()
		{
			// main directory
			Directory dir = new RAMDirectory();
			// auxiliary directory
			Directory aux = new RAMDirectory();
			
			SetUpDirs(dir, aux);
			IndexWriter writer = NewWriter(dir, false);
			writer.AddIndexesNoOptimize(new Directory[]{aux});
			
			// Adds 10 docs, then replaces them with another 10
			// docs, so 10 pending deletes:
			for (int i = 0; i < 20; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("id", "" + (i % 10), Field.Store.NO, Field.Index.NOT_ANALYZED));
				doc.Add(new Field("content", "bbb " + i, Field.Store.NO, Field.Index.ANALYZED));
				writer.UpdateDocument(new Term("id", "" + (i % 10)), doc);
			}
			// Deletes one of the 10 added docs, leaving 9:
			PhraseQuery q = new PhraseQuery();
			q.Add(new Term("content", "bbb"));
			q.Add(new Term("content", "14"));
			writer.DeleteDocuments(q);
			
			writer.Optimize();
			
			VerifyNumDocs(dir, 1039);
			VerifyTermDocs(dir, new Term("content", "aaa"), 1030);
			VerifyTermDocs(dir, new Term("content", "bbb"), 9);
			
			writer.Close();
			dir.Close();
			aux.Close();
		}
		
		[Test]
		public virtual void  TestWithPendingDeletes2()
		{
			// main directory
			Directory dir = new RAMDirectory();
			// auxiliary directory
			Directory aux = new RAMDirectory();
			
			SetUpDirs(dir, aux);
			IndexWriter writer = NewWriter(dir, false);
			
			// Adds 10 docs, then replaces them with another 10
			// docs, so 10 pending deletes:
			for (int i = 0; i < 20; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("id", "" + (i % 10), Field.Store.NO, Field.Index.NOT_ANALYZED));
				doc.Add(new Field("content", "bbb " + i, Field.Store.NO, Field.Index.ANALYZED));
				writer.UpdateDocument(new Term("id", "" + (i % 10)), doc);
			}
			
			writer.AddIndexesNoOptimize(new Directory[]{aux});
			
			// Deletes one of the 10 added docs, leaving 9:
			PhraseQuery q = new PhraseQuery();
			q.Add(new Term("content", "bbb"));
			q.Add(new Term("content", "14"));
			writer.DeleteDocuments(q);
			
			writer.Optimize();
			
			VerifyNumDocs(dir, 1039);
			VerifyTermDocs(dir, new Term("content", "aaa"), 1030);
			VerifyTermDocs(dir, new Term("content", "bbb"), 9);
			
			writer.Close();
			dir.Close();
			aux.Close();
		}
		
		[Test]
		public virtual void  TestWithPendingDeletes3()
		{
			// main directory
			Directory dir = new RAMDirectory();
			// auxiliary directory
			Directory aux = new RAMDirectory();
			
			SetUpDirs(dir, aux);
			IndexWriter writer = NewWriter(dir, false);
			
			// Adds 10 docs, then replaces them with another 10
			// docs, so 10 pending deletes:
			for (int i = 0; i < 20; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("id", "" + (i % 10), Field.Store.NO, Field.Index.NOT_ANALYZED));
				doc.Add(new Field("content", "bbb " + i, Field.Store.NO, Field.Index.ANALYZED));
				writer.UpdateDocument(new Term("id", "" + (i % 10)), doc);
			}
			
			// Deletes one of the 10 added docs, leaving 9:
			PhraseQuery q = new PhraseQuery();
			q.Add(new Term("content", "bbb"));
			q.Add(new Term("content", "14"));
			writer.DeleteDocuments(q);
			
			writer.AddIndexesNoOptimize(new Directory[]{aux});
			
			writer.Optimize();
			
			VerifyNumDocs(dir, 1039);
			VerifyTermDocs(dir, new Term("content", "aaa"), 1030);
			VerifyTermDocs(dir, new Term("content", "bbb"), 9);
			
			writer.Close();
			dir.Close();
			aux.Close();
		}
		
		// case 0: add self or exceed maxMergeDocs, expect exception
		[Test]
		public virtual void  TestAddSelf()
		{
			// main directory
			Directory dir = new RAMDirectory();
			// auxiliary directory
			Directory aux = new RAMDirectory();
			
			IndexWriter writer = null;
			
			writer = NewWriter(dir, true);
			// add 100 documents
			AddDocs(writer, 100);
			Assert.AreEqual(100, writer.DocCount());
			writer.Close();
			
			writer = NewWriter(aux, true);
			writer.SetUseCompoundFile(false); // use one without a compound file
			writer.SetMaxBufferedDocs(1000);
			// add 140 documents in separate files
			AddDocs(writer, 40);
			writer.Close();
			writer = NewWriter(aux, true);
			writer.SetUseCompoundFile(false); // use one without a compound file
			writer.SetMaxBufferedDocs(1000);
			AddDocs(writer, 100);
			writer.Close();
			
			writer = NewWriter(dir, false);
			try
			{
				// cannot add self
				writer.AddIndexesNoOptimize(new Directory[]{aux, dir});
				Assert.IsTrue(false);
			}
			catch (System.ArgumentException e)
			{
				Assert.AreEqual(100, writer.DocCount());
			}
			writer.Close();
			
			// make sure the index is correct
			VerifyNumDocs(dir, 100);
		}
		
		// in all the remaining tests, make the doc count of the oldest segment
		// in dir large so that it is never merged in addIndexesNoOptimize()
		// case 1: no tail segments
		[Test]
		public virtual void  TestNoTailSegments()
		{
			// main directory
			Directory dir = new RAMDirectory();
			// auxiliary directory
			Directory aux = new RAMDirectory();
			
			SetUpDirs(dir, aux);
			
			IndexWriter writer = NewWriter(dir, false);
			writer.SetMaxBufferedDocs(10);
			writer.SetMergeFactor(4);
			AddDocs(writer, 10);
			
			writer.AddIndexesNoOptimize(new Directory[]{aux});
			Assert.AreEqual(1040, writer.DocCount());
			Assert.AreEqual(2, writer.GetSegmentCount());
			Assert.AreEqual(1000, writer.GetDocCount(0));
			writer.Close();
			
			// make sure the index is correct
			VerifyNumDocs(dir, 1040);
		}
		
		// case 2: tail segments, invariants hold, no copy
		[Test]
		public virtual void  TestNoCopySegments()
		{
			// main directory
			Directory dir = new RAMDirectory();
			// auxiliary directory
			Directory aux = new RAMDirectory();
			
			SetUpDirs(dir, aux);
			
			IndexWriter writer = NewWriter(dir, false);
			writer.SetMaxBufferedDocs(9);
			writer.SetMergeFactor(4);
			AddDocs(writer, 2);
			
			writer.AddIndexesNoOptimize(new Directory[]{aux});
			Assert.AreEqual(1032, writer.DocCount());
			Assert.AreEqual(2, writer.GetSegmentCount());
			Assert.AreEqual(1000, writer.GetDocCount(0));
			writer.Close();
			
			// make sure the index is correct
			VerifyNumDocs(dir, 1032);
		}
		
		// case 3: tail segments, invariants hold, copy, invariants hold
		[Test]
		public virtual void  TestNoMergeAfterCopy()
		{
			// main directory
			Directory dir = new RAMDirectory();
			// auxiliary directory
			Directory aux = new RAMDirectory();
			
			SetUpDirs(dir, aux);
			
			IndexWriter writer = NewWriter(dir, false);
			writer.SetMaxBufferedDocs(10);
			writer.SetMergeFactor(4);
			
			writer.AddIndexesNoOptimize(new Directory[]{aux, new RAMDirectory(aux)});
			Assert.AreEqual(1060, writer.DocCount());
			Assert.AreEqual(1000, writer.GetDocCount(0));
			writer.Close();
			
			// make sure the index is correct
			VerifyNumDocs(dir, 1060);
		}
		
		// case 4: tail segments, invariants hold, copy, invariants not hold
		[Test]
		public virtual void  TestMergeAfterCopy()
		{
			// main directory
			Directory dir = new RAMDirectory();
			// auxiliary directory
			Directory aux = new RAMDirectory();
			
			SetUpDirs(dir, aux);
			
			IndexReader reader = IndexReader.Open(aux);
			for (int i = 0; i < 20; i++)
			{
				reader.DeleteDocument(i);
			}
			Assert.AreEqual(10, reader.NumDocs());
			reader.Close();
			
			IndexWriter writer = NewWriter(dir, false);
			writer.SetMaxBufferedDocs(4);
			writer.SetMergeFactor(4);
			
			writer.AddIndexesNoOptimize(new Directory[]{aux, new RAMDirectory(aux)});
			Assert.AreEqual(1020, writer.DocCount());
			Assert.AreEqual(1000, writer.GetDocCount(0));
			writer.Close();
			
			// make sure the index is correct
			VerifyNumDocs(dir, 1020);
		}
		
		// case 5: tail segments, invariants not hold
		[Test]
		public virtual void  TestMoreMerges()
		{
			// main directory
			Directory dir = new RAMDirectory();
			// auxiliary directory
			Directory aux = new RAMDirectory();
			Directory aux2 = new RAMDirectory();
			
			SetUpDirs(dir, aux);
			
			IndexWriter writer = NewWriter(aux2, true);
			writer.SetMaxBufferedDocs(100);
			writer.SetMergeFactor(10);
			writer.AddIndexesNoOptimize(new Directory[]{aux});
			Assert.AreEqual(30, writer.DocCount());
			Assert.AreEqual(3, writer.GetSegmentCount());
			writer.Close();
			
			IndexReader reader = IndexReader.Open(aux);
			for (int i = 0; i < 27; i++)
			{
				reader.DeleteDocument(i);
			}
			Assert.AreEqual(3, reader.NumDocs());
			reader.Close();
			
			reader = IndexReader.Open(aux2);
			for (int i = 0; i < 8; i++)
			{
				reader.DeleteDocument(i);
			}
			Assert.AreEqual(22, reader.NumDocs());
			reader.Close();
			
			writer = NewWriter(dir, false);
			writer.SetMaxBufferedDocs(6);
			writer.SetMergeFactor(4);
			
			writer.AddIndexesNoOptimize(new Directory[]{aux, aux2});
			Assert.AreEqual(1025, writer.DocCount());
			Assert.AreEqual(1000, writer.GetDocCount(0));
			writer.Close();
			
			// make sure the index is correct
			VerifyNumDocs(dir, 1025);
		}
		
		private IndexWriter NewWriter(Directory dir, bool create)
		{
			IndexWriter writer = new IndexWriter(dir, true, new WhitespaceAnalyzer(), create);
			writer.SetMergePolicy(new LogDocMergePolicy(writer));
			return writer;
		}
		
		private void  AddDocs(IndexWriter writer, int numDocs)
		{
			for (int i = 0; i < numDocs; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
		}
		
		private void  AddDocs2(IndexWriter writer, int numDocs)
		{
			for (int i = 0; i < numDocs; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("content", "bbb", Field.Store.NO, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
		}
		
		private void  VerifyNumDocs(Directory dir, int numDocs)
		{
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(numDocs, reader.MaxDoc());
			Assert.AreEqual(numDocs, reader.NumDocs());
			reader.Close();
		}
		
		private void  VerifyTermDocs(Directory dir, Term term, int numDocs)
		{
			IndexReader reader = IndexReader.Open(dir);
			TermDocs termDocs = reader.TermDocs(term);
			int count = 0;
			while (termDocs.Next())
				count++;
			Assert.AreEqual(numDocs, count);
			reader.Close();
		}
		
		private void  SetUpDirs(Directory dir, Directory aux)
		{
			IndexWriter writer = null;
			
			writer = NewWriter(dir, true);
			writer.SetMaxBufferedDocs(1000);
			// add 1000 documents in 1 segment
			AddDocs(writer, 1000);
			Assert.AreEqual(1000, writer.DocCount());
			Assert.AreEqual(1, writer.GetSegmentCount());
			writer.Close();
			
			writer = NewWriter(aux, true);
			writer.SetUseCompoundFile(false); // use one without a compound file
			writer.SetMaxBufferedDocs(100);
			writer.SetMergeFactor(10);
			// add 30 documents in 3 segments
			for (int i = 0; i < 3; i++)
			{
				AddDocs(writer, 10);
				writer.Close();
				writer = NewWriter(aux, false);
				writer.SetUseCompoundFile(false); // use one without a compound file
				writer.SetMaxBufferedDocs(100);
				writer.SetMergeFactor(10);
			}
			Assert.AreEqual(30, writer.DocCount());
			Assert.AreEqual(3, writer.GetSegmentCount());
			writer.Close();
		}
		
		// LUCENE-1270
		[Test]
		public virtual void  TestHangOnClose()
		{
			
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMergePolicy(new LogByteSizeMergePolicy(writer));
			writer.SetMaxBufferedDocs(5);
			writer.SetUseCompoundFile(false);
			writer.SetMergeFactor(100);
			
			Document doc = new Document();
			doc.Add(new Field("content", "aaa bbb ccc ddd eee fff ggg hhh iii", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			for (int i = 0; i < 60; i++)
				writer.AddDocument(doc);
			writer.SetMaxBufferedDocs(200);
			Document doc2 = new Document();
			doc2.Add(new Field("content", "aaa bbb ccc ddd eee fff ggg hhh iii", Field.Store.YES, Field.Index.NO));
			doc2.Add(new Field("content", "aaa bbb ccc ddd eee fff ggg hhh iii", Field.Store.YES, Field.Index.NO));
			doc2.Add(new Field("content", "aaa bbb ccc ddd eee fff ggg hhh iii", Field.Store.YES, Field.Index.NO));
			doc2.Add(new Field("content", "aaa bbb ccc ddd eee fff ggg hhh iii", Field.Store.YES, Field.Index.NO));
			for (int i = 0; i < 10; i++)
				writer.AddDocument(doc2);
			writer.Close();
			
			Directory dir2 = new MockRAMDirectory();
			writer = new IndexWriter(dir2, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			LogByteSizeMergePolicy lmp = new LogByteSizeMergePolicy(writer);
			lmp.SetMinMergeMB(0.0001);
			writer.SetMergePolicy(lmp);
			writer.SetMergeFactor(4);
			writer.SetUseCompoundFile(false);
			writer.SetMergeScheduler(new SerialMergeScheduler());
			writer.AddIndexesNoOptimize(new Directory[]{dir});
			writer.Close();
			dir.Close();
			dir2.Close();
		}
		
		// LUCENE-1642: make sure CFS of destination indexwriter
		// is respected when copying tail segments
		[Test]
		public virtual void  TestTargetCFS()
		{
			Directory dir = new RAMDirectory();
			IndexWriter writer = NewWriter(dir, true);
			writer.SetUseCompoundFile(false);
			AddDocs(writer, 1);
			writer.Close();
			
			Directory other = new RAMDirectory();
			writer = NewWriter(other, true);
			writer.SetUseCompoundFile(true);
			writer.AddIndexesNoOptimize(new Directory[]{dir});
			Assert.IsTrue(writer.NewestSegment().GetUseCompoundFile());
			writer.Close();
		}
	}
}