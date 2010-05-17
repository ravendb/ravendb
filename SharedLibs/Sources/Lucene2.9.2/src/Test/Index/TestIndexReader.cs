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
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using FieldSelector = Lucene.Net.Documents.FieldSelector;
using Fieldable = Lucene.Net.Documents.Fieldable;
using SetBasedFieldSelector = Lucene.Net.Documents.SetBasedFieldSelector;
using FieldOption = Lucene.Net.Index.IndexReader.FieldOption;
using AlreadyClosedException = Lucene.Net.Store.AlreadyClosedException;
using Directory = Lucene.Net.Store.Directory;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using LockObtainFailedException = Lucene.Net.Store.LockObtainFailedException;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using NoSuchDirectoryException = Lucene.Net.Store.NoSuchDirectoryException;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using FieldCache = Lucene.Net.Search.FieldCache;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using TermQuery = Lucene.Net.Search.TermQuery;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestIndexReader:LuceneTestCase
	{
		/// <summary>Main for running test case by itself. </summary>
		[STAThread]
		public static void  Main(System.String[] args)
		{
			// TestRunner.run(new TestSuite(typeof(TestIndexReader))); // {{Aroush-2.9}} how is this done in NUnit?
			//        TestRunner.run (new TestIndexReader("testBasicDelete"));
			//        TestRunner.run (new TestIndexReader("testDeleteReaderWriterConflict"));
			//        TestRunner.run (new TestIndexReader("testDeleteReaderReaderConflict"));
			//        TestRunner.run (new TestIndexReader("testFilesOpenClose"));
		}
		
		public TestIndexReader(System.String name):base(name)
		{
		}

        public TestIndexReader(): base("")
        {
        }
		
		[Test]
		public virtual void  TestCommitUserData()
		{
			RAMDirectory d = new MockRAMDirectory();

            System.Collections.Generic.IDictionary<string, string> commitUserData = new System.Collections.Generic.Dictionary<string,string>();
			commitUserData["foo"] = "fighters";
			
			// set up writer
			IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			for (int i = 0; i < 27; i++)
				AddDocumentWithFields(writer);
			writer.Close();
			
			IndexReader r = IndexReader.Open(d);
			r.DeleteDocument(5);
			r.Flush(commitUserData);
			r.Close();
			
			SegmentInfos sis = new SegmentInfos();
			sis.Read(d);
			IndexReader r2 = IndexReader.Open(d);
			IndexCommit c = r.GetIndexCommit();
			Assert.AreEqual(c.GetUserData(), commitUserData);
			
			Assert.AreEqual(sis.GetCurrentSegmentFileName(), c.GetSegmentsFileName());
			
			Assert.IsTrue(c.Equals(r.GetIndexCommit()));
			
			// Change the index
			writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			for (int i = 0; i < 7; i++)
				AddDocumentWithFields(writer);
			writer.Close();
			
			IndexReader r3 = r2.Reopen();
			Assert.IsFalse(c.Equals(r3.GetIndexCommit()));
			Assert.IsFalse(r2.GetIndexCommit().IsOptimized());
			r3.Close();
			
			writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize();
			writer.Close();
			
			r3 = r2.Reopen();
			Assert.IsTrue(r3.GetIndexCommit().IsOptimized());
			r2.Close();
			r3.Close();
			d.Close();
		}
		
		[Test]
		public virtual void  TestIsCurrent()
		{
			RAMDirectory d = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			writer.Close();
			// set up reader:
			IndexReader reader = IndexReader.Open(d);
			Assert.IsTrue(reader.IsCurrent());
			// modify index by adding another document:
			writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			writer.Close();
			Assert.IsFalse(reader.IsCurrent());
			// re-create index:
			writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			writer.Close();
			Assert.IsFalse(reader.IsCurrent());
			reader.Close();
			d.Close();
		}
		
		/// <summary> Tests the IndexReader.getFieldNames implementation</summary>
		/// <throws>  Exception on error </throws>
		[Test]
		public virtual void  TestGetFieldNames()
		{
			RAMDirectory d = new MockRAMDirectory();
			// set up writer
			IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			writer.Close();
			// set up reader
			IndexReader reader = IndexReader.Open(d);
			System.Collections.Generic.ICollection<string> fieldNames = reader.GetFieldNames(IndexReader.FieldOption.ALL);
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "keyword"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "text"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "unindexed"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "unstored"));
			reader.Close();
			// add more documents
			writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			// want to get some more segments here
			for (int i = 0; i < 5 * writer.GetMergeFactor(); i++)
			{
				AddDocumentWithFields(writer);
			}
			// new fields are in some different segments (we hope)
			for (int i = 0; i < 5 * writer.GetMergeFactor(); i++)
			{
				AddDocumentWithDifferentFields(writer);
			}
			// new termvector fields
			for (int i = 0; i < 5 * writer.GetMergeFactor(); i++)
			{
				AddDocumentWithTermVectorFields(writer);
			}
			
			writer.Close();
			// verify fields again
			reader = IndexReader.Open(d);
			fieldNames = reader.GetFieldNames(IndexReader.FieldOption.ALL);
			Assert.AreEqual(13, fieldNames.Count); // the following fields
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "keyword"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "text"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "unindexed"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "unstored"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "keyword2"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "text2"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "unindexed2"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "unstored2"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "tvnot"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "termvector"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "tvposition"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "tvoffset"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "tvpositionoffset"));
			
			// verify that only indexed fields were returned
			fieldNames = reader.GetFieldNames(IndexReader.FieldOption.INDEXED);
			Assert.AreEqual(11, fieldNames.Count); // 6 original + the 5 termvector fields 
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "keyword"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "text"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "unstored"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "keyword2"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "text2"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "unstored2"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "tvnot"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "termvector"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "tvposition"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "tvoffset"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "tvpositionoffset"));
			
			// verify that only unindexed fields were returned
			fieldNames = reader.GetFieldNames(IndexReader.FieldOption.UNINDEXED);
			Assert.AreEqual(2, fieldNames.Count); // the following fields
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "unindexed"));
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "unindexed2"));
			
			// verify index term vector fields  
			fieldNames = reader.GetFieldNames(IndexReader.FieldOption.TERMVECTOR);
			Assert.AreEqual(1, fieldNames.Count); // 1 field has term vector only
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "termvector"));
			
			fieldNames = reader.GetFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_POSITION);
			Assert.AreEqual(1, fieldNames.Count); // 4 fields are indexed with term vectors
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "tvposition"));
			
			fieldNames = reader.GetFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_OFFSET);
			Assert.AreEqual(1, fieldNames.Count); // 4 fields are indexed with term vectors
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "tvoffset"));
			
			fieldNames = reader.GetFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_POSITION_OFFSET);
			Assert.AreEqual(1, fieldNames.Count); // 4 fields are indexed with term vectors
			Assert.IsTrue(SupportClass.CollectionsHelper.Contains(fieldNames, "tvpositionoffset"));
			reader.Close();
			d.Close();
		}
		
		[Test]
		public virtual void  TestTermVectors()
		{
			RAMDirectory d = new MockRAMDirectory();
			// set up writer
			IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			// want to get some more segments here
			// new termvector fields
			for (int i = 0; i < 5 * writer.GetMergeFactor(); i++)
			{
				Document doc = new Document();
				doc.Add(new Field("tvnot", "one two two three three three", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
				doc.Add(new Field("termvector", "one two two three three three", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
				doc.Add(new Field("tvoffset", "one two two three three three", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_OFFSETS));
				doc.Add(new Field("tvposition", "one two two three three three", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS));
				doc.Add(new Field("tvpositionoffset", "one two two three three three", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
				
				writer.AddDocument(doc);
			}
			writer.Close();
			IndexReader reader = IndexReader.Open(d);
			FieldSortedTermVectorMapper mapper = new FieldSortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
			reader.GetTermFreqVector(0, mapper);
			System.Collections.IDictionary map = mapper.GetFieldToTerms();
			Assert.IsTrue(map != null, "map is null and it shouldn't be");
			Assert.IsTrue(map.Count == 4, "map Size: " + map.Count + " is not: " + 4);
            System.Collections.Generic.SortedDictionary<object, object> set_Renamed = (System.Collections.Generic.SortedDictionary<object, object>)map["termvector"];
            foreach (System.Collections.Generic.KeyValuePair<object, object> item in set_Renamed)
			{
                TermVectorEntry entry =  (TermVectorEntry)item.Key;
				Assert.IsTrue(entry != null, "entry is null and it shouldn't be");
				System.Console.Out.WriteLine("Entry: " + entry);
			}
		}
		
		private void  AssertTermDocsCount(System.String msg, IndexReader reader, Term term, int expected)
		{
			TermDocs tdocs = null;
			
			try
			{
				tdocs = reader.TermDocs(term);
				Assert.IsNotNull(tdocs, msg + ", null TermDocs");
				int count = 0;
				while (tdocs.Next())
				{
					count++;
				}
				Assert.AreEqual(expected, count, msg + ", count mismatch");
			}
			finally
			{
				if (tdocs != null)
					tdocs.Close();
			}
		}
		
		
		
		[Test]
		public virtual void  TestBasicDelete()
		{
			Directory dir = new MockRAMDirectory();
			
			IndexWriter writer = null;
			IndexReader reader = null;
			Term searchTerm = new Term("content", "aaa");
			
			//  add 100 documents with term : aaa
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer, searchTerm.Text());
			}
			writer.Close();
			
			// OPEN READER AT THIS POINT - this should fix the view of the
			// index at the point of having 100 "aaa" documents and 0 "bbb"
			reader = IndexReader.Open(dir);
			Assert.AreEqual(100, reader.DocFreq(searchTerm), "first docFreq");
			AssertTermDocsCount("first reader", reader, searchTerm, 100);
			reader.Close();
			
			// DELETE DOCUMENTS CONTAINING TERM: aaa
			int deleted = 0;
			reader = IndexReader.Open(dir);
			deleted = reader.DeleteDocuments(searchTerm);
			Assert.AreEqual(100, deleted, "deleted count");
			Assert.AreEqual(100, reader.DocFreq(searchTerm), "deleted docFreq");
			AssertTermDocsCount("deleted termDocs", reader, searchTerm, 0);
			
			// open a 2nd reader to make sure first reader can
			// commit its changes (.del) while second reader
			// is open:
			IndexReader reader2 = IndexReader.Open(dir);
			reader.Close();
			
			// CREATE A NEW READER and re-test
			reader = IndexReader.Open(dir);
			Assert.AreEqual(100, reader.DocFreq(searchTerm), "deleted docFreq");
			AssertTermDocsCount("deleted termDocs", reader, searchTerm, 0);
			reader.Close();
			reader2.Close();
			dir.Close();
		}
		
		[Test]
		public virtual void  TestBinaryFields()
		{
			Directory dir = new RAMDirectory();
			byte[] bin = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
			
			for (int i = 0; i < 10; i++)
			{
				AddDoc(writer, "document number " + (i + 1));
				AddDocumentWithFields(writer);
				AddDocumentWithDifferentFields(writer);
				AddDocumentWithTermVectorFields(writer);
			}
			writer.Close();
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("bin1", bin, Field.Store.YES));
			doc.Add(new Field("bin2", bin, Field.Store.COMPRESS));
			doc.Add(new Field("junk", "junk text", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.Close();
			IndexReader reader = IndexReader.Open(dir);
			doc = reader.Document(reader.MaxDoc() - 1);
			Field[] fields = doc.GetFields("bin1");
			Assert.IsNotNull(fields);
			Assert.AreEqual(1, fields.Length);
			Field b1 = fields[0];
			Assert.IsTrue(b1.IsBinary());
			byte[] data1 = b1.GetBinaryValue();
			Assert.AreEqual(bin.Length, b1.GetBinaryLength());
			for (int i = 0; i < bin.Length; i++)
			{
				Assert.AreEqual(bin[i], data1[i + b1.GetBinaryOffset()]);
			}
			fields = doc.GetFields("bin2");
			Assert.IsNotNull(fields);
			Assert.AreEqual(1, fields.Length);
			b1 = fields[0];
			Assert.IsTrue(b1.IsBinary());
			data1 = b1.GetBinaryValue();
			Assert.AreEqual(bin.Length, b1.GetBinaryLength());
			for (int i = 0; i < bin.Length; i++)
			{
				Assert.AreEqual(bin[i], data1[i + b1.GetBinaryOffset()]);
			}
			System.Collections.Hashtable lazyFields = new System.Collections.Hashtable();
			SupportClass.CollectionsHelper.AddIfNotContains(lazyFields, "bin1");
			FieldSelector sel = new SetBasedFieldSelector(new System.Collections.Hashtable(), lazyFields);
			doc = reader.Document(reader.MaxDoc() - 1, sel);
			Fieldable[] fieldables = doc.GetFieldables("bin1");
			Assert.IsNotNull(fieldables);
			Assert.AreEqual(1, fieldables.Length);
			Fieldable fb1 = fieldables[0];
			Assert.IsTrue(fb1.IsBinary());
			Assert.AreEqual(bin.Length, fb1.GetBinaryLength());
			data1 = fb1.GetBinaryValue();
			Assert.AreEqual(bin.Length, fb1.GetBinaryLength());
			for (int i = 0; i < bin.Length; i++)
			{
				Assert.AreEqual(bin[i], data1[i + fb1.GetBinaryOffset()]);
			}
			reader.Close();
			// force optimize
			
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize();
			writer.Close();
			reader = IndexReader.Open(dir);
			doc = reader.Document(reader.MaxDoc() - 1);
			fields = doc.GetFields("bin1");
			Assert.IsNotNull(fields);
			Assert.AreEqual(1, fields.Length);
			b1 = fields[0];
			Assert.IsTrue(b1.IsBinary());
			data1 = b1.GetBinaryValue();
			Assert.AreEqual(bin.Length, b1.GetBinaryLength());
			for (int i = 0; i < bin.Length; i++)
			{
				Assert.AreEqual(bin[i], data1[i + b1.GetBinaryOffset()]);
			}
			fields = doc.GetFields("bin2");
			Assert.IsNotNull(fields);
			Assert.AreEqual(1, fields.Length);
			b1 = fields[0];
			Assert.IsTrue(b1.IsBinary());
			data1 = b1.GetBinaryValue();
			Assert.AreEqual(bin.Length, b1.GetBinaryLength());
			for (int i = 0; i < bin.Length; i++)
			{
				Assert.AreEqual(bin[i], data1[i + b1.GetBinaryOffset()]);
			}
			reader.Close();
		}
		
		// Make sure attempts to make changes after reader is
		// closed throws IOException:
		[Test]
		public virtual void  TestChangesAfterClose()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = null;
			IndexReader reader = null;
			Term searchTerm = new Term("content", "aaa");
			
			//  add 11 documents with term : aaa
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 11; i++)
			{
				AddDoc(writer, searchTerm.Text());
			}
			writer.Close();
			
			reader = IndexReader.Open(dir);
			
			// Close reader:
			reader.Close();
			
			// Then, try to make changes:
			try
			{
				reader.DeleteDocument(4);
				Assert.Fail("deleteDocument after close failed to throw IOException");
			}
			catch (AlreadyClosedException e)
			{
				// expected
			}
			
			try
			{
				reader.SetNorm(5, "aaa", 2.0f);
				Assert.Fail("setNorm after close failed to throw IOException");
			}
			catch (AlreadyClosedException e)
			{
				// expected
			}
			
			try
			{
				reader.UndeleteAll();
				Assert.Fail("undeleteAll after close failed to throw IOException");
			}
			catch (AlreadyClosedException e)
			{
				// expected
			}
		}
		
		// Make sure we get lock obtain failed exception with 2 writers:
		[Test]
		public virtual void  TestLockObtainFailed()
		{
			Directory dir = new RAMDirectory();
			
			IndexWriter writer = null;
			IndexReader reader = null;
			Term searchTerm = new Term("content", "aaa");
			
			//  add 11 documents with term : aaa
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 11; i++)
			{
				AddDoc(writer, searchTerm.Text());
			}
			
			// Create reader:
			reader = IndexReader.Open(dir);
			
			// Try to make changes
			try
			{
				reader.DeleteDocument(4);
				Assert.Fail("deleteDocument should have hit LockObtainFailedException");
			}
			catch (LockObtainFailedException e)
			{
				// expected
			}
			
			try
			{
				reader.SetNorm(5, "aaa", 2.0f);
				Assert.Fail("setNorm should have hit LockObtainFailedException");
			}
			catch (LockObtainFailedException e)
			{
				// expected
			}
			
			try
			{
				reader.UndeleteAll();
				Assert.Fail("undeleteAll should have hit LockObtainFailedException");
			}
			catch (LockObtainFailedException e)
			{
				// expected
			}
			writer.Close();
			reader.Close();
		}
		
		// Make sure you can set norms & commit even if a reader
		// is open against the index:
		[Test]
		public virtual void  TestWritingNorms()
		{
			System.String tempDir = SupportClass.AppSettings.Get("tempDir", "");
			if (tempDir == null)
				throw new System.IO.IOException("tempDir undefined, cannot run test");
			
			System.IO.FileInfo indexDir = new System.IO.FileInfo(System.IO.Path.Combine(tempDir, "lucenetestnormwriter"));
			Directory dir = FSDirectory.Open(indexDir);
			IndexWriter writer;
			IndexReader reader;
			Term searchTerm = new Term("content", "aaa");
			
			//  add 1 documents with term : aaa
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDoc(writer, searchTerm.Text());
			writer.Close();
			
			//  now open reader & set norm for doc 0
			reader = IndexReader.Open(dir);
			reader.SetNorm(0, "content", (float) 2.0);
			
			// we should be holding the write lock now:
			Assert.IsTrue(IndexReader.IsLocked(dir), "locked");
			
			reader.Commit();
			
			// we should not be holding the write lock now:
			Assert.IsTrue(!IndexReader.IsLocked(dir), "not locked");
			
			// open a 2nd reader:
			IndexReader reader2 = IndexReader.Open(dir);
			
			// set norm again for doc 0
			reader.SetNorm(0, "content", (float) 3.0);
			Assert.IsTrue(IndexReader.IsLocked(dir), "locked");
			
			reader.Close();
			
			// we should not be holding the write lock now:
			Assert.IsTrue(!IndexReader.IsLocked(dir), "not locked");
			
			reader2.Close();
			dir.Close();
			
			RmDir(indexDir);
		}
		
		
		// Make sure you can set norms & commit, and there are
		// no extra norms files left:
		[Test]
		public virtual void  TestWritingNormsNoReader()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = null;
			IndexReader reader = null;
			Term searchTerm = new Term("content", "aaa");
			
			//  add 1 documents with term : aaa
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetUseCompoundFile(false);
			AddDoc(writer, searchTerm.Text());
			writer.Close();
			
			//  now open reader & set norm for doc 0 (writes to
			//  _0_1.s0)
			reader = IndexReader.Open(dir);
			reader.SetNorm(0, "content", (float) 2.0);
			reader.Close();
			
			//  now open reader again & set norm for doc 0 (writes to _0_2.s0)
			reader = IndexReader.Open(dir);
			reader.SetNorm(0, "content", (float) 2.0);
			reader.Close();
			Assert.IsFalse(dir.FileExists("_0_1.s0"), "failed to remove first generation norms file on writing second generation");
			
			dir.Close();
		}
		
		
		[Test]
		public virtual void  TestDeleteReaderWriterConflictUnoptimized()
		{
			DeleteReaderWriterConflict(false);
		}
		
		[Test]
		public virtual void  TestOpenEmptyDirectory()
		{
			System.String dirName = "test.empty";
			System.IO.FileInfo fileDirName = new System.IO.FileInfo(dirName);
			bool tmpBool;
			if (System.IO.File.Exists(fileDirName.FullName))
				tmpBool = true;
			else
				tmpBool = System.IO.Directory.Exists(fileDirName.FullName);
			if (!tmpBool)
			{
				System.IO.Directory.CreateDirectory(fileDirName.FullName);
			}
			try
			{
				IndexReader.Open(fileDirName);
				Assert.Fail("opening IndexReader on empty directory failed to produce FileNotFoundException");
			}
			catch (System.IO.FileNotFoundException e)
			{
				// GOOD
			}
			RmDir(fileDirName);
		}
		
		[Test]
		public virtual void  TestDeleteReaderWriterConflictOptimized()
		{
			DeleteReaderWriterConflict(true);
		}
		
		private void  DeleteReaderWriterConflict(bool optimize)
		{
			//Directory dir = new RAMDirectory();
			Directory dir = GetDirectory();
			
			Term searchTerm = new Term("content", "aaa");
			Term searchTerm2 = new Term("content", "bbb");
			
			//  add 100 documents with term : aaa
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer, searchTerm.Text());
			}
			writer.Close();
			
			// OPEN READER AT THIS POINT - this should fix the view of the
			// index at the point of having 100 "aaa" documents and 0 "bbb"
			IndexReader reader = IndexReader.Open(dir);
			Assert.AreEqual(100, reader.DocFreq(searchTerm), "first docFreq");
			Assert.AreEqual(0, reader.DocFreq(searchTerm2), "first docFreq");
			AssertTermDocsCount("first reader", reader, searchTerm, 100);
			AssertTermDocsCount("first reader", reader, searchTerm2, 0);
			
			// add 100 documents with term : bbb
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer, searchTerm2.Text());
			}
			
			// REQUEST OPTIMIZATION
			// This causes a new segment to become current for all subsequent
			// searchers. Because of this, deletions made via a previously open
			// reader, which would be applied to that reader's segment, are lost
			// for subsequent searchers/readers
			if (optimize)
				writer.Optimize();
			writer.Close();
			
			// The reader should not see the new data
			Assert.AreEqual(100, reader.DocFreq(searchTerm), "first docFreq");
			Assert.AreEqual(0, reader.DocFreq(searchTerm2), "first docFreq");
			AssertTermDocsCount("first reader", reader, searchTerm, 100);
			AssertTermDocsCount("first reader", reader, searchTerm2, 0);
			
			
			// DELETE DOCUMENTS CONTAINING TERM: aaa
			// NOTE: the reader was created when only "aaa" documents were in
			int deleted = 0;
			try
			{
				deleted = reader.DeleteDocuments(searchTerm);
				Assert.Fail("Delete allowed on an index reader with stale segment information");
			}
			catch (StaleReaderException e)
			{
				/* success */
			}
			
			// Re-open index reader and try again. This time it should see
			// the new data.
			reader.Close();
			reader = IndexReader.Open(dir);
			Assert.AreEqual(100, reader.DocFreq(searchTerm), "first docFreq");
			Assert.AreEqual(100, reader.DocFreq(searchTerm2), "first docFreq");
			AssertTermDocsCount("first reader", reader, searchTerm, 100);
			AssertTermDocsCount("first reader", reader, searchTerm2, 100);
			
			deleted = reader.DeleteDocuments(searchTerm);
			Assert.AreEqual(100, deleted, "deleted count");
			Assert.AreEqual(100, reader.DocFreq(searchTerm), "deleted docFreq");
			Assert.AreEqual(100, reader.DocFreq(searchTerm2), "deleted docFreq");
			AssertTermDocsCount("deleted termDocs", reader, searchTerm, 0);
			AssertTermDocsCount("deleted termDocs", reader, searchTerm2, 100);
			reader.Close();
			
			// CREATE A NEW READER and re-test
			reader = IndexReader.Open(dir);
			Assert.AreEqual(100, reader.DocFreq(searchTerm), "deleted docFreq");
			Assert.AreEqual(100, reader.DocFreq(searchTerm2), "deleted docFreq");
			AssertTermDocsCount("deleted termDocs", reader, searchTerm, 0);
			AssertTermDocsCount("deleted termDocs", reader, searchTerm2, 100);
			reader.Close();
		}
		
		private Directory GetDirectory()
		{
			return FSDirectory.Open(new System.IO.FileInfo(System.IO.Path.Combine(SupportClass.AppSettings.Get("tempDir", ""), "testIndex")));
		}
		
		[Test]
		public virtual void  TestFilesOpenClose()
		{
			// Create initial data set
			System.IO.FileInfo dirFile = new System.IO.FileInfo(System.IO.Path.Combine(SupportClass.AppSettings.Get("tempDir", ""), "testIndex"));
			Directory dir = GetDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDoc(writer, "test");
			writer.Close();
			dir.Close();
			
			// Try to erase the data - this ensures that the writer closed all files
			_TestUtil.RmDir(dirFile);
			dir = GetDirectory();
			
			// Now create the data set again, just as before
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDoc(writer, "test");
			writer.Close();
			dir.Close();
			
			// Now open existing directory and test that reader closes all files
			dir = GetDirectory();
			IndexReader reader1 = IndexReader.Open(dir);
			reader1.Close();
			dir.Close();
			
			// The following will fail if reader did not close
			// all files
			_TestUtil.RmDir(dirFile);
		}
		
		[Test]
		public virtual void  TestLastModified()
		{
			Assert.IsFalse(IndexReader.IndexExists("there_is_no_such_index"));
			System.IO.FileInfo fileDir = new System.IO.FileInfo(System.IO.Path.Combine(SupportClass.AppSettings.Get("tempDir", ""), "testIndex"));
			for (int i = 0; i < 2; i++)
			{
				try
				{
					Directory dir;
					if (0 == i)
						dir = new MockRAMDirectory();
					else
						dir = GetDirectory();
					Assert.IsFalse(IndexReader.IndexExists(dir));
					IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
					AddDocumentWithFields(writer);
					Assert.IsTrue(IndexReader.IsLocked(dir)); // writer open, so dir is locked
					writer.Close();
					Assert.IsTrue(IndexReader.IndexExists(dir));
					IndexReader reader = IndexReader.Open(dir);
					Assert.IsFalse(IndexReader.IsLocked(dir)); // reader only, no lock
					long version = IndexReader.LastModified(dir);
					if (i == 1)
					{
						long version2 = IndexReader.LastModified(fileDir);
						Assert.AreEqual(version, version2);
					}
					reader.Close();
					// modify index and check version has been
					// incremented:
					System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 1000));
					
					writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
					AddDocumentWithFields(writer);
					writer.Close();
					reader = IndexReader.Open(dir);
					Assert.IsTrue(version <= IndexReader.LastModified(dir), "old lastModified is " + version + "; new lastModified is " + IndexReader.LastModified(dir));
					reader.Close();
					dir.Close();
				}
				finally
				{
					if (i == 1)
						_TestUtil.RmDir(fileDir);
				}
			}
		}
		
		[Test]
		public virtual void  TestVersion()
		{
			Assert.IsFalse(IndexReader.IndexExists("there_is_no_such_index"));
			Directory dir = new MockRAMDirectory();
			Assert.IsFalse(IndexReader.IndexExists(dir));
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			Assert.IsTrue(IndexReader.IsLocked(dir)); // writer open, so dir is locked
			writer.Close();
			Assert.IsTrue(IndexReader.IndexExists(dir));
			IndexReader reader = IndexReader.Open(dir);
			Assert.IsFalse(IndexReader.IsLocked(dir)); // reader only, no lock
			long version = IndexReader.GetCurrentVersion(dir);
			reader.Close();
			// modify index and check version has been
			// incremented:
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			writer.Close();
			reader = IndexReader.Open(dir);
			Assert.IsTrue(version < IndexReader.GetCurrentVersion(dir), "old version is " + version + "; new version is " + IndexReader.GetCurrentVersion(dir));
			reader.Close();
			dir.Close();
		}
		
		[Test]
		public virtual void  TestLock()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			writer.Close();
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			IndexReader reader = IndexReader.Open(dir);
			try
			{
				reader.DeleteDocument(0);
				Assert.Fail("expected lock");
			}
			catch (System.IO.IOException e)
			{
				// expected exception
			}
			IndexReader.Unlock(dir); // this should not be done in the real world! 
			reader.DeleteDocument(0);
			reader.Close();
			writer.Close();
			dir.Close();
		}
		
		[Test]
		public virtual void  TestUndeleteAll()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			AddDocumentWithFields(writer);
			writer.Close();
			IndexReader reader = IndexReader.Open(dir);
			reader.DeleteDocument(0);
			reader.DeleteDocument(1);
			reader.UndeleteAll();
			reader.Close();
			reader = IndexReader.Open(dir);
			Assert.AreEqual(2, reader.NumDocs()); // nothing has really been deleted thanks to undeleteAll()
			reader.Close();
			dir.Close();
		}
		
		[Test]
		public virtual void  TestUndeleteAllAfterClose()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			AddDocumentWithFields(writer);
			writer.Close();
			IndexReader reader = IndexReader.Open(dir);
			reader.DeleteDocument(0);
			reader.DeleteDocument(1);
			reader.Close();
			reader = IndexReader.Open(dir);
			reader.UndeleteAll();
			Assert.AreEqual(2, reader.NumDocs()); // nothing has really been deleted thanks to undeleteAll()
			reader.Close();
			dir.Close();
		}
		
		[Test]
		public virtual void  TestUndeleteAllAfterCloseThenReopen()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			AddDocumentWithFields(writer);
			writer.Close();
			IndexReader reader = IndexReader.Open(dir);
			reader.DeleteDocument(0);
			reader.DeleteDocument(1);
			reader.Close();
			reader = IndexReader.Open(dir);
			reader.UndeleteAll();
			reader.Close();
			reader = IndexReader.Open(dir);
			Assert.AreEqual(2, reader.NumDocs()); // nothing has really been deleted thanks to undeleteAll()
			reader.Close();
			dir.Close();
		}
		
		[Test]
		public virtual void  TestDeleteReaderReaderConflictUnoptimized()
		{
			DeleteReaderReaderConflict(false);
		}
		
		[Test]
		public virtual void  TestDeleteReaderReaderConflictOptimized()
		{
			DeleteReaderReaderConflict(true);
		}
		
		/// <summary> Make sure if reader tries to commit but hits disk
		/// full that reader remains consistent and usable.
		/// </summary>
		[Test]
		public virtual void  TestDiskFull()
		{
			
			bool debug = false;
			Term searchTerm = new Term("content", "aaa");
			int START_COUNT = 157;
			int END_COUNT = 144;
			
			// First build up a starting index:
			RAMDirectory startDir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(startDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 157; i++)
			{
				Document d = new Document();
				d.Add(new Field("id", System.Convert.ToString(i), Field.Store.YES, Field.Index.NOT_ANALYZED));
				d.Add(new Field("content", "aaa " + i, Field.Store.NO, Field.Index.ANALYZED));
				writer.AddDocument(d);
			}
			writer.Close();
			
			long diskUsage = startDir.SizeInBytes();
			long diskFree = diskUsage + 100;
			
			System.IO.IOException err = null;
			
			bool done = false;
			
			// Iterate w/ ever increasing free disk space:
			while (!done)
			{
				MockRAMDirectory dir = new MockRAMDirectory(startDir);
				
				// If IndexReader hits disk full, it can write to
				// the same files again.
				dir.SetPreventDoubleWrite(false);
				
				IndexReader reader = IndexReader.Open(dir);
				
				// For each disk size, first try to commit against
				// dir that will hit random IOExceptions & disk
				// full; after, give it infinite disk space & turn
				// off random IOExceptions & retry w/ same reader:
				bool success = false;
				
				for (int x = 0; x < 2; x++)
				{
					
					double rate = 0.05;
					double diskRatio = ((double) diskFree) / diskUsage;
					long thisDiskFree;
					System.String testName;
					
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
						{
							System.Console.Out.WriteLine("\ncycle: " + diskFree + " bytes");
						}
						testName = "disk full during reader.close() @ " + thisDiskFree + " bytes";
					}
					else
					{
						thisDiskFree = 0;
						rate = 0.0;
						if (debug)
						{
							System.Console.Out.WriteLine("\ncycle: same writer: unlimited disk space");
						}
						testName = "reader re-use after disk full";
					}
					
					dir.SetMaxSizeInBytes(thisDiskFree);
					dir.SetRandomIOExceptionRate(rate, diskFree);
					
					try
					{
						if (0 == x)
						{
							int docId = 12;
							for (int i = 0; i < 13; i++)
							{
								reader.DeleteDocument(docId);
								reader.SetNorm(docId, "contents", (float) 2.0);
								docId += 12;
							}
						}
						reader.Close();
						success = true;
						if (0 == x)
						{
							done = true;
						}
					}
					catch (System.IO.IOException e)
					{
						if (debug)
						{
							System.Console.Out.WriteLine("  hit IOException: " + e);
							System.Console.Out.WriteLine(e.StackTrace);
						}
						err = e;
						if (1 == x)
						{
							System.Console.Error.WriteLine(e.StackTrace);
							Assert.Fail(testName + " hit IOException after disk space was freed up");
						}
					}
					
					// Whether we succeeded or failed, check that all
					// un-referenced files were in fact deleted (ie,
					// we did not create garbage).  Just create a
					// new IndexFileDeleter, have it delete
					// unreferenced files, then verify that in fact
					// no files were deleted:
					System.String[] startFiles = dir.ListAll();
					SegmentInfos infos = new SegmentInfos();
					infos.Read(dir);
					new IndexFileDeleter(dir, new KeepOnlyLastCommitDeletionPolicy(), infos, null, null);
					System.String[] endFiles = dir.ListAll();
					
					System.Array.Sort(startFiles);
					System.Array.Sort(endFiles);
					
					//for(int i=0;i<startFiles.length;i++) {
					//  System.out.println("  startFiles: " + i + ": " + startFiles[i]);
					//}
					
					if (!SupportClass.CollectionsHelper.Equals(startFiles, endFiles))
					{
						System.String successStr;
						if (success)
						{
							successStr = "success";
						}
						else
						{
							successStr = "IOException";
							System.Console.Error.WriteLine(err.StackTrace);
						}
						Assert.Fail("reader.close() failed to delete unreferenced files after " + successStr + " (" + diskFree + " bytes): before delete:\n    " + ArrayToString(startFiles) + "\n  after delete:\n    " + ArrayToString(endFiles));
					}
					
					// Finally, verify index is not corrupt, and, if
					// we succeeded, we see all docs changed, and if
					// we failed, we see either all docs or no docs
					// changed (transactional semantics):
					IndexReader newReader = null;
					try
					{
						newReader = IndexReader.Open(dir);
					}
					catch (System.IO.IOException e)
					{
						System.Console.Error.WriteLine(e.StackTrace);
						Assert.Fail(testName + ":exception when creating IndexReader after disk full during close: " + e);
					}
					/*
					int result = newReader.docFreq(searchTerm);
					if (success) {
					if (result != END_COUNT) {
					fail(testName + ": method did not throw exception but docFreq('aaa') is " + result + " instead of expected " + END_COUNT);
					}
					} else {
					// On hitting exception we still may have added
					// all docs:
					if (result != START_COUNT && result != END_COUNT) {
					err.printStackTrace();
					fail(testName + ": method did throw exception but docFreq('aaa') is " + result + " instead of expected " + START_COUNT + " or " + END_COUNT);
					}
					}
					*/
					
					IndexSearcher searcher = new IndexSearcher(newReader);
					ScoreDoc[] hits = null;
					try
					{
						hits = searcher.Search(new TermQuery(searchTerm), null, 1000).scoreDocs;
					}
					catch (System.IO.IOException e)
					{
						System.Console.Error.WriteLine(e.StackTrace);
						Assert.Fail(testName + ": exception when searching: " + e);
					}
					int result2 = hits.Length;
					if (success)
					{
						if (result2 != END_COUNT)
						{
							Assert.Fail(testName + ": method did not throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + END_COUNT);
						}
					}
					else
					{
						// On hitting exception we still may have added
						// all docs:
						if (result2 != START_COUNT && result2 != END_COUNT)
						{
							System.Console.Error.WriteLine(err.StackTrace);
							Assert.Fail(testName + ": method did throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + START_COUNT);
						}
					}
					
					searcher.Close();
					newReader.Close();
					
					if (result2 == END_COUNT)
					{
						break;
					}
				}
				
				dir.Close();
				
				// Try again with 10 more bytes of free space:
				diskFree += 10;
			}
			
			startDir.Close();
		}
		
		[Test]
		public virtual void  TestDocsOutOfOrderJIRA140()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 11; i++)
			{
				AddDoc(writer, "aaa");
			}
			writer.Close();
			IndexReader reader = IndexReader.Open(dir);
			
			// Try to delete an invalid docId, yet, within range
			// of the final bits of the BitVector:
			
			bool gotException = false;
			try
			{
				reader.DeleteDocument(11);
			}
			catch (System.IndexOutOfRangeException e)
			{
				gotException = true;
			}
			reader.Close();
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			
			// We must add more docs to get a new segment written
			for (int i = 0; i < 11; i++)
			{
				AddDoc(writer, "aaa");
			}
			
			// Without the fix for LUCENE-140 this call will
			// [incorrectly] hit a "docs out of order"
			// IllegalStateException because above out-of-bounds
			// deleteDocument corrupted the index:
			writer.Optimize();
			writer.Close();
			if (!gotException)
			{
				Assert.Fail("delete of out-of-bounds doc number failed to hit exception");
			}
			dir.Close();
		}
		
		[Test]
		public virtual void  TestExceptionReleaseWriteLockJIRA768()
		{
			
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDoc(writer, "aaa");
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			try
			{
				reader.DeleteDocument(1);
				Assert.Fail("did not hit exception when deleting an invalid doc number");
			}
			catch (System.IndexOutOfRangeException e)
			{
				// expected
			}
			reader.Close();
			if (IndexReader.IsLocked(dir))
			{
				Assert.Fail("write lock is still held after close");
			}
			
			reader = IndexReader.Open(dir);
			try
			{
				reader.SetNorm(1, "content", (float) 2.0);
				Assert.Fail("did not hit exception when calling setNorm on an invalid doc number");
			}
			catch (System.IndexOutOfRangeException e)
			{
				// expected
			}
			reader.Close();
			if (IndexReader.IsLocked(dir))
			{
				Assert.Fail("write lock is still held after close");
			}
			dir.Close();
		}
		
		private System.String ArrayToString(System.String[] l)
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
		
		[Test]
		public virtual void  TestOpenReaderAfterDelete()
		{
			System.IO.FileInfo dirFile = new System.IO.FileInfo(System.IO.Path.Combine(SupportClass.AppSettings.Get("tempDir", ""), "deletetest"));
			Directory dir = FSDirectory.Open(dirFile);
			try
			{
				IndexReader.Open(dir);
				Assert.Fail("expected FileNotFoundException");
			}
			catch (System.IO.FileNotFoundException e)
			{
				// expected
			}
			
			bool tmpBool;
			if (System.IO.File.Exists(dirFile.FullName))
			{
				System.IO.File.Delete(dirFile.FullName);
				tmpBool = true;
			}
			else if (System.IO.Directory.Exists(dirFile.FullName))
			{
				System.IO.Directory.Delete(dirFile.FullName);
				tmpBool = true;
			}
			else
				tmpBool = false;
			bool generatedAux = tmpBool;
			
			// Make sure we still get a CorruptIndexException (not NPE):
			try
			{
				IndexReader.Open(dir);
				Assert.Fail("expected FileNotFoundException");
			}
			catch (System.IO.FileNotFoundException e)
			{
				// expected
			}
			
			dir.Close();
		}
		
		private void  DeleteReaderReaderConflict(bool optimize)
		{
			Directory dir = GetDirectory();
			
			Term searchTerm1 = new Term("content", "aaa");
			Term searchTerm2 = new Term("content", "bbb");
			Term searchTerm3 = new Term("content", "ccc");
			
			//  add 100 documents with term : aaa
			//  add 100 documents with term : bbb
			//  add 100 documents with term : ccc
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer, searchTerm1.Text());
				AddDoc(writer, searchTerm2.Text());
				AddDoc(writer, searchTerm3.Text());
			}
			if (optimize)
				writer.Optimize();
			writer.Close();
			
			// OPEN TWO READERS
			// Both readers get segment info as exists at this time
			IndexReader reader1 = IndexReader.Open(dir);
			Assert.AreEqual(100, reader1.DocFreq(searchTerm1), "first opened");
			Assert.AreEqual(100, reader1.DocFreq(searchTerm2), "first opened");
			Assert.AreEqual(100, reader1.DocFreq(searchTerm3), "first opened");
			AssertTermDocsCount("first opened", reader1, searchTerm1, 100);
			AssertTermDocsCount("first opened", reader1, searchTerm2, 100);
			AssertTermDocsCount("first opened", reader1, searchTerm3, 100);
			
			IndexReader reader2 = IndexReader.Open(dir);
			Assert.AreEqual(100, reader2.DocFreq(searchTerm1), "first opened");
			Assert.AreEqual(100, reader2.DocFreq(searchTerm2), "first opened");
			Assert.AreEqual(100, reader2.DocFreq(searchTerm3), "first opened");
			AssertTermDocsCount("first opened", reader2, searchTerm1, 100);
			AssertTermDocsCount("first opened", reader2, searchTerm2, 100);
			AssertTermDocsCount("first opened", reader2, searchTerm3, 100);
			
			// DELETE DOCS FROM READER 2 and CLOSE IT
			// delete documents containing term: aaa
			// when the reader is closed, the segment info is updated and
			// the first reader is now stale
			reader2.DeleteDocuments(searchTerm1);
			Assert.AreEqual(100, reader2.DocFreq(searchTerm1), "after delete 1");
			Assert.AreEqual(100, reader2.DocFreq(searchTerm2), "after delete 1");
			Assert.AreEqual(100, reader2.DocFreq(searchTerm3), "after delete 1");
			AssertTermDocsCount("after delete 1", reader2, searchTerm1, 0);
			AssertTermDocsCount("after delete 1", reader2, searchTerm2, 100);
			AssertTermDocsCount("after delete 1", reader2, searchTerm3, 100);
			reader2.Close();
			
			// Make sure reader 1 is unchanged since it was open earlier
			Assert.AreEqual(100, reader1.DocFreq(searchTerm1), "after delete 1");
			Assert.AreEqual(100, reader1.DocFreq(searchTerm2), "after delete 1");
			Assert.AreEqual(100, reader1.DocFreq(searchTerm3), "after delete 1");
			AssertTermDocsCount("after delete 1", reader1, searchTerm1, 100);
			AssertTermDocsCount("after delete 1", reader1, searchTerm2, 100);
			AssertTermDocsCount("after delete 1", reader1, searchTerm3, 100);
			
			
			// ATTEMPT TO DELETE FROM STALE READER
			// delete documents containing term: bbb
			try
			{
				reader1.DeleteDocuments(searchTerm2);
				Assert.Fail("Delete allowed from a stale index reader");
			}
			catch (System.IO.IOException e)
			{
				/* success */
			}
			
			// RECREATE READER AND TRY AGAIN
			reader1.Close();
			reader1 = IndexReader.Open(dir);
			Assert.AreEqual(100, reader1.DocFreq(searchTerm1), "reopened");
			Assert.AreEqual(100, reader1.DocFreq(searchTerm2), "reopened");
			Assert.AreEqual(100, reader1.DocFreq(searchTerm3), "reopened");
			AssertTermDocsCount("reopened", reader1, searchTerm1, 0);
			AssertTermDocsCount("reopened", reader1, searchTerm2, 100);
			AssertTermDocsCount("reopened", reader1, searchTerm3, 100);
			
			reader1.DeleteDocuments(searchTerm2);
			Assert.AreEqual(100, reader1.DocFreq(searchTerm1), "deleted 2");
			Assert.AreEqual(100, reader1.DocFreq(searchTerm2), "deleted 2");
			Assert.AreEqual(100, reader1.DocFreq(searchTerm3), "deleted 2");
			AssertTermDocsCount("deleted 2", reader1, searchTerm1, 0);
			AssertTermDocsCount("deleted 2", reader1, searchTerm2, 0);
			AssertTermDocsCount("deleted 2", reader1, searchTerm3, 100);
			reader1.Close();
			
			// Open another reader to confirm that everything is deleted
			reader2 = IndexReader.Open(dir);
			Assert.AreEqual(100, reader2.DocFreq(searchTerm1), "reopened 2");
			Assert.AreEqual(100, reader2.DocFreq(searchTerm2), "reopened 2");
			Assert.AreEqual(100, reader2.DocFreq(searchTerm3), "reopened 2");
			AssertTermDocsCount("reopened 2", reader2, searchTerm1, 0);
			AssertTermDocsCount("reopened 2", reader2, searchTerm2, 0);
			AssertTermDocsCount("reopened 2", reader2, searchTerm3, 100);
			reader2.Close();
			
			dir.Close();
		}
		
		
		private void  AddDocumentWithFields(IndexWriter writer)
		{
			Document doc = new Document();
			doc.Add(new Field("keyword", "test1", Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.Add(new Field("text", "test1", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("unindexed", "test1", Field.Store.YES, Field.Index.NO));
			doc.Add(new Field("unstored", "test1", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
		}
		
		private void  AddDocumentWithDifferentFields(IndexWriter writer)
		{
			Document doc = new Document();
			doc.Add(new Field("keyword2", "test1", Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.Add(new Field("text2", "test1", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("unindexed2", "test1", Field.Store.YES, Field.Index.NO));
			doc.Add(new Field("unstored2", "test1", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
		}
		
		private void  AddDocumentWithTermVectorFields(IndexWriter writer)
		{
			Document doc = new Document();
			doc.Add(new Field("tvnot", "tvnot", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
			doc.Add(new Field("termvector", "termvector", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
			doc.Add(new Field("tvoffset", "tvoffset", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_OFFSETS));
			doc.Add(new Field("tvposition", "tvposition", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS));
			doc.Add(new Field("tvpositionoffset", "tvpositionoffset", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			
			writer.AddDocument(doc);
		}
		
		private void  AddDoc(IndexWriter writer, System.String value_Renamed)
		{
			Document doc = new Document();
			doc.Add(new Field("content", value_Renamed, Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
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
		
		public static void  AssertIndexEquals(IndexReader index1, IndexReader index2)
		{
			Assert.AreEqual(index1.NumDocs(), index2.NumDocs(), "IndexReaders have different values for numDocs.");
			Assert.AreEqual(index1.MaxDoc(), index2.MaxDoc(), "IndexReaders have different values for maxDoc.");
			Assert.AreEqual(index1.HasDeletions(), index2.HasDeletions(), "Only one IndexReader has deletions.");
			Assert.AreEqual(index1.IsOptimized(), index2.IsOptimized(), "Only one index is optimized.");
			
			// check field names
			System.Collections.Generic.ICollection<string> fieldsNames1 = index1.GetFieldNames(FieldOption.ALL);
			System.Collections.Generic.ICollection<string> fieldsNames2 = index1.GetFieldNames(FieldOption.ALL);

            System.Collections.ICollection fields1 = null;
            System.Collections.ICollection fields2 = null;

            Assert.AreEqual(fieldsNames1.Count, fieldsNames2.Count, "IndexReaders have different numbers of fields.");
            System.Collections.IEnumerator it1 = fieldsNames1.GetEnumerator();
            System.Collections.IEnumerator it2 = fieldsNames2.GetEnumerator();
			while (it1.MoveNext() && it2.MoveNext())
			{
				Assert.AreEqual((System.String) it1.Current, (System.String) it2.Current, "Different field names.");
			}
			
			// check norms
            it1 = fieldsNames1.GetEnumerator();
			while (it1.MoveNext())
			{
				System.String curField = (System.String) it1.Current;
				byte[] norms1 = index1.Norms(curField);
				byte[] norms2 = index2.Norms(curField);
				if (norms1 != null && norms2 != null)
				{
					Assert.AreEqual(norms1.Length, norms2.Length);
					for (int i = 0; i < norms1.Length; i++)
					{
						Assert.AreEqual(norms1[i], norms2[i], "Norm different for doc " + i + " and field '" + curField + "'.");
					}
				}
				else
				{
					Assert.AreSame(norms1, norms2);
				}
			}
			
			// check deletions
			for (int i = 0; i < index1.MaxDoc(); i++)
			{
				Assert.AreEqual(index1.IsDeleted(i), index2.IsDeleted(i), "Doc " + i + " only deleted in one index.");
			}
			
			// check stored fields
			for (int i = 0; i < index1.MaxDoc(); i++)
			{
				if (!index1.IsDeleted(i))
				{
					Document doc1 = index1.Document(i);
					Document doc2 = index2.Document(i);
					fields1 = doc1.GetFields();
					fields2 = doc2.GetFields();
					Assert.AreEqual(fields1.Count, fields2.Count, "Different numbers of fields for doc " + i + ".");
					it1 = fields1.GetEnumerator();
					it2 = fields2.GetEnumerator();
					while (it1.MoveNext() && it2.MoveNext())
					{
						Field curField1 = (Field) it1.Current;
						Field curField2 = (Field) it2.Current;
						Assert.AreEqual(curField1.Name(), curField2.Name(), "Different fields names for doc " + i + ".");
						Assert.AreEqual(curField1.StringValue(), curField2.StringValue(), "Different field values for doc " + i + ".");
					}
				}
			}
			
			// check dictionary and posting lists
			TermEnum enum1 = index1.Terms();
			TermEnum enum2 = index2.Terms();
			TermPositions tp1 = index1.TermPositions();
			TermPositions tp2 = index2.TermPositions();
			while (enum1.Next())
			{
				Assert.IsTrue(enum2.Next());
				Assert.AreEqual(enum1.Term(), enum2.Term(), "Different term in dictionary.");
				tp1.Seek(enum1.Term());
				tp2.Seek(enum1.Term());
				while (tp1.Next())
				{
					Assert.IsTrue(tp2.Next());
					Assert.AreEqual(tp1.Doc(), tp2.Doc(), "Different doc id in postinglist of term " + enum1.Term() + ".");
					Assert.AreEqual(tp1.Freq(), tp2.Freq(), "Different term frequence in postinglist of term " + enum1.Term() + ".");
					for (int i = 0; i < tp1.Freq(); i++)
					{
						Assert.AreEqual(tp1.NextPosition(), tp2.NextPosition(), "Different positions in postinglist of term " + enum1.Term() + ".");
					}
				}
			}
		}
		
		[Test]
		public virtual void  TestGetIndexCommit()
		{
			
			RAMDirectory d = new MockRAMDirectory();
			
			// set up writer
			IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			for (int i = 0; i < 27; i++)
				AddDocumentWithFields(writer);
			writer.Close();
			
			SegmentInfos sis = new SegmentInfos();
			sis.Read(d);
			IndexReader r = IndexReader.Open(d);
			IndexCommit c = r.GetIndexCommit();
			
			Assert.AreEqual(sis.GetCurrentSegmentFileName(), c.GetSegmentsFileName());
			
			Assert.IsTrue(c.Equals(r.GetIndexCommit()));
			
			// Change the index
			writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			for (int i = 0; i < 7; i++)
				AddDocumentWithFields(writer);
			writer.Close();
			
			IndexReader r2 = r.Reopen();
			Assert.IsFalse(c.Equals(r2.GetIndexCommit()));
			Assert.IsFalse(r2.GetIndexCommit().IsOptimized());
			r2.Close();
			
			writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize();
			writer.Close();
			
			r2 = r.Reopen();
			Assert.IsTrue(r2.GetIndexCommit().IsOptimized());
			
			r.Close();
			r2.Close();
			d.Close();
		}
		
		[Test]
		public virtual void  TestReadOnly()
		{
			RAMDirectory d = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			writer.Commit();
			AddDocumentWithFields(writer);
			writer.Close();
			
			IndexReader r = IndexReader.Open(d, true);
			try
			{
				r.DeleteDocument(0);
				Assert.Fail();
			}
			catch (System.NotSupportedException uoe)
			{
				// expected
			}
			
			writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			AddDocumentWithFields(writer);
			writer.Close();
			
			// Make sure reopen is still readonly:
			IndexReader r2 = r.Reopen();
			r.Close();
			
			Assert.IsFalse(r == r2);
			
			try
			{
				r2.DeleteDocument(0);
				Assert.Fail();
			}
			catch (System.NotSupportedException uoe)
			{
				// expected
			}
			
			writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize();
			writer.Close();
			
			// Make sure reopen to a single segment is still readonly:
			IndexReader r3 = r2.Reopen();
			r2.Close();
			
			Assert.IsFalse(r == r2);
			
			try
			{
				r3.DeleteDocument(0);
				Assert.Fail();
			}
			catch (System.NotSupportedException uoe)
			{
				// expected
			}
			
			// Make sure write lock isn't held
			writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Close();
			
			r3.Close();
		}
		
		[Test]
		public virtual void  TestFalseDirectoryAlreadyClosed()
		{
			
			System.IO.FileInfo indexDir = _TestUtil.GetTempDir("lucenetestdiralreadyclosed");
			
			try
			{
				FSDirectory dir = FSDirectory.GetDirectory(indexDir);
				IndexWriter w = new IndexWriter(indexDir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
				w.SetUseCompoundFile(false);
				Document doc = new Document();
				w.AddDocument(doc);
				w.Close();
				bool tmpBool;
				if (System.IO.File.Exists(new System.IO.FileInfo(System.IO.Path.Combine(indexDir.FullName, "_0.fnm")).FullName))
				{
					System.IO.File.Delete(new System.IO.FileInfo(System.IO.Path.Combine(indexDir.FullName, "_0.fnm")).FullName);
					tmpBool = true;
				}
				else if (System.IO.Directory.Exists(new System.IO.FileInfo(System.IO.Path.Combine(indexDir.FullName, "_0.fnm")).FullName))
				{
					System.IO.Directory.Delete(new System.IO.FileInfo(System.IO.Path.Combine(indexDir.FullName, "_0.fnm")).FullName);
					tmpBool = true;
				}
				else
					tmpBool = false;
				Assert.IsTrue(tmpBool);
				
				try
				{
					IndexReader.Open(indexDir);
					Assert.Fail("did not hit expected exception");
				}
				catch (AlreadyClosedException ace)
				{
					Assert.Fail("should not have hit AlreadyClosedException");
				}
				catch (System.IO.FileNotFoundException ioe)
				{
					// expected
				}
				
				// Make sure we really did close the dir inside IndexReader.open
				dir.Close();
				
				try
				{
					dir.FileExists("hi");
					Assert.Fail("did not hit expected exception");
				}
				catch (AlreadyClosedException ace)
				{
					// expected
				}
			}
			finally
			{
				_TestUtil.RmDir(indexDir);
			}
		}
		
		
		// LUCENE-1474
		[Test]
		public virtual void  TestIndexReader_Rename()
		{
			Directory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			writer.AddDocument(CreateDocument("a"));
			writer.AddDocument(CreateDocument("b"));
			writer.AddDocument(CreateDocument("c"));
			writer.Close();
			IndexReader reader = IndexReader.Open(dir);
			reader.DeleteDocuments(new Term("id", "a"));
			reader.Flush();
			reader.DeleteDocuments(new Term("id", "b"));
			reader.Close();
			IndexReader.Open(dir).Close();
		}
		
		// LUCENE-1647
		[Test]
		public virtual void  TestIndexReaderUnDeleteAll()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			dir.SetPreventDoubleWrite(false);
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			writer.AddDocument(CreateDocument("a"));
			writer.AddDocument(CreateDocument("b"));
			writer.AddDocument(CreateDocument("c"));
			writer.Close();
			IndexReader reader = IndexReader.Open(dir);
			reader.DeleteDocuments(new Term("id", "a"));
			reader.Flush();
			reader.DeleteDocuments(new Term("id", "b"));
			reader.UndeleteAll();
			reader.DeleteDocuments(new Term("id", "b"));
			reader.Close();
			IndexReader.Open(dir).Close();
			dir.Close();
		}
		
		private Document CreateDocument(System.String id)
		{
			Document doc = new Document();
			doc.Add(new Field("id", id, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
			return doc;
		}
		
		// LUCENE-1468 -- make sure on attempting to open an
		// IndexReader on a non-existent directory, you get a
		// good exception
		[Test]
		public virtual void  TestNoDir()
		{
			Directory dir = FSDirectory.Open(_TestUtil.GetTempDir("doesnotexist"));
			try
			{
				IndexReader.Open(dir);
				Assert.Fail("did not hit expected exception");
			}
			catch (NoSuchDirectoryException nsde)
			{
				// expected
			}
			dir.Close();
		}
		
		// LUCENE-1509
		[Test]
		public virtual void  TestNoDupCommitFileNames()
		{
			
			Directory dir = new MockRAMDirectory();
			
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			
			writer.SetMaxBufferedDocs(2);
			writer.AddDocument(CreateDocument("a"));
			writer.AddDocument(CreateDocument("a"));
			writer.AddDocument(CreateDocument("a"));
			writer.Close();
			
			System.Collections.ICollection commits = IndexReader.ListCommits(dir);
			System.Collections.IEnumerator it = commits.GetEnumerator();
			while (it.MoveNext())
			{
				IndexCommit commit = (IndexCommit) it.Current;
				System.Collections.Generic.ICollection<string> files = commit.GetFileNames();
				System.Collections.Hashtable seen = new System.Collections.Hashtable();
				System.Collections.IEnumerator it2 = files.GetEnumerator();
				while (it2.MoveNext())
				{
					System.String fileName = (System.String) it2.Current;
					Assert.IsTrue(!seen.Contains(fileName), "file " + fileName + " was duplicated");
					seen.Add(fileName, fileName);
				}
			}
			
			dir.Close();
		}
		
		// LUCENE-1579: Ensure that on a cloned reader, segments
		// reuse the doc values arrays in FieldCache
		[Test]
		public virtual void  TestFieldCacheReuseAfterClone()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			doc.Add(new Field("number", "17", Field.Store.NO, Field.Index.NOT_ANALYZED));
			writer.AddDocument(doc);
			writer.Close();
			
			// Open reader
			IndexReader r = SegmentReader.GetOnlySegmentReader(dir);
			int[] ints = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetInts(r, "number");
			Assert.AreEqual(1, ints.Length);
			Assert.AreEqual(17, ints[0]);
			
			// Clone reader
			IndexReader r2 = (IndexReader) r.Clone();
			r.Close();
			Assert.IsTrue(r2 != r);
			int[] ints2 = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetInts(r2, "number");
			r2.Close();
			
			Assert.AreEqual(1, ints2.Length);
			Assert.AreEqual(17, ints2[0]);
			Assert.IsTrue(ints == ints2);
			
			dir.Close();
		}
		
		// LUCENE-1579: Ensure that on a reopened reader, that any
		// shared segments reuse the doc values arrays in
		// FieldCache
		[Test]
		public virtual void  TestFieldCacheReuseAfterReopen()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			doc.Add(new Field("number", "17", Field.Store.NO, Field.Index.NOT_ANALYZED));
			writer.AddDocument(doc);
			writer.Commit();
			
			// Open reader1
			IndexReader r = IndexReader.Open(dir);
			IndexReader r1 = SegmentReader.GetOnlySegmentReader(r);
			int[] ints = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetInts(r1, "number");
			Assert.AreEqual(1, ints.Length);
			Assert.AreEqual(17, ints[0]);
			
			// Add new segment
			writer.AddDocument(doc);
			writer.Commit();
			
			// Reopen reader1 --> reader2
			IndexReader r2 = r.Reopen();
			r.Close();
			IndexReader sub0 = r2.GetSequentialSubReaders()[0];
			int[] ints2 = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetInts(sub0, "number");
			r2.Close();
			Assert.IsTrue(ints == ints2);
			
			dir.Close();
		}
		
		// LUCENE-1579: Make sure all SegmentReaders are new when
		// reopen switches readOnly
		[Test]
		public virtual void  TestReopenChangeReadonly()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			doc.Add(new Field("number", "17", Field.Store.NO, Field.Index.NOT_ANALYZED));
			writer.AddDocument(doc);
			writer.Commit();
			
			// Open reader1
			IndexReader r = IndexReader.Open(dir);
			Assert.IsTrue(r is DirectoryReader);
			IndexReader r1 = SegmentReader.GetOnlySegmentReader(r);
			int[] ints = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetInts(r1, "number");
			Assert.AreEqual(1, ints.Length);
			Assert.AreEqual(17, ints[0]);
			
			// Reopen to readonly w/ no chnages
			IndexReader r3 = r.Reopen(true);
			Assert.IsTrue(r3 is ReadOnlyDirectoryReader);
			r3.Close();
			
			// Add new segment
			writer.AddDocument(doc);
			writer.Commit();
			
			// Reopen reader1 --> reader2
			IndexReader r2 = r.Reopen(true);
			r.Close();
			Assert.IsTrue(r2 is ReadOnlyDirectoryReader);
			IndexReader[] subs = r2.GetSequentialSubReaders();
			int[] ints2 = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetInts(subs[0], "number");
			r2.Close();
			
			Assert.IsTrue(subs[0] is ReadOnlySegmentReader);
			Assert.IsTrue(subs[1] is ReadOnlySegmentReader);
			Assert.IsTrue(ints == ints2);
			
			dir.Close();
		}
		
		// LUCENE-1586: getUniqueTermCount
		[Test]
		public virtual void  TestUniqueTermCount()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "a b c d e f g h i j k l m n o p q r s t u v w x y z", Field.Store.NO, Field.Index.ANALYZED));
			doc.Add(new Field("number", "0 1 2 3 4 5 6 7 8 9", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.AddDocument(doc);
			writer.Commit();
			
			IndexReader r = IndexReader.Open(dir);
			IndexReader r1 = SegmentReader.GetOnlySegmentReader(r);
			Assert.AreEqual(36, r1.GetUniqueTermCount());
			writer.AddDocument(doc);
			writer.Commit();
			IndexReader r2 = r.Reopen();
			r.Close();
			try
			{
				r2.GetUniqueTermCount();
				Assert.Fail("expected exception");
			}
			catch (System.NotSupportedException uoe)
			{
				// expected
			}
			IndexReader[] subs = r2.GetSequentialSubReaders();
			for (int i = 0; i < subs.Length; i++)
			{
				Assert.AreEqual(36, subs[i].GetUniqueTermCount());
			}
			r2.Close();
			writer.Close();
			dir.Close();
		}
		
		// LUCENE-1609: don't load terms index
		[Test]
		public virtual void  TestNoTermsIndex()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "a b c d e f g h i j k l m n o p q r s t u v w x y z", Field.Store.NO, Field.Index.ANALYZED));
			doc.Add(new Field("number", "0 1 2 3 4 5 6 7 8 9", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.AddDocument(doc);
			writer.Close();
			
			IndexReader r = IndexReader.Open(dir, null, true, - 1);
			try
			{
				r.DocFreq(new Term("field", "f"));
				Assert.Fail("did not hit expected exception");
			}
			catch (System.SystemException ise)
			{
				// expected
			}
			Assert.IsFalse(((SegmentReader) r.GetSequentialSubReaders()[0]).TermsIndexLoaded());
			
			Assert.AreEqual(- 1, r.GetTermInfosIndexDivisor());
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			writer.AddDocument(doc);
			writer.Close();
			
			// LUCENE-1718: ensure re-open carries over no terms index:
			IndexReader r2 = r.Reopen();
			r.Close();
			IndexReader[] subReaders = r2.GetSequentialSubReaders();
			Assert.AreEqual(2, subReaders.Length);
			for (int i = 0; i < 2; i++)
			{
				Assert.IsFalse(((SegmentReader) subReaders[i]).TermsIndexLoaded());
			}
			r2.Close();
			dir.Close();
		}
	}
}