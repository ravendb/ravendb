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

using SimpleAnalyzer = Lucene.Net.Analysis.SimpleAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Norm = Lucene.Net.Index.SegmentReader.Norm;
using AlreadyClosedException = Lucene.Net.Store.AlreadyClosedException;
using Directory = Lucene.Net.Store.Directory;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using LockObtainFailedException = Lucene.Net.Store.LockObtainFailedException;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using Similarity = Lucene.Net.Search.Similarity;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	/// <summary> Tests cloning multiple types of readers, modifying the deletedDocs and norms
	/// and verifies copy on write semantics of the deletedDocs and norms is
	/// implemented properly
	/// </summary>
    [TestFixture]
	public class TestIndexReaderClone:LuceneTestCase
	{
		
        [Test]
		public virtual void  TestCloneReadOnlySegmentReader()
		{
			Directory dir1 = new MockRAMDirectory();
			
			TestIndexReaderReopen.CreateIndex(dir1, false);
			IndexReader reader = IndexReader.Open(dir1);
			IndexReader readOnlyReader = reader.Clone(true);
			if (!IsReadOnly(readOnlyReader))
			{
				Assert.Fail("reader isn't read only");
			}
			if (DeleteWorked(1, readOnlyReader))
			{
				Assert.Fail("deleting from the original should not have worked");
			}
			reader.Close();
			readOnlyReader.Close();
			dir1.Close();
		}
		
		// LUCENE-1453
        [Test]
		public virtual void  TestFSDirectoryClone()
		{
			
			System.String tempDir = System.IO.Path.GetTempPath();
			if (tempDir == null)
				throw new System.IO.IOException("java.io.tmpdir undefined, cannot run test");
			System.IO.FileInfo indexDir2 = new System.IO.FileInfo(System.IO.Path.Combine(tempDir, "FSDirIndexReaderClone"));
			
			Directory dir1 = FSDirectory.GetDirectory(indexDir2);
			TestIndexReaderReopen.CreateIndex(dir1, false);
			
			IndexReader reader = IndexReader.Open(indexDir2);
			IndexReader readOnlyReader = (IndexReader) reader.Clone();
			reader.Close();
			readOnlyReader.Close();
			
			// Make sure we didn't pick up too many incRef's along
			// the way -- this close should be the final close:
			dir1.Close();
			
			try
			{
				dir1.ListAll();
				Assert.Fail("did not hit AlreadyClosedException");
			}
			catch (AlreadyClosedException ace)
			{
				// expected
			}
		}
		
		// open non-readOnly reader1, clone to non-readOnly
		// reader2, make sure we can change reader2
        [Test]
		public virtual void  TestCloneNoChangesStillReadOnly()
		{
			Directory dir1 = new MockRAMDirectory();
			
			TestIndexReaderReopen.CreateIndex(dir1, true);
			IndexReader r1 = IndexReader.Open(dir1, false);
			IndexReader r2 = r1.Clone(false);
			if (!DeleteWorked(1, r2))
			{
				Assert.Fail("deleting from the cloned should have worked");
			}
			r1.Close();
			r2.Close();
			dir1.Close();
		}
		
		// open non-readOnly reader1, clone to non-readOnly
		// reader2, make sure we can change reader1
        [Test]
		public virtual void  TestCloneWriteToOrig()
		{
			Directory dir1 = new MockRAMDirectory();
			
			TestIndexReaderReopen.CreateIndex(dir1, true);
			IndexReader r1 = IndexReader.Open(dir1, false);
			IndexReader r2 = r1.Clone(false);
			if (!DeleteWorked(1, r1))
			{
				Assert.Fail("deleting from the original should have worked");
			}
			r1.Close();
			r2.Close();
			dir1.Close();
		}
		
		// open non-readOnly reader1, clone to non-readOnly
		// reader2, make sure we can change reader2
        [Test]
		public virtual void  TestCloneWriteToClone()
		{
			Directory dir1 = new MockRAMDirectory();
			
			TestIndexReaderReopen.CreateIndex(dir1, true);
			IndexReader r1 = IndexReader.Open(dir1, false);
			IndexReader r2 = r1.Clone(false);
			if (!DeleteWorked(1, r2))
			{
				Assert.Fail("deleting from the original should have worked");
			}
			// should fail because reader1 holds the write lock
			Assert.IsTrue(!DeleteWorked(1, r1), "first reader should not be able to delete");
			r2.Close();
			// should fail because we are now stale (reader1
			// committed changes)
			Assert.IsTrue(!DeleteWorked(1, r1), "first reader should not be able to delete");
			r1.Close();
			
			dir1.Close();
		}
		
		// create single-segment index, open non-readOnly
		// SegmentReader, add docs, reopen to multireader, then do
		// delete
        [Test]
		public virtual void  TestReopenSegmentReaderToMultiReader()
		{
			Directory dir1 = new MockRAMDirectory();
			
			TestIndexReaderReopen.CreateIndex(dir1, false);
			IndexReader reader1 = IndexReader.Open(dir1, false);
			
			TestIndexReaderReopen.ModifyIndex(5, dir1);
			
			IndexReader reader2 = reader1.Reopen();
			Assert.IsTrue(reader1 != reader2);
			
			Assert.IsTrue(DeleteWorked(1, reader2));
			reader1.Close();
			reader2.Close();
			dir1.Close();
		}
		
		// open non-readOnly reader1, clone to readOnly reader2
        [Test]
		public virtual void  TestCloneWriteableToReadOnly()
		{
			Directory dir1 = new MockRAMDirectory();
			
			TestIndexReaderReopen.CreateIndex(dir1, true);
			IndexReader reader = IndexReader.Open(dir1, false);
			IndexReader readOnlyReader = reader.Clone(true);
			if (!IsReadOnly(readOnlyReader))
			{
				Assert.Fail("reader isn't read only");
			}
			if (DeleteWorked(1, readOnlyReader))
			{
				Assert.Fail("deleting from the original should not have worked");
			}
			// this readonly reader shouldn't have a write lock
			if (readOnlyReader.hasChanges_ForNUnit)
			{
				Assert.Fail("readOnlyReader has a write lock");
			}
			reader.Close();
			readOnlyReader.Close();
			dir1.Close();
		}
		
		// open non-readOnly reader1, reopen to readOnly reader2
        [Test]
		public virtual void  TestReopenWriteableToReadOnly()
		{
			Directory dir1 = new MockRAMDirectory();
			
			TestIndexReaderReopen.CreateIndex(dir1, true);
			IndexReader reader = IndexReader.Open(dir1, false);
			int docCount = reader.NumDocs();
			Assert.IsTrue(DeleteWorked(1, reader));
			Assert.AreEqual(docCount - 1, reader.NumDocs());
			
			IndexReader readOnlyReader = reader.Reopen(true);
			if (!IsReadOnly(readOnlyReader))
			{
				Assert.Fail("reader isn't read only");
			}
			Assert.IsFalse(DeleteWorked(1, readOnlyReader));
			Assert.AreEqual(docCount - 1, readOnlyReader.NumDocs());
			reader.Close();
			readOnlyReader.Close();
			dir1.Close();
		}
		
		// open readOnly reader1, clone to non-readOnly reader2
        [Test]
		public virtual void  TestCloneReadOnlyToWriteable()
		{
			Directory dir1 = new MockRAMDirectory();
			
			TestIndexReaderReopen.CreateIndex(dir1, true);
			IndexReader reader1 = IndexReader.Open(dir1, true);
			
			IndexReader reader2 = reader1.Clone(false);
			if (IsReadOnly(reader2))
			{
				Assert.Fail("reader should not be read only");
			}
			Assert.IsFalse(DeleteWorked(1, reader1), "deleting from the original reader should not have worked");
			// this readonly reader shouldn't yet have a write lock
			if (reader2.hasChanges_ForNUnit)
			{
				Assert.Fail("cloned reader should not have write lock");
			}
			Assert.IsTrue(DeleteWorked(1, reader2), "deleting from the cloned reader should have worked");
			reader1.Close();
			reader2.Close();
			dir1.Close();
		}
		
		// open non-readOnly reader1 on multi-segment index, then
		// optimize the index, then clone to readOnly reader2
        [Test]
		public virtual void  TestReadOnlyCloneAfterOptimize()
		{
			Directory dir1 = new MockRAMDirectory();
			
			TestIndexReaderReopen.CreateIndex(dir1, true);
			IndexReader reader1 = IndexReader.Open(dir1, false);
			IndexWriter w = new IndexWriter(dir1, new SimpleAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			w.Optimize();
			w.Close();
			IndexReader reader2 = reader1.Clone(true);
			Assert.IsTrue(IsReadOnly(reader2));
			reader1.Close();
			reader2.Close();
			dir1.Close();
		}
		
		private static bool DeleteWorked(int doc, IndexReader r)
		{
			bool exception = false;
			try
			{
				// trying to delete from the original reader should throw an exception
				r.DeleteDocument(doc);
			}
			catch (System.Exception ex)
			{
				exception = true;
			}
			return !exception;
		}
		
        [Test]
		public virtual void  TestCloneReadOnlyDirectoryReader()
		{
			Directory dir1 = new MockRAMDirectory();
			
			TestIndexReaderReopen.CreateIndex(dir1, true);
			IndexReader reader = IndexReader.Open(dir1);
			IndexReader readOnlyReader = reader.Clone(true);
			if (!IsReadOnly(readOnlyReader))
			{
				Assert.Fail("reader isn't read only");
			}
			reader.Close();
			readOnlyReader.Close();
			dir1.Close();
		}
		
		public static bool IsReadOnly(IndexReader r)
		{
			if (r is ReadOnlySegmentReader || r is ReadOnlyDirectoryReader)
				return true;
			return false;
		}
		
        [Test]
		public virtual void  TestParallelReader()
		{
			Directory dir1 = new MockRAMDirectory();
			TestIndexReaderReopen.CreateIndex(dir1, true);
			Directory dir2 = new MockRAMDirectory();
			TestIndexReaderReopen.CreateIndex(dir2, true);
			IndexReader r1 = IndexReader.Open(dir1);
			IndexReader r2 = IndexReader.Open(dir2);
			
			ParallelReader pr1 = new ParallelReader();
			pr1.Add(r1);
			pr1.Add(r2);
			
			PerformDefaultTests(pr1);
			pr1.Close();
			dir1.Close();
			dir2.Close();
		}
		
		/// <summary> 1. Get a norm from the original reader 2. Clone the original reader 3.
		/// Delete a document and set the norm of the cloned reader 4. Verify the norms
		/// are not the same on each reader 5. Verify the doc deleted is only in the
		/// cloned reader 6. Try to delete a document in the original reader, an
		/// exception should be thrown
		/// 
		/// </summary>
		/// <param name="r1">IndexReader to perform tests on
		/// </param>
		/// <throws>  Exception </throws>
		private void  PerformDefaultTests(IndexReader r1)
		{
			float norm1 = Similarity.DecodeNorm(r1.Norms("field1")[4]);
			
			IndexReader pr1Clone = (IndexReader) r1.Clone();
			pr1Clone.DeleteDocument(10);
			pr1Clone.SetNorm(4, "field1", 0.5f);
			Assert.IsTrue(Similarity.DecodeNorm(r1.Norms("field1")[4]) == norm1);
			Assert.IsTrue(Similarity.DecodeNorm(pr1Clone.Norms("field1")[4]) != norm1);
			
			Assert.IsTrue(!r1.IsDeleted(10));
			Assert.IsTrue(pr1Clone.IsDeleted(10));
			
			// try to update the original reader, which should throw an exception
			try
			{
				r1.DeleteDocument(11);
				Assert.Fail("Tried to delete doc 11 and an exception should have been thrown");
			}
			catch (System.Exception exception)
			{
				// expectted
			}
			pr1Clone.Close();
		}
		
        [Test]
		public virtual void  TestMixedReaders()
		{
			Directory dir1 = new MockRAMDirectory();
			TestIndexReaderReopen.CreateIndex(dir1, true);
			Directory dir2 = new MockRAMDirectory();
			TestIndexReaderReopen.CreateIndex(dir2, true);
			IndexReader r1 = IndexReader.Open(dir1);
			IndexReader r2 = IndexReader.Open(dir2);
			
			MultiReader multiReader = new MultiReader(new IndexReader[]{r1, r2});
			PerformDefaultTests(multiReader);
			multiReader.Close();
			dir1.Close();
			dir2.Close();
		}
		
        [Test]
		public virtual void  TestSegmentReaderUndeleteall()
		{
			Directory dir1 = new MockRAMDirectory();
			TestIndexReaderReopen.CreateIndex(dir1, false);
			SegmentReader origSegmentReader = SegmentReader.GetOnlySegmentReader(dir1);
			origSegmentReader.DeleteDocument(10);
			AssertDelDocsRefCountEquals(1, origSegmentReader);
			origSegmentReader.UndeleteAll();
			Assert.IsNull(origSegmentReader.deletedDocsRef_ForNUnit);
			origSegmentReader.Close();
			// need to test norms?
			dir1.Close();
		}
		
        [Test]
		public virtual void  TestSegmentReaderCloseReferencing()
		{
			Directory dir1 = new MockRAMDirectory();
			TestIndexReaderReopen.CreateIndex(dir1, false);
			SegmentReader origSegmentReader = SegmentReader.GetOnlySegmentReader(dir1);
			origSegmentReader.DeleteDocument(1);
			origSegmentReader.SetNorm(4, "field1", 0.5f);
			
			SegmentReader clonedSegmentReader = (SegmentReader) origSegmentReader.Clone();
			AssertDelDocsRefCountEquals(2, origSegmentReader);
			origSegmentReader.Close();
			AssertDelDocsRefCountEquals(1, origSegmentReader);
			// check the norm refs
			Norm norm = (Norm) clonedSegmentReader.norms_ForNUnit["field1"];
			Assert.AreEqual(1, norm.BytesRef().RefCount());
			clonedSegmentReader.Close();
			dir1.Close();
		}
		
        [Test]
		public virtual void  TestSegmentReaderDelDocsReferenceCounting()
		{
			Directory dir1 = new MockRAMDirectory();
			TestIndexReaderReopen.CreateIndex(dir1, false);
			
			IndexReader origReader = IndexReader.Open(dir1);
			SegmentReader origSegmentReader = SegmentReader.GetOnlySegmentReader(origReader);
			// deletedDocsRef should be null because nothing has updated yet
			Assert.IsNull(origSegmentReader.deletedDocsRef_ForNUnit);
			
			// we deleted a document, so there is now a deletedDocs bitvector and a
			// reference to it
			origReader.DeleteDocument(1);
			AssertDelDocsRefCountEquals(1, origSegmentReader);
			
			// the cloned segmentreader should have 2 references, 1 to itself, and 1 to
			// the original segmentreader
			IndexReader clonedReader = (IndexReader) origReader.Clone();
			SegmentReader clonedSegmentReader = SegmentReader.GetOnlySegmentReader(clonedReader);
			AssertDelDocsRefCountEquals(2, origSegmentReader);
			// deleting a document creates a new deletedDocs bitvector, the refs goes to
			// 1
			clonedReader.DeleteDocument(2);
			AssertDelDocsRefCountEquals(1, origSegmentReader);
			AssertDelDocsRefCountEquals(1, clonedSegmentReader);
			
			// make sure the deletedocs objects are different (copy
			// on write)
			Assert.IsTrue(origSegmentReader.deletedDocs_ForNUnit != clonedSegmentReader.deletedDocs_ForNUnit);
			
			AssertDocDeleted(origSegmentReader, clonedSegmentReader, 1);
			Assert.IsTrue(!origSegmentReader.IsDeleted(2)); // doc 2 should not be deleted
			// in original segmentreader
			Assert.IsTrue(clonedSegmentReader.IsDeleted(2)); // doc 2 should be deleted in
			// cloned segmentreader
			
			// deleting a doc from the original segmentreader should throw an exception
			try
			{
				origReader.DeleteDocument(4);
				Assert.Fail("expected exception");
			}
			catch (LockObtainFailedException lbfe)
			{
				// expected
			}
			
			origReader.Close();
			// try closing the original segment reader to see if it affects the
			// clonedSegmentReader
			clonedReader.DeleteDocument(3);
			clonedReader.Flush();
			AssertDelDocsRefCountEquals(1, clonedSegmentReader);
			
			// test a reopened reader
			IndexReader reopenedReader = clonedReader.Reopen();
			IndexReader cloneReader2 = (IndexReader) reopenedReader.Clone();
			SegmentReader cloneSegmentReader2 = SegmentReader.GetOnlySegmentReader(cloneReader2);
			AssertDelDocsRefCountEquals(2, cloneSegmentReader2);
			clonedReader.Close();
			reopenedReader.Close();
			cloneReader2.Close();
			
			dir1.Close();
		}
		
		// LUCENE-1648
        [Test]
		public virtual void  TestCloneWithDeletes()
		{
			Directory dir1 = new MockRAMDirectory();
			TestIndexReaderReopen.CreateIndex(dir1, false);
			IndexReader origReader = IndexReader.Open(dir1);
			origReader.DeleteDocument(1);
			
			IndexReader clonedReader = (IndexReader) origReader.Clone();
			origReader.Close();
			clonedReader.Close();
			
			IndexReader r = IndexReader.Open(dir1);
			Assert.IsTrue(r.IsDeleted(1));
			r.Close();
			dir1.Close();
		}
		
		// LUCENE-1648
        [Test]
		public virtual void  TestCloneWithSetNorm()
		{
			Directory dir1 = new MockRAMDirectory();
			TestIndexReaderReopen.CreateIndex(dir1, false);
			IndexReader orig = IndexReader.Open(dir1);
			orig.SetNorm(1, "field1", 17.0f);
			byte encoded = Similarity.EncodeNorm(17.0f);
			Assert.AreEqual(encoded, orig.Norms("field1")[1]);
			
			// the cloned segmentreader should have 2 references, 1 to itself, and 1 to
			// the original segmentreader
			IndexReader clonedReader = (IndexReader) orig.Clone();
			orig.Close();
			clonedReader.Close();
			
			IndexReader r = IndexReader.Open(dir1);
			Assert.AreEqual(encoded, r.Norms("field1")[1]);
			r.Close();
			dir1.Close();
		}
		
		private void  AssertDocDeleted(SegmentReader reader, SegmentReader reader2, int doc)
		{
			Assert.AreEqual(reader.IsDeleted(doc), reader2.IsDeleted(doc));
		}
		
		private void  AssertDelDocsRefCountEquals(int refCount, SegmentReader reader)
		{
			Assert.AreEqual(refCount, reader.deletedDocsRef_ForNUnit.RefCount());
		}
		
        [Test]
		public virtual void  TestCloneSubreaders()
		{
			Directory dir1 = new MockRAMDirectory();
			
			TestIndexReaderReopen.CreateIndex(dir1, true);
			IndexReader reader = IndexReader.Open(dir1);
			reader.DeleteDocument(1); // acquire write lock
			IndexReader[] subs = reader.GetSequentialSubReaders();
			System.Diagnostics.Debug.Assert(subs.Length > 1);
			
			IndexReader[] clones = new IndexReader[subs.Length];
			for (int x = 0; x < subs.Length; x++)
			{
				clones[x] = (IndexReader) subs[x].Clone();
			}
			reader.Close();
			for (int x = 0; x < subs.Length; x++)
			{
				clones[x].Close();
			}
			dir1.Close();
		}
		
        [Test]
		public virtual void  TestLucene1516Bug()
		{
			Directory dir1 = new MockRAMDirectory();
			TestIndexReaderReopen.CreateIndex(dir1, false);
			IndexReader r1 = IndexReader.Open(dir1);
			r1.IncRef();
			IndexReader r2 = r1.Clone(false);
			r1.DeleteDocument(5);
			r1.DecRef();
			
			r1.IncRef();
			
			r2.Close();
			r1.DecRef();
			r1.Close();
			dir1.Close();
		}
		
        [Test]
		public virtual void  TestCloseStoredFields()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new SimpleAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			w.SetUseCompoundFile(false);
			Document doc = new Document();
			doc.Add(new Field("field", "yes it's stored", Field.Store.YES, Field.Index.ANALYZED));
			w.AddDocument(doc);
			w.Close();
			IndexReader r1 = IndexReader.Open(dir);
			IndexReader r2 = r1.Clone(false);
			r1.Close();
			r2.Close();
			dir.Close();
		}
	}
}