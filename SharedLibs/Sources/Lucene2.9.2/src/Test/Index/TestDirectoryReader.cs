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

using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestDirectoryReader:LuceneTestCase
	{
		protected internal Directory dir;
		private Document doc1;
		private Document doc2;
		protected internal SegmentReader[] readers = new SegmentReader[2];
		protected internal SegmentInfos sis;
		
		
		public TestDirectoryReader(System.String s):base(s)
		{
		}

        public TestDirectoryReader() : base("")
        {
        }
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			dir = new RAMDirectory();
			doc1 = new Document();
			doc2 = new Document();
			DocHelper.SetupDoc(doc1);
			DocHelper.SetupDoc(doc2);
			DocHelper.WriteDoc(dir, doc1);
			DocHelper.WriteDoc(dir, doc2);
			sis = new SegmentInfos();
			sis.Read(dir);
		}
		
		protected internal virtual IndexReader OpenReader()
		{
			IndexReader reader;
			reader = IndexReader.Open(dir);
			Assert.IsTrue(reader is DirectoryReader);
			
			Assert.IsTrue(dir != null);
			Assert.IsTrue(sis != null);
			Assert.IsTrue(reader != null);
			
			return reader;
		}
		
        [Test]
		public virtual void  Test()
		{
			SetUp();
			DoTestDocument();
			DoTestUndeleteAll();
		}
		
		public virtual void  DoTestDocument()
		{
			sis.Read(dir);
			IndexReader reader = OpenReader();
			Assert.IsTrue(reader != null);
			Document newDoc1 = reader.Document(0);
			Assert.IsTrue(newDoc1 != null);
			Assert.IsTrue(DocHelper.NumFields(newDoc1) == DocHelper.NumFields(doc1) - DocHelper.unstored.Count);
			Document newDoc2 = reader.Document(1);
			Assert.IsTrue(newDoc2 != null);
			Assert.IsTrue(DocHelper.NumFields(newDoc2) == DocHelper.NumFields(doc2) - DocHelper.unstored.Count);
			TermFreqVector vector = reader.GetTermFreqVector(0, DocHelper.TEXT_FIELD_2_KEY);
			Assert.IsTrue(vector != null);
			TestSegmentReader.CheckNorms(reader);
		}
		
		public virtual void  DoTestUndeleteAll()
		{
			sis.Read(dir);
			IndexReader reader = OpenReader();
			Assert.IsTrue(reader != null);
			Assert.AreEqual(2, reader.NumDocs());
			reader.DeleteDocument(0);
			Assert.AreEqual(1, reader.NumDocs());
			reader.UndeleteAll();
			Assert.AreEqual(2, reader.NumDocs());
			
			// Ensure undeleteAll survives commit/close/reopen:
			reader.Commit();
			reader.Close();
			
			if (reader is MultiReader)
			// MultiReader does not "own" the directory so it does
			// not write the changes to sis on commit:
				sis.Commit(dir);
			
			sis.Read(dir);
			reader = OpenReader();
			Assert.AreEqual(2, reader.NumDocs());
			
			reader.DeleteDocument(0);
			Assert.AreEqual(1, reader.NumDocs());
			reader.Commit();
			reader.Close();
			if (reader is MultiReader)
			// MultiReader does not "own" the directory so it does
			// not write the changes to sis on commit:
				sis.Commit(dir);
			sis.Read(dir);
			reader = OpenReader();
			Assert.AreEqual(1, reader.NumDocs());
		}
		
		
		public virtual void  _testTermVectors()
		{
			MultiReader reader = new MultiReader(readers);
			Assert.IsTrue(reader != null);
		}
		
		
        [Test]
		public virtual void  TestIsCurrent()
		{
			RAMDirectory ramDir1 = new RAMDirectory();
			AddDoc(ramDir1, "test foo", true);
			RAMDirectory ramDir2 = new RAMDirectory();
			AddDoc(ramDir2, "test blah", true);
			IndexReader[] readers = new IndexReader[]{IndexReader.Open(ramDir1), IndexReader.Open(ramDir2)};
			MultiReader mr = new MultiReader(readers);
			Assert.IsTrue(mr.IsCurrent()); // just opened, must be current
			AddDoc(ramDir1, "more text", false);
			Assert.IsFalse(mr.IsCurrent()); // has been modified, not current anymore
			AddDoc(ramDir2, "even more text", false);
			Assert.IsFalse(mr.IsCurrent()); // has been modified even more, not current anymore
			try
			{
				mr.GetVersion();
				Assert.Fail();
			}
			catch (System.NotSupportedException e)
			{
				// expected exception
			}
			mr.Close();
		}
		
        [Test]
		public virtual void  TestMultiTermDocs()
		{
			RAMDirectory ramDir1 = new RAMDirectory();
			AddDoc(ramDir1, "test foo", true);
			RAMDirectory ramDir2 = new RAMDirectory();
			AddDoc(ramDir2, "test blah", true);
			RAMDirectory ramDir3 = new RAMDirectory();
			AddDoc(ramDir3, "test wow", true);
			
			IndexReader[] readers1 = new IndexReader[]{IndexReader.Open(ramDir1), IndexReader.Open(ramDir3)};
			IndexReader[] readers2 = new IndexReader[]{IndexReader.Open(ramDir1), IndexReader.Open(ramDir2), IndexReader.Open(ramDir3)};
			MultiReader mr2 = new MultiReader(readers1);
			MultiReader mr3 = new MultiReader(readers2);
			
			// test mixing up TermDocs and TermEnums from different readers.
			TermDocs td2 = mr2.TermDocs();
			TermEnum te3 = mr3.Terms(new Term("body", "wow"));
			td2.Seek(te3);
			int ret = 0;
			
			// This should blow up if we forget to check that the TermEnum is from the same
			// reader as the TermDocs.
			while (td2.Next())
				ret += td2.Doc();
			td2.Close();
			te3.Close();
			
			// really a dummy assert to ensure that we got some docs and to ensure that
			// nothing is optimized out.
			Assert.IsTrue(ret > 0);
		}
		
        [Test]
		public virtual void  TestAllTermDocs()
		{
			IndexReader reader = OpenReader();
			int NUM_DOCS = 2;
			TermDocs td = reader.TermDocs(null);
			for (int i = 0; i < NUM_DOCS; i++)
			{
				Assert.IsTrue(td.Next());
				Assert.AreEqual(i, td.Doc());
				Assert.AreEqual(1, td.Freq());
			}
			td.Close();
			reader.Close();
		}
		
		private void  AddDoc(RAMDirectory ramDir1, System.String s, bool create)
		{
			IndexWriter iw = new IndexWriter(ramDir1, new StandardAnalyzer(), create, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("body", s, Field.Store.YES, Field.Index.ANALYZED));
			iw.AddDocument(doc);
			iw.Close();
		}
	}
}