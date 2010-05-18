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
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using Searcher = Lucene.Net.Search.Searcher;
using TermQuery = Lucene.Net.Search.TermQuery;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Documents
{
	
	/// <summary> Tests {@link Document} class.
	/// 
	/// 
	/// </summary>
	/// <version>  $Id: TestDocument.java 754789 2009-03-15 23:24:39Z mikemccand $
	/// </version>
	[TestFixture]
	public class TestDocument:LuceneTestCase
	{
		
		internal System.String binaryVal = "this text will be stored as a byte array in the index";
		internal System.String binaryVal2 = "this text will be also stored as a byte array in the index";
		
		[Test]
		public virtual void  TestBinaryField()
		{
			Document doc = new Document();
			Fieldable stringFld = new Field("string", binaryVal, Field.Store.YES, Field.Index.NO);
			Fieldable binaryFld = new Field("binary", System.Text.UTF8Encoding.UTF8.GetBytes(binaryVal), Field.Store.YES);
			Fieldable binaryFld2 = new Field("binary", System.Text.UTF8Encoding.UTF8.GetBytes(binaryVal2), Field.Store.YES);
			
			doc.Add(stringFld);
			doc.Add(binaryFld);
			
			Assert.AreEqual(2, doc.fields_ForNUnit.Count);
			
			Assert.IsTrue(binaryFld.IsBinary());
			Assert.IsTrue(binaryFld.IsStored());
			Assert.IsFalse(binaryFld.IsIndexed());
			Assert.IsFalse(binaryFld.IsTokenized());
			
			System.String binaryTest = new System.String(System.Text.UTF8Encoding.UTF8.GetChars(doc.GetBinaryValue("binary")));
			Assert.IsTrue(binaryTest.Equals(binaryVal));
			
			System.String stringTest = doc.Get("string");
			Assert.IsTrue(binaryTest.Equals(stringTest));
			
			doc.Add(binaryFld2);
			
			Assert.AreEqual(3, doc.fields_ForNUnit.Count);
			
			byte[][] binaryTests = doc.GetBinaryValues("binary");
			
			Assert.AreEqual(2, binaryTests.Length);
			
			binaryTest = new System.String(System.Text.UTF8Encoding.UTF8.GetChars(binaryTests[0]));
			System.String binaryTest2 = new System.String(System.Text.UTF8Encoding.UTF8.GetChars(binaryTests[1]));
			
			Assert.IsFalse(binaryTest.Equals(binaryTest2));
			
			Assert.IsTrue(binaryTest.Equals(binaryVal));
			Assert.IsTrue(binaryTest2.Equals(binaryVal2));
			
			doc.RemoveField("string");
			Assert.AreEqual(2, doc.fields_ForNUnit.Count);
			
			doc.RemoveFields("binary");
			Assert.AreEqual(0, doc.fields_ForNUnit.Count);
		}
		
		/// <summary> Tests {@link Document#RemoveField(String)} method for a brand new Document
		/// that has not been indexed yet.
		/// 
		/// </summary>
		/// <throws>  Exception on error </throws>
		[Test]
		public virtual void  TestRemoveForNewDocument()
		{
			Document doc = MakeDocumentWithFields();
			Assert.AreEqual(8, doc.fields_ForNUnit.Count);
			doc.RemoveFields("keyword");
			Assert.AreEqual(6, doc.fields_ForNUnit.Count);
			doc.RemoveFields("doesnotexists"); // removing non-existing fields is siltenlty ignored
			doc.RemoveFields("keyword"); // removing a field more than once
			Assert.AreEqual(6, doc.fields_ForNUnit.Count);
			doc.RemoveField("text");
			Assert.AreEqual(5, doc.fields_ForNUnit.Count);
			doc.RemoveField("text");
			Assert.AreEqual(4, doc.fields_ForNUnit.Count);
			doc.RemoveField("text");
			Assert.AreEqual(4, doc.fields_ForNUnit.Count);
			doc.RemoveField("doesnotexists"); // removing non-existing fields is siltenlty ignored
			Assert.AreEqual(4, doc.fields_ForNUnit.Count);
			doc.RemoveFields("unindexed");
			Assert.AreEqual(2, doc.fields_ForNUnit.Count);
			doc.RemoveFields("unstored");
			Assert.AreEqual(0, doc.fields_ForNUnit.Count);
			doc.RemoveFields("doesnotexists"); // removing non-existing fields is siltenlty ignored
			Assert.AreEqual(0, doc.fields_ForNUnit.Count);
		}
		
		[Test]
		public virtual void  TestConstructorExceptions()
		{
			new Field("name", "value", Field.Store.YES, Field.Index.NO); // okay
			new Field("name", "value", Field.Store.NO, Field.Index.NOT_ANALYZED); // okay
			try
			{
				new Field("name", "value", Field.Store.NO, Field.Index.NO);
				Assert.Fail();
			}
			catch (System.ArgumentException e)
			{
				// expected exception
			}
			new Field("name", "value", Field.Store.YES, Field.Index.NO, Field.TermVector.NO); // okay
			try
			{
				new Field("name", "value", Field.Store.YES, Field.Index.NO, Field.TermVector.YES);
				Assert.Fail();
			}
			catch (System.ArgumentException e)
			{
				// expected exception
			}
		}
		
		/// <summary> Tests {@link Document#GetValues(String)} method for a brand new Document
		/// that has not been indexed yet.
		/// 
		/// </summary>
		/// <throws>  Exception on error </throws>
		[Test]
		public virtual void  TestGetValuesForNewDocument()
		{
			DoAssert(MakeDocumentWithFields(), false);
		}
		
		/// <summary> Tests {@link Document#GetValues(String)} method for a Document retrieved from
		/// an index.
		/// 
		/// </summary>
		/// <throws>  Exception on error </throws>
		[Test]
		public virtual void  TestGetValuesForIndexedDocument()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.AddDocument(MakeDocumentWithFields());
			writer.Close();
			
			Searcher searcher = new IndexSearcher(dir);
			
			// search for something that does exists
			Query query = new TermQuery(new Term("keyword", "test1"));
			
			// ensure that queries return expected results without DateFilter first
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			DoAssert(searcher.Doc(hits[0].doc), true);
			searcher.Close();
		}
		
		private Document MakeDocumentWithFields()
		{
			Document doc = new Document();
			doc.Add(new Field("keyword", "test1", Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.Add(new Field("keyword", "test2", Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.Add(new Field("text", "test1", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("text", "test2", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("unindexed", "test1", Field.Store.YES, Field.Index.NO));
			doc.Add(new Field("unindexed", "test2", Field.Store.YES, Field.Index.NO));
			doc.Add(new Field("unstored", "test1", Field.Store.NO, Field.Index.ANALYZED));
			doc.Add(new Field("unstored", "test2", Field.Store.NO, Field.Index.ANALYZED));
			return doc;
		}
		
		private void  DoAssert(Document doc, bool fromIndex)
		{
			System.String[] keywordFieldValues = doc.GetValues("keyword");
			System.String[] textFieldValues = doc.GetValues("text");
			System.String[] unindexedFieldValues = doc.GetValues("unindexed");
			System.String[] unstoredFieldValues = doc.GetValues("unstored");
			
			Assert.IsTrue(keywordFieldValues.Length == 2);
			Assert.IsTrue(textFieldValues.Length == 2);
			Assert.IsTrue(unindexedFieldValues.Length == 2);
			// this test cannot work for documents retrieved from the index
			// since unstored fields will obviously not be returned
			if (!fromIndex)
			{
				Assert.IsTrue(unstoredFieldValues.Length == 2);
			}
			
			Assert.IsTrue(keywordFieldValues[0].Equals("test1"));
			Assert.IsTrue(keywordFieldValues[1].Equals("test2"));
			Assert.IsTrue(textFieldValues[0].Equals("test1"));
			Assert.IsTrue(textFieldValues[1].Equals("test2"));
			Assert.IsTrue(unindexedFieldValues[0].Equals("test1"));
			Assert.IsTrue(unindexedFieldValues[1].Equals("test2"));
			// this test cannot work for documents retrieved from the index
			// since unstored fields will obviously not be returned
			if (!fromIndex)
			{
				Assert.IsTrue(unstoredFieldValues[0].Equals("test1"));
				Assert.IsTrue(unstoredFieldValues[1].Equals("test2"));
			}
		}
		
		[Test]
		public virtual void  TestFieldSetValue()
		{
			
			Field field = new Field("id", "id1", Field.Store.YES, Field.Index.NOT_ANALYZED);
			Document doc = new Document();
			doc.Add(field);
			doc.Add(new Field("keyword", "test", Field.Store.YES, Field.Index.NOT_ANALYZED));
			
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.AddDocument(doc);
			field.SetValue("id2");
			writer.AddDocument(doc);
			field.SetValue("id3");
			writer.AddDocument(doc);
			writer.Close();
			
			Searcher searcher = new IndexSearcher(dir);
			
			Query query = new TermQuery(new Term("keyword", "test"));
			
			// ensure that queries return expected results without DateFilter first
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			int result = 0;
			for (int i = 0; i < 3; i++)
			{
				Document doc2 = searcher.Doc(hits[i].doc);
				Field f = doc2.GetField("id");
				if (f.StringValue().Equals("id1"))
					result |= 1;
				else if (f.StringValue().Equals("id2"))
					result |= 2;
				else if (f.StringValue().Equals("id3"))
					result |= 4;
				else
					Assert.Fail("unexpected id field");
			}
			searcher.Close();
			dir.Close();
			Assert.AreEqual(7, result, "did not see all IDs");
		}
		
		[Test]
		public virtual void  TestFieldSetValueChangeBinary()
		{
			Field field1 = new Field("field1", new byte[0], Field.Store.YES);
			Field field2 = new Field("field2", "", Field.Store.YES, Field.Index.ANALYZED);
			try
			{
				field1.SetValue("abc");
				Assert.Fail("did not hit expected exception");
			}
			catch (System.ArgumentException iae)
			{
				// expected
			}
			try
			{
				field2.SetValue(new byte[0]);
				Assert.Fail("did not hit expected exception");
			}
			catch (System.ArgumentException iae)
			{
				// expected
			}
		}
	}
}