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
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	
    [TestFixture]
	public class TestSegmentTermEnum:LuceneTestCase
	{
		internal Directory dir = new RAMDirectory();
		
		[Test]
		public virtual void  TestTermEnum()
		{
			IndexWriter writer = null;
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			// ADD 100 documents with term : aaa
			// add 100 documents with terms: aaa bbb
			// Therefore, term 'aaa' has document frequency of 200 and term 'bbb' 100
			for (int i = 0; i < 100; i++)
			{
				AddDoc(writer, "aaa");
				AddDoc(writer, "aaa bbb");
			}
			
			writer.Close();
			
			// verify document frequency of terms in an unoptimized index
			VerifyDocFreq();
			
			// merge segments by optimizing the index
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize();
			writer.Close();
			
			// verify document frequency of terms in an optimized index
			VerifyDocFreq();
		}
		
		[Test]
		public virtual void  TestPrevTermAtEnd()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDoc(writer, "aaa bbb");
			writer.Close();
			SegmentReader reader = SegmentReader.GetOnlySegmentReader(dir);
			SegmentTermEnum termEnum = (SegmentTermEnum) reader.Terms();
			Assert.IsTrue(termEnum.Next());
			Assert.AreEqual("aaa", termEnum.Term().Text());
			Assert.IsTrue(termEnum.Next());
			Assert.AreEqual("aaa", termEnum.Prev().Text());
			Assert.AreEqual("bbb", termEnum.Term().Text());
			Assert.IsFalse(termEnum.Next());
			Assert.AreEqual("bbb", termEnum.Prev().Text());
		}
		
		private void  VerifyDocFreq()
		{
			IndexReader reader = IndexReader.Open(dir);
			TermEnum termEnum = null;
			
			// create enumeration of all terms
			termEnum = reader.Terms();
			// go to the first term (aaa)
			termEnum.Next();
			// assert that term is 'aaa'
			Assert.AreEqual("aaa", termEnum.Term().Text());
			Assert.AreEqual(200, termEnum.DocFreq());
			// go to the second term (bbb)
			termEnum.Next();
			// assert that term is 'bbb'
			Assert.AreEqual("bbb", termEnum.Term().Text());
			Assert.AreEqual(100, termEnum.DocFreq());
			
			termEnum.Close();
			
			
			// create enumeration of terms after term 'aaa', including 'aaa'
			termEnum = reader.Terms(new Term("content", "aaa"));
			// assert that term is 'aaa'
			Assert.AreEqual("aaa", termEnum.Term().Text());
			Assert.AreEqual(200, termEnum.DocFreq());
			// go to term 'bbb'
			termEnum.Next();
			// assert that term is 'bbb'
			Assert.AreEqual("bbb", termEnum.Term().Text());
			Assert.AreEqual(100, termEnum.DocFreq());
			
			termEnum.Close();
		}
		
		private void  AddDoc(IndexWriter writer, System.String value_Renamed)
		{
			Document doc = new Document();
			doc.Add(new Field("content", value_Renamed, Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
		}
	}
}