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
using Index = Lucene.Net.Documents.Field.Index;
using Store = Lucene.Net.Documents.Field.Store;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestParallelTermEnum:LuceneTestCase
	{
		private IndexReader ir1;
		private IndexReader ir2;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			Document doc;
			
			RAMDirectory rd1 = new RAMDirectory();
			IndexWriter iw1 = new IndexWriter(rd1, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			doc = new Document();
			doc.Add(new Field("field1", "the quick brown fox jumps", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("field2", "the quick brown fox jumps", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("field4", "", Field.Store.NO, Field.Index.ANALYZED));
			iw1.AddDocument(doc);
			
			iw1.Close();
			RAMDirectory rd2 = new RAMDirectory();
			IndexWriter iw2 = new IndexWriter(rd2, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			doc = new Document();
			doc.Add(new Field("field0", "", Field.Store.NO, Field.Index.ANALYZED));
			doc.Add(new Field("field1", "the fox jumps over the lazy dog", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("field3", "the fox jumps over the lazy dog", Field.Store.YES, Field.Index.ANALYZED));
			iw2.AddDocument(doc);
			
			iw2.Close();
			
			this.ir1 = IndexReader.Open(rd1);
			this.ir2 = IndexReader.Open(rd2);
		}
		
		[TearDown]
		public override void  TearDown()
		{
			base.TearDown();
			
			ir1.Close();
			ir2.Close();
		}
		
		[Test]
		public virtual void  Test1()
		{
			ParallelReader pr = new ParallelReader();
			pr.Add(ir1);
			pr.Add(ir2);
			
			TermDocs td = pr.TermDocs();
			
			TermEnum te = pr.Terms();
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field1:brown", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field1:fox", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field1:jumps", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field1:quick", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field1:the", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field2:brown", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field2:fox", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field2:jumps", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field2:quick", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field2:the", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field3:dog", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field3:fox", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field3:jumps", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field3:lazy", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field3:over", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsTrue(te.Next());
			Assert.AreEqual("field3:the", te.Term().ToString());
			td.Seek(te.Term());
			Assert.IsTrue(td.Next());
			Assert.AreEqual(0, td.Doc());
			Assert.IsFalse(td.Next());
			Assert.IsFalse(te.Next());
		}
	}
}