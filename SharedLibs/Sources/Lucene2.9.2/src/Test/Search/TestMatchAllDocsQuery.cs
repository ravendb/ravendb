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

using Analyzer = Lucene.Net.Analysis.Analyzer;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using Version = Lucene.Net.Util.Version;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> Tests MatchAllDocsQuery.
	/// 
	/// </summary>
    [TestFixture]
	public class TestMatchAllDocsQuery:LuceneTestCase
	{
		public TestMatchAllDocsQuery()
		{
			InitBlock();
		}
		private void  InitBlock()
		{
			analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		}
		private Analyzer analyzer;
		
		[Test]
		public virtual void  TestQuery()
		{
			
			RAMDirectory dir = new RAMDirectory();
			IndexWriter iw = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			iw.SetMaxBufferedDocs(2); // force multi-segment
			AddDoc("one", iw, 1f);
			AddDoc("two", iw, 20f);
			AddDoc("three four", iw, 300f);
			iw.Close();
			
			IndexReader ir = IndexReader.Open(dir);
			IndexSearcher is_Renamed = new IndexSearcher(ir);
			ScoreDoc[] hits;
			
			// assert with norms scoring turned off
			
			hits = is_Renamed.Search(new MatchAllDocsQuery(), null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			Assert.AreEqual(ir.Document(hits[0].doc).Get("key"), "one");
			Assert.AreEqual(ir.Document(hits[1].doc).Get("key"), "two");
			Assert.AreEqual(ir.Document(hits[2].doc).Get("key"), "three four");
			
			// assert with norms scoring turned on
			
			MatchAllDocsQuery normsQuery = new MatchAllDocsQuery("key");
			hits = is_Renamed.Search(normsQuery, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			
			Assert.AreEqual(ir.Document(hits[0].doc).Get("key"), "three four");
			Assert.AreEqual(ir.Document(hits[1].doc).Get("key"), "two");
			Assert.AreEqual(ir.Document(hits[2].doc).Get("key"), "one");
			
			// change norm & retest
			ir.SetNorm(0, "key", 400f);
			normsQuery = new MatchAllDocsQuery("key");
			hits = is_Renamed.Search(normsQuery, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			
			Assert.AreEqual(ir.Document(hits[0].doc).Get("key"), "one");
			Assert.AreEqual(ir.Document(hits[1].doc).Get("key"), "three four");
			Assert.AreEqual(ir.Document(hits[2].doc).Get("key"), "two");
			
			// some artificial queries to trigger the use of skipTo():
			
			BooleanQuery bq = new BooleanQuery();
			bq.Add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
			bq.Add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
			hits = is_Renamed.Search(bq, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			
			bq = new BooleanQuery();
			bq.Add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
			bq.Add(new TermQuery(new Term("key", "three")), BooleanClause.Occur.MUST);
			hits = is_Renamed.Search(bq, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			// delete a document:
			is_Renamed.GetIndexReader().DeleteDocument(0);
			hits = is_Renamed.Search(new MatchAllDocsQuery(), null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length);
			
			// test parsable toString()
			QueryParser qp = new QueryParser("key", analyzer);
			hits = is_Renamed.Search(qp.Parse(new MatchAllDocsQuery().ToString()), null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length);
			
			// test parsable toString() with non default boost
			Query maq = new MatchAllDocsQuery();
			maq.SetBoost(2.3f);
			Query pq = qp.Parse(maq.ToString());
			hits = is_Renamed.Search(pq, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length);
			
			is_Renamed.Close();
			ir.Close();
			dir.Close();
		}
		
		[Test]
		public virtual void  TestEquals()
		{
			Query q1 = new MatchAllDocsQuery();
			Query q2 = new MatchAllDocsQuery();
			Assert.IsTrue(q1.Equals(q2));
			q1.SetBoost(1.5f);
			Assert.IsFalse(q1.Equals(q2));
		}
		
		private void  AddDoc(System.String text, IndexWriter iw, float boost)
		{
			Document doc = new Document();
			Field f = new Field("key", text, Field.Store.YES, Field.Index.ANALYZED);
			f.SetBoost(boost);
			doc.Add(f);
			iw.AddDocument(doc);
		}
	}
}