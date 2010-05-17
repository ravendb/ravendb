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
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using SpanNearQuery = Lucene.Net.Search.Spans.SpanNearQuery;
using SpanQuery = Lucene.Net.Search.Spans.SpanQuery;
using SpanTermQuery = Lucene.Net.Search.Spans.SpanTermQuery;

namespace Lucene.Net.Search
{
	
	
	/// <summary> TestExplanations subclass focusing on basic query types</summary>
    [TestFixture]
	public class TestSimpleExplanations:TestExplanations
	{
		
		// we focus on queries that don't rewrite to other queries.
		// if we get those covered well, then the ones that rewrite should
		// also be covered.
		
		
		/* simple term tests */
		
		[Test]
		public virtual void  TestT1()
		{
			Qtest("w1", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestT2()
		{
			Qtest("w1^1000", new int[]{0, 1, 2, 3});
		}
		
		/* MatchAllDocs */
		
		[Test]
		public virtual void  TestMA1()
		{
			Qtest(new MatchAllDocsQuery(), new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestMA2()
		{
			Query q = new MatchAllDocsQuery();
			q.SetBoost(1000);
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		
		/* some simple phrase tests */
		
		[Test]
		public virtual void  TestP1()
		{
			Qtest("\"w1 w2\"", new int[]{0});
		}
		[Test]
		public virtual void  TestP2()
		{
			Qtest("\"w1 w3\"", new int[]{1, 3});
		}
		[Test]
		public virtual void  TestP3()
		{
			Qtest("\"w1 w2\"~1", new int[]{0, 1, 2});
		}
		[Test]
		public virtual void  TestP4()
		{
			Qtest("\"w2 w3\"~1", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestP5()
		{
			Qtest("\"w3 w2\"~1", new int[]{1, 3});
		}
		[Test]
		public virtual void  TestP6()
		{
			Qtest("\"w3 w2\"~2", new int[]{0, 1, 3});
		}
        [Test]
        public virtual void TestP7()
		{
			Qtest("\"w3 w2\"~3", new int[]{0, 1, 2, 3});
		}
		
		/* some simple filtered query tests */
		
		[Test]
		public virtual void  TestFQ1()
		{
			Qtest(new FilteredQuery(qp.Parse("w1"), new ItemizedFilter(new int[]{0, 1, 2, 3})), new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestFQ2()
		{
			Qtest(new FilteredQuery(qp.Parse("w1"), new ItemizedFilter(new int[]{0, 2, 3})), new int[]{0, 2, 3});
		}
		[Test]
		public virtual void  TestFQ3()
		{
			Qtest(new FilteredQuery(qp.Parse("xx"), new ItemizedFilter(new int[]{1, 3})), new int[]{3});
		}
		[Test]
		public virtual void  TestFQ4()
		{
			Qtest(new FilteredQuery(qp.Parse("xx^1000"), new ItemizedFilter(new int[]{1, 3})), new int[]{3});
		}
		[Test]
		public virtual void  TestFQ6()
		{
			Query q = new FilteredQuery(qp.Parse("xx"), new ItemizedFilter(new int[]{1, 3}));
			q.SetBoost(1000);
			Qtest(q, new int[]{3});
		}
		
		/* ConstantScoreQueries */
		
		[Test]
		public virtual void  TestCSQ1()
		{
			Query q = new ConstantScoreQuery(new ItemizedFilter(new int[]{0, 1, 2, 3}));
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestCSQ2()
		{
			Query q = new ConstantScoreQuery(new ItemizedFilter(new int[]{1, 3}));
			Qtest(q, new int[]{1, 3});
		}
		[Test]
		public virtual void  TestCSQ3()
		{
			Query q = new ConstantScoreQuery(new ItemizedFilter(new int[]{0, 2}));
			q.SetBoost(1000);
			Qtest(q, new int[]{0, 2});
		}
		
		/* DisjunctionMaxQuery */

        [Test]
        public virtual void TestDMQ1()
		{
			DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.0f);
			q.Add(qp.Parse("w1"));
			q.Add(qp.Parse("w5"));
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestDMQ2()
		{
			DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
			q.Add(qp.Parse("w1"));
			q.Add(qp.Parse("w5"));
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestDMQ3()
		{
			DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
			q.Add(qp.Parse("QQ"));
			q.Add(qp.Parse("w5"));
			Qtest(q, new int[]{0});
		}
		[Test]
		public virtual void  TestDMQ4()
		{
			DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
			q.Add(qp.Parse("QQ"));
			q.Add(qp.Parse("xx"));
			Qtest(q, new int[]{2, 3});
		}
		[Test]
		public virtual void  TestDMQ5()
		{
			DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
			q.Add(qp.Parse("yy -QQ"));
			q.Add(qp.Parse("xx"));
			Qtest(q, new int[]{2, 3});
		}
        [Test]
        public virtual void TestDMQ6()
		{
			DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
			q.Add(qp.Parse("-yy w3"));
			q.Add(qp.Parse("xx"));
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestDMQ7()
		{
			DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
			q.Add(qp.Parse("-yy w3"));
			q.Add(qp.Parse("w2"));
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestDMQ8()
		{
			DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
			q.Add(qp.Parse("yy w5^100"));
			q.Add(qp.Parse("xx^100000"));
			Qtest(q, new int[]{0, 2, 3});
		}
		[Test]
		public virtual void  TestDMQ9()
		{
			DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
			q.Add(qp.Parse("yy w5^100"));
			q.Add(qp.Parse("xx^0"));
			Qtest(q, new int[]{0, 2, 3});
		}
		
		/* MultiPhraseQuery */
		
		[Test]
		public virtual void  TestMPQ1()
		{
			MultiPhraseQuery q = new MultiPhraseQuery();
			q.Add(Ta(new System.String[]{"w1"}));
			q.Add(Ta(new System.String[]{"w2", "w3", "xx"}));
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestMPQ2()
		{
			MultiPhraseQuery q = new MultiPhraseQuery();
			q.Add(Ta(new System.String[]{"w1"}));
			q.Add(Ta(new System.String[]{"w2", "w3"}));
			Qtest(q, new int[]{0, 1, 3});
		}
		[Test]
		public virtual void  TestMPQ3()
		{
			MultiPhraseQuery q = new MultiPhraseQuery();
			q.Add(Ta(new System.String[]{"w1", "xx"}));
			q.Add(Ta(new System.String[]{"w2", "w3"}));
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestMPQ4()
		{
			MultiPhraseQuery q = new MultiPhraseQuery();
			q.Add(Ta(new System.String[]{"w1"}));
			q.Add(Ta(new System.String[]{"w2"}));
			Qtest(q, new int[]{0});
		}
		[Test]
		public virtual void  TestMPQ5()
		{
			MultiPhraseQuery q = new MultiPhraseQuery();
			q.Add(Ta(new System.String[]{"w1"}));
			q.Add(Ta(new System.String[]{"w2"}));
			q.SetSlop(1);
			Qtest(q, new int[]{0, 1, 2});
		}
		[Test]
		public virtual void  TestMPQ6()
		{
			MultiPhraseQuery q = new MultiPhraseQuery();
			q.Add(Ta(new System.String[]{"w1", "w3"}));
			q.Add(Ta(new System.String[]{"w2"}));
			q.SetSlop(1);
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		
		/* some simple tests of boolean queries containing term queries */
		
		[Test]
		public virtual void  TestBQ1()
		{
			Qtest("+w1 +w2", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ2()
		{
			Qtest("+yy +w3", new int[]{2, 3});
		}
		[Test]
		public virtual void  TestBQ3()
		{
			Qtest("yy +w3", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ4()
		{
			Qtest("w1 (-xx w2)", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ5()
		{
			Qtest("w1 (+qq w2)", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ6()
		{
			Qtest("w1 -(-qq w5)", new int[]{1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ7()
		{
			Qtest("+w1 +(qq (xx -w2) (+w3 +w4))", new int[]{0});
		}
		[Test]
		public virtual void  TestBQ8()
		{
			Qtest("+w1 (qq (xx -w2) (+w3 +w4))", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ9()
		{
			Qtest("+w1 (qq (-xx w2) -(+w3 +w4))", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ10()
		{
			Qtest("+w1 +(qq (-xx w2) -(+w3 +w4))", new int[]{1});
		}
		[Test]
		public virtual void  TestBQ11()
		{
			Qtest("w1 w2^1000.0", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ14()
		{
			BooleanQuery q = new BooleanQuery(true);
			q.Add(qp.Parse("QQQQQ"), BooleanClause.Occur.SHOULD);
			q.Add(qp.Parse("w1"), BooleanClause.Occur.SHOULD);
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ15()
		{
			BooleanQuery q = new BooleanQuery(true);
			q.Add(qp.Parse("QQQQQ"), BooleanClause.Occur.MUST_NOT);
			q.Add(qp.Parse("w1"), BooleanClause.Occur.SHOULD);
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ16()
		{
			BooleanQuery q = new BooleanQuery(true);
			q.Add(qp.Parse("QQQQQ"), BooleanClause.Occur.SHOULD);
			q.Add(qp.Parse("w1 -xx"), BooleanClause.Occur.SHOULD);
			Qtest(q, new int[]{0, 1});
		}
        [Test]
        public virtual void TestBQ17()
		{
			BooleanQuery q = new BooleanQuery(true);
			q.Add(qp.Parse("w2"), BooleanClause.Occur.SHOULD);
			q.Add(qp.Parse("w1 -xx"), BooleanClause.Occur.SHOULD);
			Qtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ19()
		{
			Qtest("-yy w3", new int[]{0, 1});
		}
		
		[Test]
		public virtual void  TestBQ20()
		{
			BooleanQuery q = new BooleanQuery();
			q.SetMinimumNumberShouldMatch(2);
			q.Add(qp.Parse("QQQQQ"), BooleanClause.Occur.SHOULD);
			q.Add(qp.Parse("yy"), BooleanClause.Occur.SHOULD);
			q.Add(qp.Parse("zz"), BooleanClause.Occur.SHOULD);
			q.Add(qp.Parse("w5"), BooleanClause.Occur.SHOULD);
			q.Add(qp.Parse("w4"), BooleanClause.Occur.SHOULD);
			
			Qtest(q, new int[]{0, 3});
		}
		
		
		[Test]
		public virtual void  TestTermQueryMultiSearcherExplain()
		{
			// creating two directories for indices
			Directory indexStoreA = new MockRAMDirectory();
			Directory indexStoreB = new MockRAMDirectory();
			
			Document lDoc = new Document();
			lDoc.Add(new Field("handle", "1 2", Field.Store.YES, Field.Index.ANALYZED));
			Document lDoc2 = new Document();
			lDoc2.Add(new Field("handle", "1 2", Field.Store.YES, Field.Index.ANALYZED));
			Document lDoc3 = new Document();
			lDoc3.Add(new Field("handle", "1 2", Field.Store.YES, Field.Index.ANALYZED));
			
			IndexWriter writerA = new IndexWriter(indexStoreA, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			IndexWriter writerB = new IndexWriter(indexStoreB, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			writerA.AddDocument(lDoc);
			writerA.AddDocument(lDoc2);
			writerA.Optimize();
			writerA.Close();
			
			writerB.AddDocument(lDoc3);
			writerB.Close();
			
			QueryParser parser = new QueryParser("fulltext", new StandardAnalyzer());
			Query query = parser.Parse("handle:1");
			
			Searcher[] searchers = new Searcher[2];
			searchers[0] = new IndexSearcher(indexStoreB);
			searchers[1] = new IndexSearcher(indexStoreA);
			Searcher mSearcher = new MultiSearcher(searchers);
			ScoreDoc[] hits = mSearcher.Search(query, null, 1000).scoreDocs;
			
			Assert.AreEqual(3, hits.Length);
			
			Explanation explain = mSearcher.Explain(query, hits[0].doc);
			System.String exp = explain.ToString(0);
			Assert.IsTrue(exp.IndexOf("maxDocs=3") > - 1, exp);
			Assert.IsTrue(exp.IndexOf("docFreq=3") > - 1, exp);
			
			query = parser.Parse("handle:\"1 2\"");
			hits = mSearcher.Search(query, null, 1000).scoreDocs;
			
			Assert.AreEqual(3, hits.Length);
			
			explain = mSearcher.Explain(query, hits[0].doc);
			exp = explain.ToString(0);
			Assert.IsTrue(exp.IndexOf("1=3") > - 1, exp);
			Assert.IsTrue(exp.IndexOf("2=3") > - 1, exp);
			
			query = new SpanNearQuery(new SpanQuery[]{new SpanTermQuery(new Term("handle", "1")), new SpanTermQuery(new Term("handle", "2"))}, 0, true);
			hits = mSearcher.Search(query, null, 1000).scoreDocs;
			
			Assert.AreEqual(3, hits.Length);
			
			explain = mSearcher.Explain(query, hits[0].doc);
			exp = explain.ToString(0);
			Assert.IsTrue(exp.IndexOf("1=3") > - 1, exp);
			Assert.IsTrue(exp.IndexOf("2=3") > - 1, exp);
			mSearcher.Close();
		}
	}
}