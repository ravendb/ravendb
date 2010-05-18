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
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using DocIdBitSet = Lucene.Net.Util.DocIdBitSet;
using Occur = Lucene.Net.Search.BooleanClause.Occur;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> FilteredQuery JUnit tests.
	/// 
	/// <p/>Created: Apr 21, 2004 1:21:46 PM
	/// 
	/// 
	/// </summary>
	/// <version>  $Id: TestFilteredQuery.java 807821 2009-08-25 21:55:49Z mikemccand $
	/// </version>
	/// <since>   1.4
	/// </since>
    [TestFixture]
	public class TestFilteredQuery:LuceneTestCase
	{
		[Serializable]
		private class AnonymousClassFilter:Filter
		{
			public override DocIdSet GetDocIdSet(IndexReader reader)
			{
				System.Collections.BitArray bitset = new System.Collections.BitArray((5 % 64 == 0?5 / 64:5 / 64 + 1) * 64);
				bitset.Set(1, true);
				bitset.Set(3, true);
				return new DocIdBitSet(bitset);
			}
		}
		[Serializable]
		private class AnonymousClassFilter1:Filter
		{
			public override DocIdSet GetDocIdSet(IndexReader reader)
			{
				System.Collections.BitArray bitset = new System.Collections.BitArray((5 % 64 == 0?5 / 64:5 / 64 + 1) * 64);
				for (int i = 0; i < 5; i++) bitset.Set(i, true);
				return new DocIdBitSet(bitset);
			}
		}
		
		private IndexSearcher searcher;
		private RAMDirectory directory;
		private Query query;
		private Filter filter;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			Document doc = new Document();
			doc.Add(new Field("field", "one two three four five", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("sorter", "b", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			doc = new Document();
			doc.Add(new Field("field", "one two three four", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("sorter", "d", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			doc = new Document();
			doc.Add(new Field("field", "one two three y", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("sorter", "a", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			doc = new Document();
			doc.Add(new Field("field", "one two x", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("sorter", "c", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			writer.Optimize();
			writer.Close();
			
			searcher = new IndexSearcher(directory);
			query = new TermQuery(new Term("field", "three"));
			filter = NewStaticFilterB();
		}
		
		// must be static for serialization tests
		private static Filter NewStaticFilterB()
		{
			return new AnonymousClassFilter();
		}
		
		[TearDown]
		public override void  TearDown()
		{
			searcher.Close();
			directory.Close();
			base.TearDown();
		}
		
		[Test]
		public virtual void  TestFilteredQuery_Renamed()
		{
			Query filteredquery = new FilteredQuery(query, filter);
			ScoreDoc[] hits = searcher.Search(filteredquery, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(1, hits[0].doc);
			QueryUtils.Check(filteredquery, searcher);
			
			hits = searcher.Search(filteredquery, null, 1000, new Sort("sorter")).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(1, hits[0].doc);
			
			filteredquery = new FilteredQuery(new TermQuery(new Term("field", "one")), filter);
			hits = searcher.Search(filteredquery, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length);
			QueryUtils.Check(filteredquery, searcher);
			
			filteredquery = new FilteredQuery(new TermQuery(new Term("field", "x")), filter);
			hits = searcher.Search(filteredquery, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(3, hits[0].doc);
			QueryUtils.Check(filteredquery, searcher);
			
			filteredquery = new FilteredQuery(new TermQuery(new Term("field", "y")), filter);
			hits = searcher.Search(filteredquery, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			QueryUtils.Check(filteredquery, searcher);
			
			// test boost
			Filter f = NewStaticFilterA();
			
			float boost = 2.5f;
			BooleanQuery bq1 = new BooleanQuery();
			TermQuery tq = new TermQuery(new Term("field", "one"));
			tq.SetBoost(boost);
			bq1.Add(tq, Occur.MUST);
			bq1.Add(new TermQuery(new Term("field", "five")), Occur.MUST);
			
			BooleanQuery bq2 = new BooleanQuery();
			tq = new TermQuery(new Term("field", "one"));
			filteredquery = new FilteredQuery(tq, f);
			filteredquery.SetBoost(boost);
			bq2.Add(filteredquery, Occur.MUST);
			bq2.Add(new TermQuery(new Term("field", "five")), Occur.MUST);
			AssertScoreEquals(bq1, bq2);
			
			Assert.AreEqual(boost, filteredquery.GetBoost(), 0);
			Assert.AreEqual(1.0f, tq.GetBoost(), 0); // the boost value of the underlying query shouldn't have changed 
		}
		
		// must be static for serialization tests 
		private static Filter NewStaticFilterA()
		{
			return new AnonymousClassFilter1();
		}
		
		/// <summary> Tests whether the scores of the two queries are the same.</summary>
		public virtual void  AssertScoreEquals(Query q1, Query q2)
		{
			ScoreDoc[] hits1 = searcher.Search(q1, null, 1000).scoreDocs;
			ScoreDoc[] hits2 = searcher.Search(q2, null, 1000).scoreDocs;
			
			Assert.AreEqual(hits1.Length, hits2.Length);
			
			for (int i = 0; i < hits1.Length; i++)
			{
				Assert.AreEqual(hits1[i].score, hits2[i].score, 0.0000001f);
			}
		}
		
		/// <summary> This tests FilteredQuery's rewrite correctness</summary>
		[Test]
		public virtual void  TestRangeQuery()
		{
			TermRangeQuery rq = new TermRangeQuery("sorter", "b", "d", true, true);
			
			Query filteredquery = new FilteredQuery(rq, filter);
			ScoreDoc[] hits = searcher.Search(filteredquery, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length);
			QueryUtils.Check(filteredquery, searcher);
		}
		
		[Test]
		public virtual void  TestBoolean()
		{
			BooleanQuery bq = new BooleanQuery();
			Query query = new FilteredQuery(new MatchAllDocsQuery(), new SingleDocTestFilter(0));
			bq.Add(query, BooleanClause.Occur.MUST);
			query = new FilteredQuery(new MatchAllDocsQuery(), new SingleDocTestFilter(1));
			bq.Add(query, BooleanClause.Occur.MUST);
			ScoreDoc[] hits = searcher.Search(bq, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			QueryUtils.Check(query, searcher);
		}
		
		// Make sure BooleanQuery, which does out-of-order
		// scoring, inside FilteredQuery, works
		[Test]
		public virtual void  TestBoolean2()
		{
			BooleanQuery bq = new BooleanQuery();
			Query query = new FilteredQuery(bq, new SingleDocTestFilter(0));
			bq.Add(new TermQuery(new Term("field", "one")), BooleanClause.Occur.SHOULD);
			bq.Add(new TermQuery(new Term("field", "two")), BooleanClause.Occur.SHOULD);
			ScoreDoc[] hits = searcher.Search(query, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			QueryUtils.Check(query, searcher);
		}
	}
}