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
using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestMultiTermConstantScore:BaseTestRangeFilter
	{
		private class AnonymousClassCollector:Collector
		{
			public AnonymousClassCollector(TestMultiTermConstantScore enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestMultiTermConstantScore enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestMultiTermConstantScore enclosingInstance;
			public TestMultiTermConstantScore Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private int base_Renamed = 0;
			private Scorer scorer;
			public override void  SetScorer(Scorer scorer)
			{
				this.scorer = scorer;
			}
			public override void  Collect(int doc)
			{
				Enclosing_Instance.AssertEquals("score for doc " + (doc + base_Renamed) + " was not correct", 1.0f, scorer.Score());
			}
			public override void  SetNextReader(IndexReader reader, int docBase)
			{
				base_Renamed = docBase;
			}
			public override bool AcceptsDocsOutOfOrder()
			{
				return true;
			}
		}
		
		/// <summary>threshold for comparing floats </summary>
		public const float SCORE_COMP_THRESH = 1e-6f;
		
		public TestMultiTermConstantScore(System.String name):base(name)
		{
		}
		
		public TestMultiTermConstantScore():base()
		{
		}
		
		internal Directory small;
		
		internal virtual void  AssertEquals(System.String m, float e, float a)
		{
			Assert.AreEqual(e, a, SCORE_COMP_THRESH, m);
		}
		
		static public void  AssertEquals(System.String m, int e, int a)
		{
			Assert.AreEqual(e, a, m);
		}
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			
			System.String[] data = new System.String[]{"A 1 2 3 4 5 6", "Z       4 5 6", null, "B   2   4 5 6", "Y     3   5 6", null, "C     3     6", "X       4 5 6"};
			
			small = new RAMDirectory();
			IndexWriter writer = new IndexWriter(small, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			for (int i = 0; i < data.Length; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("id", System.Convert.ToString(i), Field.Store.YES, Field.Index.NOT_ANALYZED)); // Field.Keyword("id",String.valueOf(i)));
				doc.Add(new Field("all", "all", Field.Store.YES, Field.Index.NOT_ANALYZED)); // Field.Keyword("all","all"));
				if (null != data[i])
				{
					doc.Add(new Field("data", data[i], Field.Store.YES, Field.Index.ANALYZED)); // Field.Text("data",data[i]));
				}
				writer.AddDocument(doc);
			}
			
			writer.Optimize();
			writer.Close();
		}
		
		/// <summary>macro for readability </summary>
		public static Query Csrq(System.String f, System.String l, System.String h, bool il, bool ih)
		{
			TermRangeQuery query = new TermRangeQuery(f, l, h, il, ih);
			query.SetRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
			return query;
		}
		
		public static Query Csrq(System.String f, System.String l, System.String h, bool il, bool ih, MultiTermQuery.RewriteMethod method)
		{
			TermRangeQuery query = new TermRangeQuery(f, l, h, il, ih);
			query.SetRewriteMethod(method);
			return query;
		}
		
		/// <summary>macro for readability </summary>
		public static Query Csrq(System.String f, System.String l, System.String h, bool il, bool ih, System.Globalization.CompareInfo c)
		{
			TermRangeQuery query = new TermRangeQuery(f, l, h, il, ih, c);
			query.SetRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
			return query;
		}
		
		/// <summary>macro for readability </summary>
		public static Query Cspq(Term prefix)
		{
			PrefixQuery query = new PrefixQuery(prefix);
			query.SetRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
			return query;
		}
		
		/// <summary>macro for readability </summary>
		public static Query Cswcq(Term wild)
		{
			WildcardQuery query = new WildcardQuery(wild);
			query.SetRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
			return query;
		}
		
        [Test]
		public virtual void  TestBasics()
		{
			QueryUtils.Check(Csrq("data", "1", "6", T, T));
			QueryUtils.Check(Csrq("data", "A", "Z", T, T));
			QueryUtils.CheckUnequal(Csrq("data", "1", "6", T, T), Csrq("data", "A", "Z", T, T));
			
			QueryUtils.Check(Cspq(new Term("data", "p*u?")));
			QueryUtils.CheckUnequal(Cspq(new Term("data", "pre*")), Cspq(new Term("data", "pres*")));
			
			QueryUtils.Check(Cswcq(new Term("data", "p")));
			QueryUtils.CheckUnequal(Cswcq(new Term("data", "pre*n?t")), Cswcq(new Term("data", "pr*t?j")));
		}
		
        [Test]
		public virtual void  TestBasicsRngCollating()
		{
			System.Globalization.CompareInfo c = new System.Globalization.CultureInfo("en").CompareInfo;
			QueryUtils.Check(Csrq("data", "1", "6", T, T, c));
			QueryUtils.Check(Csrq("data", "A", "Z", T, T, c));
			QueryUtils.CheckUnequal(Csrq("data", "1", "6", T, T, c), Csrq("data", "A", "Z", T, T, c));
		}
		
        [Test]
		public virtual void  TestEqualScores()
		{
			// NOTE: uses index build in *this* setUp
			
			IndexReader reader = IndexReader.Open(small);
			IndexSearcher search = new IndexSearcher(reader);
			
			ScoreDoc[] result;
			
			// some hits match more terms then others, score should be the same
			
			result = search.Search(Csrq("data", "1", "6", T, T), null, 1000).scoreDocs;
			int numHits = result.Length;
			AssertEquals("wrong number of results", 6, numHits);
			float score = result[0].score;
			for (int i = 1; i < numHits; i++)
			{
				AssertEquals("score for " + i + " was not the same", score, result[i].score);
			}
			
			result = search.Search(Csrq("data", "1", "6", T, T, MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE), null, 1000).scoreDocs;
			numHits = result.Length;
			AssertEquals("wrong number of results", 6, numHits);
			for (int i = 0; i < numHits; i++)
			{
				AssertEquals("score for " + i + " was not the same", score, result[i].score);
			}
		}
		
        [Test]
		public virtual void  TestBoost()
		{
			// NOTE: uses index build in *this* setUp
			
			IndexReader reader = IndexReader.Open(small);
			IndexSearcher search = new IndexSearcher(reader);
			
			// test for correct application of query normalization
			// must use a non score normalizing method for this.
			Query q = Csrq("data", "1", "6", T, T);
			q.SetBoost(100);
			search.Search(q, null, new AnonymousClassCollector(this));
			
			//
			// Ensure that boosting works to score one clause of a query higher
			// than another.
			//
			Query q1 = Csrq("data", "A", "A", T, T); // matches document #0
			q1.SetBoost(.1f);
			Query q2 = Csrq("data", "Z", "Z", T, T); // matches document #1
			BooleanQuery bq = new BooleanQuery(true);
			bq.Add(q1, BooleanClause.Occur.SHOULD);
			bq.Add(q2, BooleanClause.Occur.SHOULD);
			
			ScoreDoc[] hits = search.Search(bq, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits[0].doc);
			Assert.AreEqual(0, hits[1].doc);
			Assert.IsTrue(hits[0].score > hits[1].score);
			
			q1 = Csrq("data", "A", "A", T, T, MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE); // matches document #0
			q1.SetBoost(.1f);
			q2 = Csrq("data", "Z", "Z", T, T, MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE); // matches document #1
			bq = new BooleanQuery(true);
			bq.Add(q1, BooleanClause.Occur.SHOULD);
			bq.Add(q2, BooleanClause.Occur.SHOULD);
			
			hits = search.Search(bq, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits[0].doc);
			Assert.AreEqual(0, hits[1].doc);
			Assert.IsTrue(hits[0].score > hits[1].score);
			
			q1 = Csrq("data", "A", "A", T, T); // matches document #0
			q1.SetBoost(10f);
			q2 = Csrq("data", "Z", "Z", T, T); // matches document #1
			bq = new BooleanQuery(true);
			bq.Add(q1, BooleanClause.Occur.SHOULD);
			bq.Add(q2, BooleanClause.Occur.SHOULD);
			
			hits = search.Search(bq, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits[0].doc);
			Assert.AreEqual(1, hits[1].doc);
			Assert.IsTrue(hits[0].score > hits[1].score);
		}
		
        [Test]
		public virtual void  TestBooleanOrderUnAffected()
		{
			// NOTE: uses index build in *this* setUp
			
			IndexReader reader = IndexReader.Open(small);
			IndexSearcher search = new IndexSearcher(reader);
			
			// first do a regular TermRangeQuery which uses term expansion so
			// docs with more terms in range get higher scores
			
			Query rq = new TermRangeQuery("data", "1", "4", T, T);
			
			ScoreDoc[] expected = search.Search(rq, null, 1000).scoreDocs;
			int numHits = expected.Length;
			
			// now do a boolean where which also contains a
			// ConstantScoreRangeQuery and make sure hte order is the same
			
			BooleanQuery q = new BooleanQuery();
			q.Add(rq, BooleanClause.Occur.MUST); // T, F);
			q.Add(Csrq("data", "1", "6", T, T), BooleanClause.Occur.MUST); // T, F);
			
			ScoreDoc[] actual = search.Search(q, null, 1000).scoreDocs;
			
			AssertEquals("wrong numebr of hits", numHits, actual.Length);
			for (int i = 0; i < numHits; i++)
			{
				AssertEquals("mismatch in docid for hit#" + i, expected[i].doc, actual[i].doc);
			}
		}
		
        [Test]
		public virtual void  TestRangeQueryId()
		{
			// NOTE: uses index build in *super* setUp
			
			IndexReader reader = IndexReader.Open(signedIndex.index);
			IndexSearcher search = new IndexSearcher(reader);
			
			int medId = ((maxId - minId) / 2);
			
			System.String minIP = Pad(minId);
			System.String maxIP = Pad(maxId);
			System.String medIP = Pad(medId);
			
			int numDocs = reader.NumDocs();
			
			AssertEquals("num of docs", numDocs, 1 + maxId - minId);
			
			ScoreDoc[] result;
			
			// test id, bounded on both ends
			
			result = search.Search(Csrq("id", minIP, maxIP, T, T), null, numDocs).scoreDocs;
			AssertEquals("find all", numDocs, result.Length);
			
			result = search.Search(Csrq("id", minIP, maxIP, T, T, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("find all", numDocs, result.Length);
			
			result = search.Search(Csrq("id", minIP, maxIP, T, F), null, numDocs).scoreDocs;
			AssertEquals("all but last", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("id", minIP, maxIP, T, F, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("all but last", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("id", minIP, maxIP, F, T), null, numDocs).scoreDocs;
			AssertEquals("all but first", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("id", minIP, maxIP, F, T, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("all but first", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("id", minIP, maxIP, F, F), null, numDocs).scoreDocs;
			AssertEquals("all but ends", numDocs - 2, result.Length);
			
			result = search.Search(Csrq("id", minIP, maxIP, F, F, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("all but ends", numDocs - 2, result.Length);
			
			result = search.Search(Csrq("id", medIP, maxIP, T, T), null, numDocs).scoreDocs;
			AssertEquals("med and up", 1 + maxId - medId, result.Length);
			
			result = search.Search(Csrq("id", medIP, maxIP, T, T, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("med and up", 1 + maxId - medId, result.Length);
			
			result = search.Search(Csrq("id", minIP, medIP, T, T), null, numDocs).scoreDocs;
			AssertEquals("up to med", 1 + medId - minId, result.Length);
			
			result = search.Search(Csrq("id", minIP, medIP, T, T, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("up to med", 1 + medId - minId, result.Length);
			
			// unbounded id
			
			result = search.Search(Csrq("id", minIP, null, T, F), null, numDocs).scoreDocs;
			AssertEquals("min and up", numDocs, result.Length);
			
			result = search.Search(Csrq("id", null, maxIP, F, T), null, numDocs).scoreDocs;
			AssertEquals("max and down", numDocs, result.Length);
			
			result = search.Search(Csrq("id", minIP, null, F, F), null, numDocs).scoreDocs;
			AssertEquals("not min, but up", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("id", null, maxIP, F, F), null, numDocs).scoreDocs;
			AssertEquals("not max, but down", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("id", medIP, maxIP, T, F), null, numDocs).scoreDocs;
			AssertEquals("med and up, not max", maxId - medId, result.Length);
			
			result = search.Search(Csrq("id", minIP, medIP, F, T), null, numDocs).scoreDocs;
			AssertEquals("not min, up to med", medId - minId, result.Length);
			
			// very small sets
			
			result = search.Search(Csrq("id", minIP, minIP, F, F), null, numDocs).scoreDocs;
			AssertEquals("min,min,F,F", 0, result.Length);
			
			result = search.Search(Csrq("id", minIP, minIP, F, F, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("min,min,F,F", 0, result.Length);
			
			result = search.Search(Csrq("id", medIP, medIP, F, F), null, numDocs).scoreDocs;
			AssertEquals("med,med,F,F", 0, result.Length);
			
			result = search.Search(Csrq("id", medIP, medIP, F, F, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("med,med,F,F", 0, result.Length);
			
			result = search.Search(Csrq("id", maxIP, maxIP, F, F), null, numDocs).scoreDocs;
			AssertEquals("max,max,F,F", 0, result.Length);
			
			result = search.Search(Csrq("id", maxIP, maxIP, F, F, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("max,max,F,F", 0, result.Length);
			
			result = search.Search(Csrq("id", minIP, minIP, T, T), null, numDocs).scoreDocs;
			AssertEquals("min,min,T,T", 1, result.Length);
			
			result = search.Search(Csrq("id", minIP, minIP, T, T, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("min,min,T,T", 1, result.Length);
			
			result = search.Search(Csrq("id", null, minIP, F, T), null, numDocs).scoreDocs;
			AssertEquals("nul,min,F,T", 1, result.Length);
			
			result = search.Search(Csrq("id", null, minIP, F, T, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("nul,min,F,T", 1, result.Length);
			
			result = search.Search(Csrq("id", maxIP, maxIP, T, T), null, numDocs).scoreDocs;
			AssertEquals("max,max,T,T", 1, result.Length);
			
			result = search.Search(Csrq("id", maxIP, maxIP, T, T, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("max,max,T,T", 1, result.Length);
			
			result = search.Search(Csrq("id", maxIP, null, T, F), null, numDocs).scoreDocs;
			AssertEquals("max,nul,T,T", 1, result.Length);
			
			result = search.Search(Csrq("id", maxIP, null, T, F, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("max,nul,T,T", 1, result.Length);
			
			result = search.Search(Csrq("id", medIP, medIP, T, T), null, numDocs).scoreDocs;
			AssertEquals("med,med,T,T", 1, result.Length);
			
			result = search.Search(Csrq("id", medIP, medIP, T, T, MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT), null, numDocs).scoreDocs;
			AssertEquals("med,med,T,T", 1, result.Length);
		}
		
        [Test]
		public virtual void  TestRangeQueryIdCollating()
		{
			// NOTE: uses index build in *super* setUp
			
			IndexReader reader = IndexReader.Open(signedIndex.index);
			IndexSearcher search = new IndexSearcher(reader);
			
			int medId = ((maxId - minId) / 2);
			
			System.String minIP = Pad(minId);
			System.String maxIP = Pad(maxId);
			System.String medIP = Pad(medId);
			
			int numDocs = reader.NumDocs();
			
			AssertEquals("num of docs", numDocs, 1 + maxId - minId);
			
			ScoreDoc[] result;
			
			System.Globalization.CompareInfo c = new System.Globalization.CultureInfo("en").CompareInfo;
			
			// test id, bounded on both ends
			
			result = search.Search(Csrq("id", minIP, maxIP, T, T, c), null, numDocs).scoreDocs;
			AssertEquals("find all", numDocs, result.Length);
			
			result = search.Search(Csrq("id", minIP, maxIP, T, F, c), null, numDocs).scoreDocs;
			AssertEquals("all but last", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("id", minIP, maxIP, F, T, c), null, numDocs).scoreDocs;
			AssertEquals("all but first", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("id", minIP, maxIP, F, F, c), null, numDocs).scoreDocs;
			AssertEquals("all but ends", numDocs - 2, result.Length);
			
			result = search.Search(Csrq("id", medIP, maxIP, T, T, c), null, numDocs).scoreDocs;
			AssertEquals("med and up", 1 + maxId - medId, result.Length);
			
			result = search.Search(Csrq("id", minIP, medIP, T, T, c), null, numDocs).scoreDocs;
			AssertEquals("up to med", 1 + medId - minId, result.Length);
			
			// unbounded id
			
			result = search.Search(Csrq("id", minIP, null, T, F, c), null, numDocs).scoreDocs;
			AssertEquals("min and up", numDocs, result.Length);
			
			result = search.Search(Csrq("id", null, maxIP, F, T, c), null, numDocs).scoreDocs;
			AssertEquals("max and down", numDocs, result.Length);
			
			result = search.Search(Csrq("id", minIP, null, F, F, c), null, numDocs).scoreDocs;
			AssertEquals("not min, but up", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("id", null, maxIP, F, F, c), null, numDocs).scoreDocs;
			AssertEquals("not max, but down", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("id", medIP, maxIP, T, F, c), null, numDocs).scoreDocs;
			AssertEquals("med and up, not max", maxId - medId, result.Length);
			
			result = search.Search(Csrq("id", minIP, medIP, F, T, c), null, numDocs).scoreDocs;
			AssertEquals("not min, up to med", medId - minId, result.Length);
			
			// very small sets
			
			result = search.Search(Csrq("id", minIP, minIP, F, F, c), null, numDocs).scoreDocs;
			AssertEquals("min,min,F,F,c", 0, result.Length);
			result = search.Search(Csrq("id", medIP, medIP, F, F, c), null, numDocs).scoreDocs;
			AssertEquals("med,med,F,F,c", 0, result.Length);
			result = search.Search(Csrq("id", maxIP, maxIP, F, F, c), null, numDocs).scoreDocs;
			AssertEquals("max,max,F,F,c", 0, result.Length);
			
			result = search.Search(Csrq("id", minIP, minIP, T, T, c), null, numDocs).scoreDocs;
			AssertEquals("min,min,T,T,c", 1, result.Length);
			result = search.Search(Csrq("id", null, minIP, F, T, c), null, numDocs).scoreDocs;
			AssertEquals("nul,min,F,T,c", 1, result.Length);
			
			result = search.Search(Csrq("id", maxIP, maxIP, T, T, c), null, numDocs).scoreDocs;
			AssertEquals("max,max,T,T,c", 1, result.Length);
			result = search.Search(Csrq("id", maxIP, null, T, F, c), null, numDocs).scoreDocs;
			AssertEquals("max,nul,T,T,c", 1, result.Length);
			
			result = search.Search(Csrq("id", medIP, medIP, T, T, c), null, numDocs).scoreDocs;
			AssertEquals("med,med,T,T,c", 1, result.Length);
		}
		
        [Test]
		public virtual void  TestRangeQueryRand()
		{
			// NOTE: uses index build in *super* setUp
			
			IndexReader reader = IndexReader.Open(signedIndex.index);
			IndexSearcher search = new IndexSearcher(reader);
			
			System.String minRP = Pad(signedIndex.minR);
			System.String maxRP = Pad(signedIndex.maxR);
			
			int numDocs = reader.NumDocs();
			
			AssertEquals("num of docs", numDocs, 1 + maxId - minId);
			
			ScoreDoc[] result;
			
			// test extremes, bounded on both ends
			
			result = search.Search(Csrq("rand", minRP, maxRP, T, T), null, numDocs).scoreDocs;
			AssertEquals("find all", numDocs, result.Length);
			
			result = search.Search(Csrq("rand", minRP, maxRP, T, F), null, numDocs).scoreDocs;
			AssertEquals("all but biggest", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("rand", minRP, maxRP, F, T), null, numDocs).scoreDocs;
			AssertEquals("all but smallest", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("rand", minRP, maxRP, F, F), null, numDocs).scoreDocs;
			AssertEquals("all but extremes", numDocs - 2, result.Length);
			
			// unbounded
			
			result = search.Search(Csrq("rand", minRP, null, T, F), null, numDocs).scoreDocs;
			AssertEquals("smallest and up", numDocs, result.Length);
			
			result = search.Search(Csrq("rand", null, maxRP, F, T), null, numDocs).scoreDocs;
			AssertEquals("biggest and down", numDocs, result.Length);
			
			result = search.Search(Csrq("rand", minRP, null, F, F), null, numDocs).scoreDocs;
			AssertEquals("not smallest, but up", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("rand", null, maxRP, F, F), null, numDocs).scoreDocs;
			AssertEquals("not biggest, but down", numDocs - 1, result.Length);
			
			// very small sets
			
			result = search.Search(Csrq("rand", minRP, minRP, F, F), null, numDocs).scoreDocs;
			AssertEquals("min,min,F,F", 0, result.Length);
			result = search.Search(Csrq("rand", maxRP, maxRP, F, F), null, numDocs).scoreDocs;
			AssertEquals("max,max,F,F", 0, result.Length);
			
			result = search.Search(Csrq("rand", minRP, minRP, T, T), null, numDocs).scoreDocs;
			AssertEquals("min,min,T,T", 1, result.Length);
			result = search.Search(Csrq("rand", null, minRP, F, T), null, numDocs).scoreDocs;
			AssertEquals("nul,min,F,T", 1, result.Length);
			
			result = search.Search(Csrq("rand", maxRP, maxRP, T, T), null, numDocs).scoreDocs;
			AssertEquals("max,max,T,T", 1, result.Length);
			result = search.Search(Csrq("rand", maxRP, null, T, F), null, numDocs).scoreDocs;
			AssertEquals("max,nul,T,T", 1, result.Length);
		}
		
        [Test]
		public virtual void  TestRangeQueryRandCollating()
		{
			// NOTE: uses index build in *super* setUp
			
			// using the unsigned index because collation seems to ignore hyphens
			IndexReader reader = IndexReader.Open(unsignedIndex.index);
			IndexSearcher search = new IndexSearcher(reader);
			
			System.String minRP = Pad(unsignedIndex.minR);
			System.String maxRP = Pad(unsignedIndex.maxR);
			
			int numDocs = reader.NumDocs();
			
			AssertEquals("num of docs", numDocs, 1 + maxId - minId);
			
			ScoreDoc[] result;
			
			System.Globalization.CompareInfo c = new System.Globalization.CultureInfo("en").CompareInfo;
			
			// test extremes, bounded on both ends
			
			result = search.Search(Csrq("rand", minRP, maxRP, T, T, c), null, numDocs).scoreDocs;
			AssertEquals("find all", numDocs, result.Length);
			
			result = search.Search(Csrq("rand", minRP, maxRP, T, F, c), null, numDocs).scoreDocs;
			AssertEquals("all but biggest", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("rand", minRP, maxRP, F, T, c), null, numDocs).scoreDocs;
			AssertEquals("all but smallest", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("rand", minRP, maxRP, F, F, c), null, numDocs).scoreDocs;
			AssertEquals("all but extremes", numDocs - 2, result.Length);
			
			// unbounded
			
			result = search.Search(Csrq("rand", minRP, null, T, F, c), null, numDocs).scoreDocs;
			AssertEquals("smallest and up", numDocs, result.Length);
			
			result = search.Search(Csrq("rand", null, maxRP, F, T, c), null, numDocs).scoreDocs;
			AssertEquals("biggest and down", numDocs, result.Length);
			
			result = search.Search(Csrq("rand", minRP, null, F, F, c), null, numDocs).scoreDocs;
			AssertEquals("not smallest, but up", numDocs - 1, result.Length);
			
			result = search.Search(Csrq("rand", null, maxRP, F, F, c), null, numDocs).scoreDocs;
			AssertEquals("not biggest, but down", numDocs - 1, result.Length);
			
			// very small sets
			
			result = search.Search(Csrq("rand", minRP, minRP, F, F, c), null, numDocs).scoreDocs;
			AssertEquals("min,min,F,F,c", 0, result.Length);
			result = search.Search(Csrq("rand", maxRP, maxRP, F, F, c), null, numDocs).scoreDocs;
			AssertEquals("max,max,F,F,c", 0, result.Length);
			
			result = search.Search(Csrq("rand", minRP, minRP, T, T, c), null, numDocs).scoreDocs;
			AssertEquals("min,min,T,T,c", 1, result.Length);
			result = search.Search(Csrq("rand", null, minRP, F, T, c), null, numDocs).scoreDocs;
			AssertEquals("nul,min,F,T,c", 1, result.Length);
			
			result = search.Search(Csrq("rand", maxRP, maxRP, T, T, c), null, numDocs).scoreDocs;
			AssertEquals("max,max,T,T,c", 1, result.Length);
			result = search.Search(Csrq("rand", maxRP, null, T, F, c), null, numDocs).scoreDocs;
			AssertEquals("max,nul,T,T,c", 1, result.Length);
		}
		
        [Test]
		public virtual void  TestFarsi()
		{
			
			/* build an index */
			RAMDirectory farsiIndex = new RAMDirectory();
			IndexWriter writer = new IndexWriter(farsiIndex, new SimpleAnalyzer(), T, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("content", "\u0633\u0627\u0628", Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.Add(new Field("body", "body", Field.Store.YES, Field.Index.NOT_ANALYZED));
			writer.AddDocument(doc);
			
			writer.Optimize();
			writer.Close();
			
			IndexReader reader = IndexReader.Open(farsiIndex);
			IndexSearcher search = new IndexSearcher(reader);
			
			// Neither Java 1.4.2 nor 1.5.0 has Farsi Locale collation available in
			// RuleBasedCollator. However, the Arabic Locale seems to order the Farsi
			// characters properly.
			System.Globalization.CompareInfo c = new System.Globalization.CultureInfo("ar").CompareInfo;
			
			// Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
			// orders the U+0698 character before the U+0633 character, so the single
			// index Term below should NOT be returned by a ConstantScoreRangeQuery
			// with a Farsi Collator (or an Arabic one for the case when Farsi is
			// not supported).
			ScoreDoc[] result = search.Search(Csrq("content", "\u062F", "\u0698", T, T, c), null, 1000).scoreDocs;
			AssertEquals("The index Term should not be included.", 0, result.Length);
			
			result = search.Search(Csrq("content", "\u0633", "\u0638", T, T, c), null, 1000).scoreDocs;
			AssertEquals("The index Term should be included.", 1, result.Length);
			search.Close();
		}
		
        [Test]
		public virtual void  TestDanish()
		{
			
			/* build an index */
			RAMDirectory danishIndex = new RAMDirectory();
			IndexWriter writer = new IndexWriter(danishIndex, new SimpleAnalyzer(), T, IndexWriter.MaxFieldLength.LIMITED);
			
			// Danish collation orders the words below in the given order
			// (example taken from TestSort.testInternationalSort() ).
			System.String[] words = new System.String[]{"H\u00D8T", "H\u00C5T", "MAND"};
			for (int docnum = 0; docnum < words.Length; ++docnum)
			{
				Document doc = new Document();
				doc.Add(new Field("content", words[docnum], Field.Store.YES, Field.Index.UN_TOKENIZED));
				doc.Add(new Field("body", "body", Field.Store.YES, Field.Index.UN_TOKENIZED));
				writer.AddDocument(doc);
			}
			writer.Optimize();
			writer.Close();
			
			IndexReader reader = IndexReader.Open(danishIndex);
			IndexSearcher search = new IndexSearcher(reader);
			
			System.Globalization.CompareInfo c = new System.Globalization.CultureInfo("da" + "-" + "dk").CompareInfo;
			
			// Unicode order would not include "H\u00C5T" in [ "H\u00D8T", "MAND" ],
			// but Danish collation does.
			ScoreDoc[] result = search.Search(Csrq("content", "H\u00D8T", "MAND", F, F, c), null, 1000).scoreDocs;
			AssertEquals("The index Term should be included.", 1, result.Length);
			
			result = search.Search(Csrq("content", "H\u00C5T", "MAND", F, F, c), null, 1000).scoreDocs;
			AssertEquals("The index Term should not be included.", 0, result.Length);
			search.Close();
		}
	}
}