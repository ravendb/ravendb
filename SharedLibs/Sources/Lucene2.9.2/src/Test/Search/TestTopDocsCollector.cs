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

using KeywordAnalyzer = Lucene.Net.Analysis.KeywordAnalyzer;
using Document = Lucene.Net.Documents.Document;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using MaxFieldLength = Lucene.Net.Index.IndexWriter.MaxFieldLength;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestTopDocsCollector:LuceneTestCase
	{
		
		private sealed class MyTopsDocCollector:TopDocsCollector
		{
			
			private int idx = 0;
			private int base_Renamed = 0;
			
			public MyTopsDocCollector(int size):base(new HitQueue(size, false))
			{
			}
			
			public /*protected internal*/ override TopDocs NewTopDocs(ScoreDoc[] results, int start)
			{
				if (results == null)
				{
					return EMPTY_TOPDOCS;
				}
				
				float maxScore = System.Single.NaN;
				if (start == 0)
				{
					maxScore = results[0].score;
				}
				else
				{
					for (int i = pq.Size(); i > 1; i--)
					{
						pq.Pop();
					}
					maxScore = ((ScoreDoc) pq.Pop()).score;
				}
				
				return new TopDocs(totalHits, results, maxScore);
			}
			
			public override void  Collect(int doc)
			{
				++totalHits;
				pq.InsertWithOverflow(new ScoreDoc(doc + base_Renamed, Lucene.Net.Search.TestTopDocsCollector.scores[idx++]));
			}
			
			public override void  SetNextReader(IndexReader reader, int docBase)
			{
				base_Renamed = docBase;
			}
			
			public override void  SetScorer(Scorer scorer)
			{
				// Don't do anything. Assign scores in random
			}
			
			public override bool AcceptsDocsOutOfOrder()
			{
				return true;
			}
		}
		
		// Scores array to be used by MyTopDocsCollector. If it is changed, MAX_SCORE
		// must also change.
		private static readonly float[] scores = new float[]{0.7767749f, 1.7839992f, 8.9925785f, 7.9608946f, 0.07948637f, 2.6356435f, 7.4950366f, 7.1490803f, 8.108544f, 4.961808f, 2.2423935f, 7.285586f, 4.6699767f, 2.9655676f, 6.953706f, 5.383931f, 6.9916306f, 8.365894f, 7.888485f, 8.723962f, 3.1796896f, 0.39971232f, 1.3077754f, 6.8489285f, 9.17561f, 5.060466f, 7.9793315f, 8.601509f, 4.1858315f, 0.28146625f};
		
		private const float MAX_SCORE = 9.17561f;
		
		private Directory dir = new RAMDirectory();
		
		private TopDocsCollector doSearch(int numResults)
		{
			Query q = new MatchAllDocsQuery();
			IndexSearcher searcher = new IndexSearcher(dir);
			TopDocsCollector tdc = new MyTopsDocCollector(numResults);
			searcher.Search(q, tdc);
			searcher.Close();
			return tdc;
		}
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			
			// populate an index with 30 documents, this should be enough for the test.
			// The documents have no content - the test uses MatchAllDocsQuery().
            dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new KeywordAnalyzer(), MaxFieldLength.UNLIMITED);
			for (int i = 0; i < 30; i++)
			{
				writer.AddDocument(new Document());
			}
			writer.Close();
		}
		
		[TearDown]
		public override void  TearDown()
		{
			dir.Close();
			dir = null;
			base.TearDown();
		}
		
        [Test]
		public virtual void  TestInvalidArguments()
		{
			int numResults = 5;
			TopDocsCollector tdc = doSearch(numResults);
			
			// start < 0
			Assert.AreEqual(0, tdc.TopDocs(- 1).scoreDocs.Length);
			
			// start > pq.size()
			Assert.AreEqual(0, tdc.TopDocs(numResults + 1).scoreDocs.Length);
			
			// start == pq.size()
			Assert.AreEqual(0, tdc.TopDocs(numResults).scoreDocs.Length);
			
			// howMany < 0
			Assert.AreEqual(0, tdc.TopDocs(0, - 1).scoreDocs.Length);
			
			// howMany == 0
			Assert.AreEqual(0, tdc.TopDocs(0, 0).scoreDocs.Length);
		}
		
        [Test]
		public virtual void  TestZeroResults()
		{
			TopDocsCollector tdc = new MyTopsDocCollector(5);
			Assert.AreEqual(0, tdc.TopDocs(0, 1).scoreDocs.Length);
		}
		
        [Test]
		public virtual void  TestFirstResultsPage()
		{
			TopDocsCollector tdc = doSearch(15);
			Assert.AreEqual(10, tdc.TopDocs(0, 10).scoreDocs.Length);
		}
		
        [Test]
		public virtual void  TestSecondResultsPages()
		{
			TopDocsCollector tdc = doSearch(15);
			// ask for more results than are available
			Assert.AreEqual(5, tdc.TopDocs(10, 10).scoreDocs.Length);
			
			// ask for 5 results (exactly what there should be
			tdc = doSearch(15);
			Assert.AreEqual(5, tdc.TopDocs(10, 5).scoreDocs.Length);
			
			// ask for less results than there are
			tdc = doSearch(15);
			Assert.AreEqual(4, tdc.TopDocs(10, 4).scoreDocs.Length);
		}
		
        [Test]
		public virtual void  TestGetAllResults()
		{
			TopDocsCollector tdc = doSearch(15);
			Assert.AreEqual(15, tdc.TopDocs().scoreDocs.Length);
		}
		
        [Test]
		public virtual void  TestGetResultsFromStart()
		{
			TopDocsCollector tdc = doSearch(15);
			// should bring all results
			Assert.AreEqual(15, tdc.TopDocs(0).scoreDocs.Length);
			
			tdc = doSearch(15);
			// get the last 5 only.
			Assert.AreEqual(5, tdc.TopDocs(10).scoreDocs.Length);
		}
		
        [Test]
		public virtual void  TestMaxScore()
		{
			// ask for all results
			TopDocsCollector tdc = doSearch(15);
			TopDocs td = tdc.TopDocs();
			Assert.AreEqual(MAX_SCORE, td.GetMaxScore(), 0f);
			
			// ask for 5 last results
			tdc = doSearch(15);
			td = tdc.TopDocs(10);
			Assert.AreEqual(MAX_SCORE, td.GetMaxScore(), 0f);
		}
		
		// This does not test the PQ's correctness, but whether topDocs()
		// implementations return the results in decreasing score order.
        [Test]
		public virtual void  TestResultsOrder()
		{
			TopDocsCollector tdc = doSearch(15);
			ScoreDoc[] sd = tdc.TopDocs().scoreDocs;
			
			Assert.AreEqual(MAX_SCORE, sd[0].score, 0f);
			for (int i = 1; i < sd.Length; i++)
			{
				Assert.IsTrue(sd[i - 1].score >= sd[i].score);
			}
		}
	}
}