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

using Document = Lucene.Net.Documents.Document;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using MaxFieldLength = Lucene.Net.Index.IndexWriter.MaxFieldLength;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using Occur = Lucene.Net.Search.BooleanClause.Occur;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestTopScoreDocCollector:LuceneTestCase
	{
		
		public TestTopScoreDocCollector()
		{
		}
		
		public TestTopScoreDocCollector(System.String name):base(name)
		{
		}
		
        [Test]
		public virtual void  TestOutOfOrderCollection()
		{
			
			Directory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, null, MaxFieldLength.UNLIMITED);
			for (int i = 0; i < 10; i++)
			{
				writer.AddDocument(new Document());
			}
			writer.Commit();
			writer.Close();
			
			bool[] inOrder = new bool[]{false, true};
			System.String[] actualTSDCClass = new System.String[]{"OutOfOrderTopScoreDocCollector", "InOrderTopScoreDocCollector"};
			
			// Save the original value to set later.
			bool origVal = BooleanQuery.GetAllowDocsOutOfOrder();
			
			BooleanQuery.SetAllowDocsOutOfOrder(true);
			
			BooleanQuery bq = new BooleanQuery();
			// Add a Query with SHOULD, since bw.scorer() returns BooleanScorer2
			// which delegates to BS if there are no mandatory clauses.
			bq.Add(new MatchAllDocsQuery(), Occur.SHOULD);
			// Set minNrShouldMatch to 1 so that BQ will not optimize rewrite to return
			// the clause instead of BQ.
			bq.SetMinimumNumberShouldMatch(1);
			try
			{
				
				IndexSearcher searcher = new IndexSearcher(dir);
				for (int i = 0; i < inOrder.Length; i++)
				{
					TopDocsCollector tdc = TopScoreDocCollector.create(3, inOrder[i]);
					Assert.AreEqual("Lucene.Net.Search.TopScoreDocCollector+" + actualTSDCClass[i], tdc.GetType().FullName);
					
					searcher.Search(new MatchAllDocsQuery(), tdc);
					
					ScoreDoc[] sd = tdc.TopDocs().scoreDocs;
					Assert.AreEqual(3, sd.Length);
					for (int j = 0; j < sd.Length; j++)
					{
						Assert.AreEqual(j, sd[j].doc, "expected doc Id " + j + " found " + sd[j].doc);
					}
				}
			}
			finally
			{
				// Whatever happens, reset BooleanQuery.allowDocsOutOfOrder to the
				// original value. Don't set it to false in case the implementation in BQ
				// will change some day.
				BooleanQuery.SetAllowDocsOutOfOrder(origVal);
			}
		}
	}
}