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
using DateTools = Lucene.Net.Documents.DateTools;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> DateFilter JUnit tests.
	/// 
	/// 
	/// </summary>
	/// <version>  $Revision: 791175 $
	/// </version>
    [TestFixture]
	public class TestDateFilter:LuceneTestCase
	{
		
		/// <summary> </summary>
		[Test]
		public virtual void  TestBefore()
		{
			// create an index
			RAMDirectory indexStore = new RAMDirectory();
			IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			long now = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
			
			Document doc = new Document();
			// add time that is in the past
			doc.Add(new Field("datefield", DateTools.TimeToString(now - 1000, DateTools.Resolution.MILLISECOND), Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.Add(new Field("body", "Today is a very sunny day in New York City", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.Optimize();
			writer.Close();
			
			IndexSearcher searcher = new IndexSearcher(indexStore);
			
			// filter that should preserve matches
			//DateFilter df1 = DateFilter.Before("datefield", now);
			TermRangeFilter df1 = new TermRangeFilter("datefield", DateTools.TimeToString(now - 2000, DateTools.Resolution.MILLISECOND), DateTools.TimeToString(now, DateTools.Resolution.MILLISECOND), false, true);
			// filter that should discard matches
			//DateFilter df2 = DateFilter.Before("datefield", now - 999999);
			TermRangeFilter df2 = new TermRangeFilter("datefield", DateTools.TimeToString(0, DateTools.Resolution.MILLISECOND), DateTools.TimeToString(now - 2000, DateTools.Resolution.MILLISECOND), true, false);
			
			// search something that doesn't exist with DateFilter
			Query query1 = new TermQuery(new Term("body", "NoMatchForThis"));
			
			// search for something that does exists
			Query query2 = new TermQuery(new Term("body", "sunny"));
			
			ScoreDoc[] result;
			
			// ensure that queries return expected results without DateFilter first
			result = searcher.Search(query1, null, 1000).scoreDocs;
			Assert.AreEqual(0, result.Length);
			
			result = searcher.Search(query2, null, 1000).scoreDocs;
			Assert.AreEqual(1, result.Length);
			
			
			// run queries with DateFilter
			result = searcher.Search(query1, df1, 1000).scoreDocs;
			Assert.AreEqual(0, result.Length);
			
			result = searcher.Search(query1, df2, 1000).scoreDocs;
			Assert.AreEqual(0, result.Length);
			
			result = searcher.Search(query2, df1, 1000).scoreDocs;
			Assert.AreEqual(1, result.Length);
			
			result = searcher.Search(query2, df2, 1000).scoreDocs;
			Assert.AreEqual(0, result.Length);
		}
		
		/// <summary> </summary>
		[Test]
		public virtual void  TestAfter()
		{
			// create an index
			RAMDirectory indexStore = new RAMDirectory();
			IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			long now = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
			
			Document doc = new Document();
			// add time that is in the future
			doc.Add(new Field("datefield", DateTools.TimeToString(now + 888888, DateTools.Resolution.MILLISECOND), Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.Add(new Field("body", "Today is a very sunny day in New York City", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.Optimize();
			writer.Close();
			
			IndexSearcher searcher = new IndexSearcher(indexStore);
			
			// filter that should preserve matches
			//DateFilter df1 = DateFilter.After("datefield", now);
			TermRangeFilter df1 = new TermRangeFilter("datefield", DateTools.TimeToString(now, DateTools.Resolution.MILLISECOND), DateTools.TimeToString(now + 999999, DateTools.Resolution.MILLISECOND), true, false);
			// filter that should discard matches
			//DateFilter df2 = DateFilter.After("datefield", now + 999999);
			TermRangeFilter df2 = new TermRangeFilter("datefield", DateTools.TimeToString(now + 999999, DateTools.Resolution.MILLISECOND), DateTools.TimeToString(now + 999999999, DateTools.Resolution.MILLISECOND), false, true);
			
			// search something that doesn't exist with DateFilter
			Query query1 = new TermQuery(new Term("body", "NoMatchForThis"));
			
			// search for something that does exists
			Query query2 = new TermQuery(new Term("body", "sunny"));
			
			ScoreDoc[] result;
			
			// ensure that queries return expected results without DateFilter first
			result = searcher.Search(query1, null, 1000).scoreDocs;
			Assert.AreEqual(0, result.Length);
			
			result = searcher.Search(query2, null, 1000).scoreDocs;
			Assert.AreEqual(1, result.Length);
			
			
			// run queries with DateFilter
			result = searcher.Search(query1, df1, 1000).scoreDocs;
			Assert.AreEqual(0, result.Length);
			
			result = searcher.Search(query1, df2, 1000).scoreDocs;
			Assert.AreEqual(0, result.Length);
			
			result = searcher.Search(query2, df1, 1000).scoreDocs;
			Assert.AreEqual(1, result.Length);
			
			result = searcher.Search(query2, df2, 1000).scoreDocs;
			Assert.AreEqual(0, result.Length);
		}
	}
}