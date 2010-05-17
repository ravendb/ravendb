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
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestBooleanQuery:LuceneTestCase
	{
		
		[Test]
		public virtual void  TestEquality()
		{
			BooleanQuery bq1 = new BooleanQuery();
			bq1.Add(new TermQuery(new Term("field", "value1")), BooleanClause.Occur.SHOULD);
			bq1.Add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.SHOULD);
			BooleanQuery nested1 = new BooleanQuery();
			nested1.Add(new TermQuery(new Term("field", "nestedvalue1")), BooleanClause.Occur.SHOULD);
			nested1.Add(new TermQuery(new Term("field", "nestedvalue2")), BooleanClause.Occur.SHOULD);
			bq1.Add(nested1, BooleanClause.Occur.SHOULD);
			
			BooleanQuery bq2 = new BooleanQuery();
			bq2.Add(new TermQuery(new Term("field", "value1")), BooleanClause.Occur.SHOULD);
			bq2.Add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.SHOULD);
			BooleanQuery nested2 = new BooleanQuery();
			nested2.Add(new TermQuery(new Term("field", "nestedvalue1")), BooleanClause.Occur.SHOULD);
			nested2.Add(new TermQuery(new Term("field", "nestedvalue2")), BooleanClause.Occur.SHOULD);
			bq2.Add(nested2, BooleanClause.Occur.SHOULD);
			
			Assert.AreEqual(bq1, bq2);
		}
		
		[Test]
		public virtual void  TestException()
		{
			try
			{
				BooleanQuery.SetMaxClauseCount(0);
				Assert.Fail();
			}
			catch (System.ArgumentException e)
			{
				// okay
			}
		}
		
		// LUCENE-1630
		[Test]
		public virtual void  TestNullOrSubScorer()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "a b c d", Field.Store.NO, Field.Index.ANALYZED));
			w.AddDocument(doc);
			IndexReader r = w.GetReader();
			IndexSearcher s = new IndexSearcher(r);
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
			
			// PhraseQuery w/ no terms added returns a null scorer
			PhraseQuery pq = new PhraseQuery();
			q.Add(pq, BooleanClause.Occur.SHOULD);
			Assert.AreEqual(1, s.Search(q, 10).totalHits);
			
			// A required clause which returns null scorer should return null scorer to
			// IndexSearcher.
			q = new BooleanQuery();
			pq = new PhraseQuery();
			q.Add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
			q.Add(pq, BooleanClause.Occur.MUST);
			Assert.AreEqual(0, s.Search(q, 10).totalHits);
			
			DisjunctionMaxQuery dmq = new DisjunctionMaxQuery(1.0f);
			dmq.Add(new TermQuery(new Term("field", "a")));
			dmq.Add(pq);
			Assert.AreEqual(1, s.Search(dmq, 10).totalHits);
			
			r.Close();
			w.Close();
			dir.Close();
		}
	}
}