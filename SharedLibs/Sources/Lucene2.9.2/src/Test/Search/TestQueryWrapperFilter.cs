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
using Index = Lucene.Net.Documents.Field.Index;
using Store = Lucene.Net.Documents.Field.Store;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using Occur = Lucene.Net.Search.BooleanClause.Occur;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestQueryWrapperFilter:LuceneTestCase
	{
		
        [Test]
		public virtual void  TestBasic()
		{
			Directory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "value", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.Close();
			
			TermQuery termQuery = new TermQuery(new Term("field", "value"));
			
			// should not throw exception with primitive query
			QueryWrapperFilter qwf = new QueryWrapperFilter(termQuery);
			
			IndexSearcher searcher = new IndexSearcher(dir, true);
			TopDocs hits = searcher.Search(new MatchAllDocsQuery(), qwf, 10);
			Assert.AreEqual(1, hits.totalHits);
			
			// should not throw exception with complex primitive query
			BooleanQuery booleanQuery = new BooleanQuery();
			booleanQuery.Add(termQuery, Occur.MUST);
			booleanQuery.Add(new TermQuery(new Term("field", "missing")), Occur.MUST_NOT);
			qwf = new QueryWrapperFilter(termQuery);
			
			hits = searcher.Search(new MatchAllDocsQuery(), qwf, 10);
			Assert.AreEqual(1, hits.totalHits);
			
			// should not throw exception with non primitive Query (doesn't implement
			// Query#createWeight)
			qwf = new QueryWrapperFilter(new FuzzyQuery(new Term("field", "valu")));
			
			hits = searcher.Search(new MatchAllDocsQuery(), qwf, 10);
			Assert.AreEqual(1, hits.totalHits);
		}
	}
}