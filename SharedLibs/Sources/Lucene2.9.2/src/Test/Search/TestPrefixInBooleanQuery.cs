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

using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;

namespace Lucene.Net.Search
{
	
	/// <summary> https://issues.apache.org/jira/browse/LUCENE-1974
	/// 
	/// represent the bug of
	/// 
	/// BooleanScorer.score(Collector collector, int max, int firstDocID)
	/// 
	/// Line 273, end=8192, subScorerDocID=11378, then more got false?
	/// 
	/// </summary>
	[TestFixture]
	public class TestPrefixInBooleanQuery:LuceneTestCase
	{
		
		private const System.String FIELD = "name";
		private RAMDirectory directory = new RAMDirectory();

		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			for (int i = 0; i < 5137; ++i)
			{
				Document doc = new Document();
				doc.Add(new Field(FIELD, "meaninglessnames", Field.Store.YES, Field.Index.NOT_ANALYZED));
				writer.AddDocument(doc);
			}
			{
				Document doc = new Document();
				doc.Add(new Field(FIELD, "tangfulin", Field.Store.YES, Field.Index.NOT_ANALYZED));
				writer.AddDocument(doc);
			}
			
			for (int i = 5138; i < 11377; ++i)
			{
				Document doc = new Document();
				doc.Add(new Field(FIELD, "meaninglessnames", Field.Store.YES, Field.Index.NOT_ANALYZED));
				writer.AddDocument(doc);
			}
			{
				Document doc = new Document();
				doc.Add(new Field(FIELD, "tangfulin", Field.Store.YES, Field.Index.NOT_ANALYZED));
				writer.AddDocument(doc);
			}
			
			writer.Close();
		}
		
		[Test]
		public virtual void  TestPrefixQuery()
		{
			IndexSearcher indexSearcher = new IndexSearcher(directory, true);
			Query query = new PrefixQuery(new Term(FIELD, "tang"));
			Assert.AreEqual(2, indexSearcher.Search(query, null, 1000).totalHits, "Number of matched documents");
		}
		
		[Test]
		public virtual void  TestTermQuery()
		{
			IndexSearcher indexSearcher = new IndexSearcher(directory, true);
			Query query = new TermQuery(new Term(FIELD, "tangfulin"));
			Assert.AreEqual(2, indexSearcher.Search(query, null, 1000).totalHits, "Number of matched documents");
		}
		
		[Test]
		public virtual void  TestTermBooleanQuery()
		{
			IndexSearcher indexSearcher = new IndexSearcher(directory, true);
			BooleanQuery query = new BooleanQuery();
			query.Add(new TermQuery(new Term(FIELD, "tangfulin")), BooleanClause.Occur.SHOULD);
			query.Add(new TermQuery(new Term(FIELD, "notexistnames")), BooleanClause.Occur.SHOULD);
			Assert.AreEqual(2, indexSearcher.Search(query, null, 1000).totalHits, "Number of matched documents");
		}
		
		[Test]
		public virtual void  TestPrefixBooleanQuery()
		{
			IndexSearcher indexSearcher = new IndexSearcher(directory, true);
			BooleanQuery query = new BooleanQuery();
			query.Add(new PrefixQuery(new Term(FIELD, "tang")), BooleanClause.Occur.SHOULD);
			query.Add(new TermQuery(new Term(FIELD, "notexistnames")), BooleanClause.Occur.SHOULD);
			Assert.AreEqual(2, indexSearcher.Search(query, null, 1000).totalHits, "Number of matched documents");
		}
	}
}