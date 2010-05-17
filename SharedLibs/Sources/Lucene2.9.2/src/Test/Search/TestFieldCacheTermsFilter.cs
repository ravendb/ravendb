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
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using MaxFieldLength = Lucene.Net.Index.IndexWriter.MaxFieldLength;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> A basic unit test for FieldCacheTermsFilter
	/// 
	/// </summary>
	/// <seealso cref="Lucene.Net.Search.FieldCacheTermsFilter">
	/// </seealso>
    [TestFixture]
	public class TestFieldCacheTermsFilter:LuceneTestCase
	{
        [Test]
		public virtual void  TestMissingTerms()
		{
			System.String fieldName = "field1";
			MockRAMDirectory rd = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(rd, new KeywordAnalyzer(), MaxFieldLength.UNLIMITED);
			for (int i = 0; i < 100; i++)
			{
				Document doc = new Document();
				int term = i * 10; //terms are units of 10;
				doc.Add(new Field(fieldName, "" + term, Field.Store.YES, Field.Index.NOT_ANALYZED));
				w.AddDocument(doc);
			}
			w.Close();
			
			IndexReader reader = IndexReader.Open(rd);
			IndexSearcher searcher = new IndexSearcher(reader);
			int numDocs = reader.NumDocs();
			ScoreDoc[] results;
			MatchAllDocsQuery q = new MatchAllDocsQuery();
			
			System.Collections.ArrayList terms = new System.Collections.ArrayList();
			terms.Add("5");
			results = searcher.Search(q, new FieldCacheTermsFilter(fieldName, (System.String[]) terms.ToArray(typeof(System.String))), numDocs).scoreDocs;
			Assert.AreEqual(0, results.Length, "Must match nothing");
			
			terms = new System.Collections.ArrayList();
			terms.Add("10");
            results = searcher.Search(q, new FieldCacheTermsFilter(fieldName, (System.String[])terms.ToArray(typeof(System.String))), numDocs).scoreDocs;
			Assert.AreEqual(1, results.Length, "Must match 1");
			
			terms = new System.Collections.ArrayList();
			terms.Add("10");
			terms.Add("20");
			results = searcher.Search(q, new FieldCacheTermsFilter(fieldName, (System.String[]) terms.ToArray(typeof(System.String))), numDocs).scoreDocs;
			Assert.AreEqual(2, results.Length, "Must match 2");
			
			reader.Close();
			rd.Close();
		}
	}
}