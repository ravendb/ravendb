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
using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;

namespace Lucene.Net.Search
{
	
	/// <summary> Tests {@link FuzzyQuery}.
	/// 
	/// </summary>
    [TestFixture]
	public class TestFuzzyQuery:LuceneTestCase
	{
		
		[Test]
		public virtual void  TestFuzziness()
		{
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDoc("aaaaa", writer);
			AddDoc("aaaab", writer);
			AddDoc("aaabb", writer);
			AddDoc("aabbb", writer);
			AddDoc("abbbb", writer);
			AddDoc("bbbbb", writer);
			AddDoc("ddddd", writer);
			writer.Optimize();
			writer.Close();
			IndexSearcher searcher = new IndexSearcher(directory);
			
			FuzzyQuery query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 0);
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			
			// same with prefix
			query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 1);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 2);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 3);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 4);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length);
			query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 5);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 6);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			// not similar enough:
			query = new FuzzyQuery(new Term("field", "xxxxx"), FuzzyQuery.defaultMinSimilarity, 0);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			query = new FuzzyQuery(new Term("field", "aaccc"), FuzzyQuery.defaultMinSimilarity, 0); // edit distance to "aaaaa" = 3
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// query identical to a word in the index:
			query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 0);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("aaaaa"));
			// default allows for up to two edits:
			Assert.AreEqual(searcher.Doc(hits[1].doc).Get("field"), ("aaaab"));
			Assert.AreEqual(searcher.Doc(hits[2].doc).Get("field"), ("aaabb"));
			
			// query similar to a word in the index:
			query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 0);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("aaaaa"));
			Assert.AreEqual(searcher.Doc(hits[1].doc).Get("field"), ("aaaab"));
			Assert.AreEqual(searcher.Doc(hits[2].doc).Get("field"), ("aaabb"));
			
			// now with prefix
			query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 1);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("aaaaa"));
			Assert.AreEqual(searcher.Doc(hits[1].doc).Get("field"), ("aaaab"));
			Assert.AreEqual(searcher.Doc(hits[2].doc).Get("field"), ("aaabb"));
			query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 2);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("aaaaa"));
			Assert.AreEqual(searcher.Doc(hits[1].doc).Get("field"), ("aaaab"));
			Assert.AreEqual(searcher.Doc(hits[2].doc).Get("field"), ("aaabb"));
			query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 3);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("aaaaa"));
			Assert.AreEqual(searcher.Doc(hits[1].doc).Get("field"), ("aaaab"));
			Assert.AreEqual(searcher.Doc(hits[2].doc).Get("field"), ("aaabb"));
			query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 4);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("aaaaa"));
			Assert.AreEqual(searcher.Doc(hits[1].doc).Get("field"), ("aaaab"));
			query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 5);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			
			query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 0);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("ddddd"));
			
			// now with prefix
			query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 1);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("ddddd"));
			query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 2);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("ddddd"));
			query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 3);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("ddddd"));
			query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 4);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("ddddd"));
			query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 5);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			
			// different field = no match:
			query = new FuzzyQuery(new Term("anotherfield", "ddddX"), FuzzyQuery.defaultMinSimilarity, 0);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			searcher.Close();
			directory.Close();
		}
		
		[Test]
		public virtual void  TestFuzzinessLong()
		{
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDoc("aaaaaaa", writer);
			AddDoc("segment", writer);
			writer.Optimize();
			writer.Close();
			IndexSearcher searcher = new IndexSearcher(directory);
			
			FuzzyQuery query;
			// not similar enough:
			query = new FuzzyQuery(new Term("field", "xxxxx"), FuzzyQuery.defaultMinSimilarity, 0);
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			// edit distance to "aaaaaaa" = 3, this matches because the string is longer than
			// in testDefaultFuzziness so a bigger difference is allowed:
			query = new FuzzyQuery(new Term("field", "aaaaccc"), FuzzyQuery.defaultMinSimilarity, 0);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("aaaaaaa"));
			
			// now with prefix
			query = new FuzzyQuery(new Term("field", "aaaaccc"), FuzzyQuery.defaultMinSimilarity, 1);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("aaaaaaa"));
			query = new FuzzyQuery(new Term("field", "aaaaccc"), FuzzyQuery.defaultMinSimilarity, 4);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), ("aaaaaaa"));
			query = new FuzzyQuery(new Term("field", "aaaaccc"), FuzzyQuery.defaultMinSimilarity, 5);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// no match, more than half of the characters is wrong:
			query = new FuzzyQuery(new Term("field", "aaacccc"), FuzzyQuery.defaultMinSimilarity, 0);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// now with prefix
			query = new FuzzyQuery(new Term("field", "aaacccc"), FuzzyQuery.defaultMinSimilarity, 2);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// "student" and "stellent" are indeed similar to "segment" by default:
			query = new FuzzyQuery(new Term("field", "student"), FuzzyQuery.defaultMinSimilarity, 0);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			query = new FuzzyQuery(new Term("field", "stellent"), FuzzyQuery.defaultMinSimilarity, 0);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			// now with prefix
			query = new FuzzyQuery(new Term("field", "student"), FuzzyQuery.defaultMinSimilarity, 1);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			query = new FuzzyQuery(new Term("field", "stellent"), FuzzyQuery.defaultMinSimilarity, 1);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			query = new FuzzyQuery(new Term("field", "student"), FuzzyQuery.defaultMinSimilarity, 2);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			query = new FuzzyQuery(new Term("field", "stellent"), FuzzyQuery.defaultMinSimilarity, 2);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// "student" doesn't match anymore thanks to increased minimum similarity:
			query = new FuzzyQuery(new Term("field", "student"), 0.6f, 0);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			try
			{
				query = new FuzzyQuery(new Term("field", "student"), 1.1f);
				Assert.Fail("Expected IllegalArgumentException");
			}
			catch (System.ArgumentException e)
			{
				// expecting exception
			}
			try
			{
				query = new FuzzyQuery(new Term("field", "student"), - 0.1f);
				Assert.Fail("Expected IllegalArgumentException");
			}
			catch (System.ArgumentException e)
			{
				// expecting exception
			}
			
			searcher.Close();
			directory.Close();
		}
		
		[Test]
		public virtual void  TestTokenLengthOpt()
		{
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddDoc("12345678911", writer);
			AddDoc("segment", writer);
			writer.Optimize();
			writer.Close();
			IndexSearcher searcher = new IndexSearcher(directory);
			
			Query query;
			// term not over 10 chars, so optimization shortcuts
			query = new FuzzyQuery(new Term("field", "1234569"), 0.9f);
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// 10 chars, so no optimization
			query = new FuzzyQuery(new Term("field", "1234567891"), 0.9f);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// over 10 chars, so no optimization
			query = new FuzzyQuery(new Term("field", "12345678911"), 0.9f);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			// over 10 chars, no match
			query = new FuzzyQuery(new Term("field", "sdfsdfsdfsdf"), 0.9f);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
		}
		
		[Test]
		public virtual void  TestGiga()
		{
			
			StandardAnalyzer analyzer = new StandardAnalyzer();
			
			Directory index = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(index, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED);
			
			AddDoc("Lucene in Action", w);
			AddDoc("Lucene for Dummies", w);
			
			// addDoc("Giga", w);
			AddDoc("Giga byte", w);
			
			AddDoc("ManagingGigabytesManagingGigabyte", w);
			AddDoc("ManagingGigabytesManagingGigabytes", w);
			
			AddDoc("The Art of Computer Science", w);
			AddDoc("J. K. Rowling", w);
			AddDoc("JK Rowling", w);
			AddDoc("Joanne K Roling", w);
			AddDoc("Bruce Willis", w);
			AddDoc("Willis bruce", w);
			AddDoc("Brute willis", w);
			AddDoc("B. willis", w);
			IndexReader r = w.GetReader();
			w.Close();
			
			Query q = new QueryParser("field", analyzer).Parse("giga~0.9");
			
			// 3. search
			IndexSearcher searcher = new IndexSearcher(r);
			ScoreDoc[] hits = searcher.Search(q, 10).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			Assert.AreEqual(searcher.Doc(hits[0].doc).Get("field"), "Giga byte");
			r.Close();
		}
		
		private void  AddDoc(System.String text, IndexWriter writer)
		{
			Document doc = new Document();
			doc.Add(new Field("field", text, Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
		}
	}
}