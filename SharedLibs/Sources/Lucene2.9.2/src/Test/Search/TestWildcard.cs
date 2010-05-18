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
using Index = Lucene.Net.Documents.Field.Index;
using Store = Lucene.Net.Documents.Field.Store;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> TestWildcard tests the '*' and '?' wildcard characters.
	/// 
	/// </summary>
	/// <version>  $Id: TestWildcard.java 694004 2008-09-10 21:38:52Z mikemccand $
	/// 
	/// </version>
    [TestFixture]
	public class TestWildcard:LuceneTestCase
	{
		[Test]
		public virtual void  TestEquals()
		{
			WildcardQuery wq1 = new WildcardQuery(new Term("field", "b*a"));
			WildcardQuery wq2 = new WildcardQuery(new Term("field", "b*a"));
			WildcardQuery wq3 = new WildcardQuery(new Term("field", "b*a"));
			
			// reflexive?
			Assert.AreEqual(wq1, wq2);
			Assert.AreEqual(wq2, wq1);
			
			// transitive?
			Assert.AreEqual(wq2, wq3);
			Assert.AreEqual(wq1, wq3);
			
			Assert.IsFalse(wq1.Equals(null));
			
			FuzzyQuery fq = new FuzzyQuery(new Term("field", "b*a"));
			Assert.IsFalse(wq1.Equals(fq));
			Assert.IsFalse(fq.Equals(wq1));
		}
		
		/// <summary> Tests if a WildcardQuery that has no wildcard in the term is rewritten to a single
		/// TermQuery.
		/// </summary>
		[Test]
		public virtual void  TestTermWithoutWildcard()
		{
			RAMDirectory indexStore = GetIndexStore("field", new System.String[]{"nowildcard", "nowildcardx"});
			IndexSearcher searcher = new IndexSearcher(indexStore);
			
			Query wq = new WildcardQuery(new Term("field", "nowildcard"));
			AssertMatches(searcher, wq, 1);
			
			wq = searcher.Rewrite(wq);
			Assert.IsTrue(wq is TermQuery);
		}
		
		/// <summary> Tests Wildcard queries with an asterisk.</summary>
		[Test]
		public virtual void  TestAsterisk()
		{
			RAMDirectory indexStore = GetIndexStore("body", new System.String[]{"metal", "metals"});
			IndexSearcher searcher = new IndexSearcher(indexStore);
			Query query1 = new TermQuery(new Term("body", "metal"));
			Query query2 = new WildcardQuery(new Term("body", "metal*"));
			Query query3 = new WildcardQuery(new Term("body", "m*tal"));
			Query query4 = new WildcardQuery(new Term("body", "m*tal*"));
			Query query5 = new WildcardQuery(new Term("body", "m*tals"));
			
			BooleanQuery query6 = new BooleanQuery();
			query6.Add(query5, BooleanClause.Occur.SHOULD);
			
			BooleanQuery query7 = new BooleanQuery();
			query7.Add(query3, BooleanClause.Occur.SHOULD);
			query7.Add(query5, BooleanClause.Occur.SHOULD);
			
			// Queries do not automatically lower-case search terms:
			Query query8 = new WildcardQuery(new Term("body", "M*tal*"));
			
			AssertMatches(searcher, query1, 1);
			AssertMatches(searcher, query2, 2);
			AssertMatches(searcher, query3, 1);
			AssertMatches(searcher, query4, 2);
			AssertMatches(searcher, query5, 1);
			AssertMatches(searcher, query6, 1);
			AssertMatches(searcher, query7, 2);
			AssertMatches(searcher, query8, 0);
			AssertMatches(searcher, new WildcardQuery(new Term("body", "*tall")), 0);
			AssertMatches(searcher, new WildcardQuery(new Term("body", "*tal")), 1);
			AssertMatches(searcher, new WildcardQuery(new Term("body", "*tal*")), 2);
		}
		
		/// <summary> Tests Wildcard queries with a question mark.
		/// 
		/// </summary>
		/// <throws>  IOException if an error occurs </throws>
		[Test]
		public virtual void  TestQuestionmark()
		{
			RAMDirectory indexStore = GetIndexStore("body", new System.String[]{"metal", "metals", "mXtals", "mXtXls"});
			IndexSearcher searcher = new IndexSearcher(indexStore);
			Query query1 = new WildcardQuery(new Term("body", "m?tal"));
			Query query2 = new WildcardQuery(new Term("body", "metal?"));
			Query query3 = new WildcardQuery(new Term("body", "metals?"));
			Query query4 = new WildcardQuery(new Term("body", "m?t?ls"));
			Query query5 = new WildcardQuery(new Term("body", "M?t?ls"));
			Query query6 = new WildcardQuery(new Term("body", "meta??"));
			
			AssertMatches(searcher, query1, 1);
			AssertMatches(searcher, query2, 1);
			AssertMatches(searcher, query3, 0);
			AssertMatches(searcher, query4, 3);
			AssertMatches(searcher, query5, 0);
			AssertMatches(searcher, query6, 1); // Query: 'meta??' matches 'metals' not 'metal'
		}
		
		private RAMDirectory GetIndexStore(System.String field, System.String[] contents)
		{
			RAMDirectory indexStore = new RAMDirectory();
			IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < contents.Length; ++i)
			{
				Document doc = new Document();
				doc.Add(new Field(field, contents[i], Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
			writer.Optimize();
			writer.Close();
			
			return indexStore;
		}
		
		private void  AssertMatches(IndexSearcher searcher, Query q, int expectedMatches)
		{
			ScoreDoc[] result = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(expectedMatches, result.Length);
		}
		
		/// <summary> Test that wild card queries are parsed to the correct type and are searched correctly.
		/// This test looks at both parsing and execution of wildcard queries.
		/// Although placed here, it also tests prefix queries, verifying that
		/// prefix queries are not parsed into wild card queries, and viceversa.
		/// </summary>
		/// <throws>  Exception </throws>
		[Test]
		public virtual void  TestParsingAndSearching()
		{
			System.String field = "content";
			bool dbg = false;
			QueryParser qp = new QueryParser(field, new WhitespaceAnalyzer());
			qp.SetAllowLeadingWildcard(true);
			System.String[] docs = new System.String[]{"\\ abcdefg1", "\\79 hijklmn1", "\\\\ opqrstu1"};
			// queries that should find all docs
			System.String[] matchAll = new System.String[]{"*", "*1", "**1", "*?", "*?1", "?*1", "**", "***", "\\\\*"};
			// queries that should find no docs
			System.String[] matchNone = new System.String[]{"a*h", "a?h", "*a*h", "?a", "a?"};
			// queries that should be parsed to prefix queries
			System.String[][] matchOneDocPrefix = new System.String[][]{new System.String[]{"a*", "ab*", "abc*"}, new System.String[]{"h*", "hi*", "hij*", "\\\\7*"}, new System.String[]{"o*", "op*", "opq*", "\\\\\\\\*"}};
			// queries that should be parsed to wildcard queries
			System.String[][] matchOneDocWild = new System.String[][]{new System.String[]{"*a*", "*ab*", "*abc**", "ab*e*", "*g?", "*f?1", "abc**"}, new System.String[]{"*h*", "*hi*", "*hij**", "hi*k*", "*n?", "*m?1", "hij**"}, new System.String[]{"*o*", "*op*", "*opq**", "op*q*", "*u?", "*t?1", "opq**"}};
			
			// prepare the index
			RAMDirectory dir = new RAMDirectory();
			IndexWriter iw = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < docs.Length; i++)
			{
				Document doc = new Document();
				doc.Add(new Field(field, docs[i], Field.Store.NO, Field.Index.ANALYZED));
				iw.AddDocument(doc);
			}
			iw.Close();
			
			IndexSearcher searcher = new IndexSearcher(dir);
			
			// test queries that must find all
			for (int i = 0; i < matchAll.Length; i++)
			{
				System.String qtxt = matchAll[i];
				Query q = qp.Parse(qtxt);
				if (dbg)
				{
					System.Console.Out.WriteLine("matchAll: qtxt=" + qtxt + " q=" + q + " " + q.GetType().FullName);
				}
				ScoreDoc[] hits = searcher.Search(q, null, 1000).scoreDocs;
				Assert.AreEqual(docs.Length, hits.Length);
			}
			
			// test queries that must find none
			for (int i = 0; i < matchNone.Length; i++)
			{
				System.String qtxt = matchNone[i];
				Query q = qp.Parse(qtxt);
				if (dbg)
				{
					System.Console.Out.WriteLine("matchNone: qtxt=" + qtxt + " q=" + q + " " + q.GetType().FullName);
				}
				ScoreDoc[] hits = searcher.Search(q, null, 1000).scoreDocs;
				Assert.AreEqual(0, hits.Length);
			}
			
			// test queries that must be prefix queries and must find only one doc
			for (int i = 0; i < matchOneDocPrefix.Length; i++)
			{
				for (int j = 0; j < matchOneDocPrefix[i].Length; j++)
				{
					System.String qtxt = matchOneDocPrefix[i][j];
					Query q = qp.Parse(qtxt);
					if (dbg)
					{
						System.Console.Out.WriteLine("match 1 prefix: doc=" + docs[i] + " qtxt=" + qtxt + " q=" + q + " " + q.GetType().FullName);
					}
					Assert.AreEqual(typeof(PrefixQuery), q.GetType());
					ScoreDoc[] hits = searcher.Search(q, null, 1000).scoreDocs;
					Assert.AreEqual(1, hits.Length);
					Assert.AreEqual(i, hits[0].doc);
				}
			}
			
			// test queries that must be wildcard queries and must find only one doc
			for (int i = 0; i < matchOneDocPrefix.Length; i++)
			{
				for (int j = 0; j < matchOneDocWild[i].Length; j++)
				{
					System.String qtxt = matchOneDocWild[i][j];
					Query q = qp.Parse(qtxt);
					if (dbg)
					{
						System.Console.Out.WriteLine("match 1 wild: doc=" + docs[i] + " qtxt=" + qtxt + " q=" + q + " " + q.GetType().FullName);
					}
					Assert.AreEqual(typeof(WildcardQuery), q.GetType());
					ScoreDoc[] hits = searcher.Search(q, null, 1000).scoreDocs;
					Assert.AreEqual(1, hits.Length);
					Assert.AreEqual(i, hits[0].doc);
				}
			}
			
			searcher.Close();
		}
	}
}