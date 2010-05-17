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
using IndexWriter = Lucene.Net.Index.IndexWriter;
using ParseException = Lucene.Net.QueryParsers.ParseException;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> Tests {@link MultiSearcher} ranking, i.e. makes sure this bug is fixed:
	/// http://issues.apache.org/bugzilla/show_bug.cgi?id=31841
	/// 
	/// </summary>
	/// <version>  $Id: TestMultiSearcher.java 150492 2004-09-06 22:01:49Z dnaber $
	/// </version>
    [TestFixture]
	public class TestMultiSearcherRanking:LuceneTestCase
	{
		
		private bool verbose = false; // set to true to output hits
		private System.String FIELD_NAME = "body";
		private Searcher multiSearcher;
		private Searcher singleSearcher;
		
		[Test]
		public virtual void  TestOneTermQuery()
		{
			CheckQuery("three");
		}
		
		[Test]
		public virtual void  TestTwoTermQuery()
		{
			CheckQuery("three foo");
		}
		
		[Test]
		public virtual void  TestPrefixQuery()
		{
			CheckQuery("multi*");
		}
		
		[Test]
		public virtual void  TestFuzzyQuery()
		{
			CheckQuery("multiThree~");
		}
		
		[Test]
		public virtual void  TestRangeQuery()
		{
			CheckQuery("{multiA TO multiP}");
		}
		
		[Test]
		public virtual void  TestMultiPhraseQuery()
		{
			CheckQuery("\"blueberry pi*\"");
		}
		
		[Test]
		public virtual void  TestNoMatchQuery()
		{
			CheckQuery("+three +nomatch");
		}
		
		/*
		public void testTermRepeatedQuery() throws IOException, ParseException {
		// TODO: this corner case yields different results.
		checkQuery("multi* multi* foo");
		}
		*/
		
		/// <summary> checks if a query yields the same result when executed on
		/// a single IndexSearcher containing all documents and on a
		/// MultiSearcher aggregating sub-searchers
		/// </summary>
		/// <param name="queryStr"> the query to check.
		/// </param>
		/// <throws>  IOException </throws>
		/// <throws>  ParseException </throws>
		private void  CheckQuery(System.String queryStr)
		{
			// check result hit ranking
			if (verbose)
				System.Console.Out.WriteLine("Query: " + queryStr);
			QueryParser queryParser = new QueryParser(FIELD_NAME, new StandardAnalyzer());
			Query query = queryParser.Parse(queryStr);
			ScoreDoc[] multiSearcherHits = multiSearcher.Search(query, null, 1000).scoreDocs;
			ScoreDoc[] singleSearcherHits = singleSearcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(multiSearcherHits.Length, singleSearcherHits.Length);
			for (int i = 0; i < multiSearcherHits.Length; i++)
			{
				Document docMulti = multiSearcher.Doc(multiSearcherHits[i].doc);
				Document docSingle = singleSearcher.Doc(singleSearcherHits[i].doc);
				if (verbose)
					System.Console.Out.WriteLine("Multi:  " + docMulti.Get(FIELD_NAME) + " score=" + multiSearcherHits[i].score);
				if (verbose)
					System.Console.Out.WriteLine("Single: " + docSingle.Get(FIELD_NAME) + " score=" + singleSearcherHits[i].score);
				Assert.AreEqual(multiSearcherHits[i].score, singleSearcherHits[i].score, 0.001f);
				Assert.AreEqual(docMulti.Get(FIELD_NAME), docSingle.Get(FIELD_NAME));
			}
			if (verbose)
				System.Console.Out.WriteLine();
		}
		
		/// <summary> initializes multiSearcher and singleSearcher with the same document set</summary>
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			// create MultiSearcher from two seperate searchers
			Directory d1 = new RAMDirectory();
			IndexWriter iw1 = new IndexWriter(d1, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddCollection1(iw1);
			iw1.Close();
			Directory d2 = new RAMDirectory();
			IndexWriter iw2 = new IndexWriter(d2, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddCollection2(iw2);
			iw2.Close();
			
			Searchable[] s = new Searchable[2];
			s[0] = new IndexSearcher(d1);
			s[1] = new IndexSearcher(d2);
			multiSearcher = new MultiSearcher(s);
			
			// create IndexSearcher which contains all documents
			Directory d = new RAMDirectory();
			IndexWriter iw = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			AddCollection1(iw);
			AddCollection2(iw);
			iw.Close();
			singleSearcher = new IndexSearcher(d);
		}
		
		private void  AddCollection1(IndexWriter iw)
		{
			Add("one blah three", iw);
			Add("one foo three multiOne", iw);
			Add("one foobar three multiThree", iw);
			Add("blueberry pie", iw);
			Add("blueberry strudel", iw);
			Add("blueberry pizza", iw);
		}
		
		private void  AddCollection2(IndexWriter iw)
		{
			Add("two blah three", iw);
			Add("two foo xxx multiTwo", iw);
			Add("two foobar xxx multiThreee", iw);
			Add("blueberry chewing gum", iw);
			Add("bluebird pizza", iw);
			Add("bluebird foobar pizza", iw);
			Add("piccadilly circus", iw);
		}
		
		private void  Add(System.String value_Renamed, IndexWriter iw)
		{
			Document d = new Document();
			d.Add(new Field(FIELD_NAME, value_Renamed, Field.Store.YES, Field.Index.ANALYZED));
			iw.AddDocument(d);
		}
	}
}