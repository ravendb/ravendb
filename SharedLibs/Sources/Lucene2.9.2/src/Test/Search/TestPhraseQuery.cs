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

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> Tests {@link PhraseQuery}.
	/// 
	/// </summary>
	/// <seealso cref="TestPositionIncrement">
	/// </seealso>
    [TestFixture]
	public class TestPhraseQuery:LuceneTestCase
	{
		private class AnonymousClassAnalyzer:Analyzer
		{
			public AnonymousClassAnalyzer(TestPhraseQuery enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestPhraseQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestPhraseQuery enclosingInstance;
			public TestPhraseQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				return new WhitespaceTokenizer(reader);
			}
			
			public override int GetPositionIncrementGap(System.String fieldName)
			{
				return 100;
			}
		}
		
		/// <summary>threshold for comparing floats </summary>
		public const float SCORE_COMP_THRESH = 1e-6f;
		
		private IndexSearcher searcher;
		private PhraseQuery query;
		private RAMDirectory directory;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			directory = new RAMDirectory();
			Analyzer analyzer = new AnonymousClassAnalyzer(this);
			IndexWriter writer = new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			
			Document doc = new Document();
			doc.Add(new Field("field", "one two three four five", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("repeated", "this is a repeated field - first part", Field.Store.YES, Field.Index.ANALYZED));
			Fieldable repeatedField = new Field("repeated", "second part of a repeated field", Field.Store.YES, Field.Index.ANALYZED);
			doc.Add(repeatedField);
			doc.Add(new Field("palindrome", "one two three two one", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			doc = new Document();
			doc.Add(new Field("nonexist", "phrase exist notexist exist found", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			doc = new Document();
			doc.Add(new Field("nonexist", "phrase exist notexist exist found", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			writer.Optimize();
			writer.Close();
			
			searcher = new IndexSearcher(directory);
			query = new PhraseQuery();
		}
		
		[TearDown]
		public override void  TearDown()
		{
			base.TearDown();
			searcher.Close();
			directory.Close();
		}
		
		[Test]
		public virtual void  TestNotCloseEnough()
		{
			query.SetSlop(2);
			query.Add(new Term("field", "one"));
			query.Add(new Term("field", "five"));
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			QueryUtils.Check(query, searcher);
		}
		
		[Test]
		public virtual void  TestBarelyCloseEnough()
		{
			query.SetSlop(3);
			query.Add(new Term("field", "one"));
			query.Add(new Term("field", "five"));
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			QueryUtils.Check(query, searcher);
		}
		
		/// <summary> Ensures slop of 0 works for exact matches, but not reversed</summary>
		[Test]
		public virtual void  TestExact()
		{
			// slop is zero by default
			query.Add(new Term("field", "four"));
			query.Add(new Term("field", "five"));
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "exact match");
			QueryUtils.Check(query, searcher);
			
			
			query = new PhraseQuery();
			query.Add(new Term("field", "two"));
			query.Add(new Term("field", "one"));
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length, "reverse not exact");
			QueryUtils.Check(query, searcher);
		}
		
		[Test]
		public virtual void  TestSlop1()
		{
			// Ensures slop of 1 works with terms in order.
			query.SetSlop(1);
			query.Add(new Term("field", "one"));
			query.Add(new Term("field", "two"));
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "in order");
			QueryUtils.Check(query, searcher);
			
			
			// Ensures slop of 1 does not work for phrases out of order;
			// must be at least 2.
			query = new PhraseQuery();
			query.SetSlop(1);
			query.Add(new Term("field", "two"));
			query.Add(new Term("field", "one"));
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length, "reversed, slop not 2 or more");
			QueryUtils.Check(query, searcher);
		}
		
		/// <summary> As long as slop is at least 2, terms can be reversed</summary>
		[Test]
		public virtual void  TestOrderDoesntMatter()
		{
			query.SetSlop(2); // must be at least two for reverse order match
			query.Add(new Term("field", "two"));
			query.Add(new Term("field", "one"));
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "just sloppy enough");
			QueryUtils.Check(query, searcher);
			
			
			query = new PhraseQuery();
			query.SetSlop(2);
			query.Add(new Term("field", "three"));
			query.Add(new Term("field", "one"));
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length, "not sloppy enough");
			QueryUtils.Check(query, searcher);
		}
		
		/// <summary> slop is the total number of positional moves allowed
		/// to line up a phrase
		/// </summary>
		[Test]
		public virtual void  TestMulipleTerms()
		{
			query.SetSlop(2);
			query.Add(new Term("field", "one"));
			query.Add(new Term("field", "three"));
			query.Add(new Term("field", "five"));
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "two total moves");
			QueryUtils.Check(query, searcher);
			
			
			query = new PhraseQuery();
			query.SetSlop(5); // it takes six moves to match this phrase
			query.Add(new Term("field", "five"));
			query.Add(new Term("field", "three"));
			query.Add(new Term("field", "one"));
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length, "slop of 5 not close enough");
			QueryUtils.Check(query, searcher);
			
			
			query.SetSlop(6);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "slop of 6 just right");
			QueryUtils.Check(query, searcher);
		}
		
		[Test]
		public virtual void  TestPhraseQueryWithStopAnalyzer()
		{
			RAMDirectory directory = new RAMDirectory();
			StopAnalyzer stopAnalyzer = new StopAnalyzer();
			IndexWriter writer = new IndexWriter(directory, stopAnalyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "the stop words are here", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.Close();
			
			IndexSearcher searcher = new IndexSearcher(directory);
			
			// valid exact phrase query
			PhraseQuery query = new PhraseQuery();
			query.Add(new Term("field", "stop"));
			query.Add(new Term("field", "words"));
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			QueryUtils.Check(query, searcher);
			
			
			// currently StopAnalyzer does not leave "holes", so this matches.
			query = new PhraseQuery();
			query.Add(new Term("field", "words"));
			query.Add(new Term("field", "here"));
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			QueryUtils.Check(query, searcher);
			
			
			searcher.Close();
		}
		
		[Test]
		public virtual void  TestPhraseQueryInConjunctionScorer()
		{
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			Document doc = new Document();
			doc.Add(new Field("source", "marketing info", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			doc = new Document();
			doc.Add(new Field("contents", "foobar", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("source", "marketing info", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			writer.Optimize();
			writer.Close();
			
			IndexSearcher searcher = new IndexSearcher(directory);
			
			PhraseQuery phraseQuery = new PhraseQuery();
			phraseQuery.Add(new Term("source", "marketing"));
			phraseQuery.Add(new Term("source", "info"));
			ScoreDoc[] hits = searcher.Search(phraseQuery, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length);
			QueryUtils.Check(phraseQuery, searcher);
			
			
			TermQuery termQuery = new TermQuery(new Term("contents", "foobar"));
			BooleanQuery booleanQuery = new BooleanQuery();
			booleanQuery.Add(termQuery, BooleanClause.Occur.MUST);
			booleanQuery.Add(phraseQuery, BooleanClause.Occur.MUST);
			hits = searcher.Search(booleanQuery, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			QueryUtils.Check(termQuery, searcher);
			
			
			searcher.Close();
			
			writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			doc = new Document();
			doc.Add(new Field("contents", "map entry woo", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			doc = new Document();
			doc.Add(new Field("contents", "woo map entry", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			doc = new Document();
			doc.Add(new Field("contents", "map foobarword entry woo", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			writer.Optimize();
			writer.Close();
			
			searcher = new IndexSearcher(directory);
			
			termQuery = new TermQuery(new Term("contents", "woo"));
			phraseQuery = new PhraseQuery();
			phraseQuery.Add(new Term("contents", "map"));
			phraseQuery.Add(new Term("contents", "entry"));
			
			hits = searcher.Search(termQuery, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			hits = searcher.Search(phraseQuery, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length);
			
			
			booleanQuery = new BooleanQuery();
			booleanQuery.Add(termQuery, BooleanClause.Occur.MUST);
			booleanQuery.Add(phraseQuery, BooleanClause.Occur.MUST);
			hits = searcher.Search(booleanQuery, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length);
			
			booleanQuery = new BooleanQuery();
			booleanQuery.Add(phraseQuery, BooleanClause.Occur.MUST);
			booleanQuery.Add(termQuery, BooleanClause.Occur.MUST);
			hits = searcher.Search(booleanQuery, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length);
			QueryUtils.Check(booleanQuery, searcher);
			
			
			searcher.Close();
			directory.Close();
		}
		
		[Test]
		public virtual void  TestSlopScoring()
		{
			Directory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			Document doc = new Document();
			doc.Add(new Field("field", "foo firstname lastname foo", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			Document doc2 = new Document();
			doc2.Add(new Field("field", "foo firstname xxx lastname foo", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc2);
			
			Document doc3 = new Document();
			doc3.Add(new Field("field", "foo firstname xxx yyy lastname foo", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc3);
			
			writer.Optimize();
			writer.Close();
			
			Searcher searcher = new IndexSearcher(directory);
			PhraseQuery query = new PhraseQuery();
			query.Add(new Term("field", "firstname"));
			query.Add(new Term("field", "lastname"));
			query.SetSlop(System.Int32.MaxValue);
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length);
			// Make sure that those matches where the terms appear closer to
			// each other get a higher score:
			Assert.AreEqual(0.71, hits[0].score, 0.01);
			Assert.AreEqual(0, hits[0].doc);
			Assert.AreEqual(0.44, hits[1].score, 0.01);
			Assert.AreEqual(1, hits[1].doc);
			Assert.AreEqual(0.31, hits[2].score, 0.01);
			Assert.AreEqual(2, hits[2].doc);
			QueryUtils.Check(query, searcher);
		}
		
		[Test]
		public virtual void  TestToString()
		{
			StopAnalyzer analyzer = new StopAnalyzer();
			StopFilter.SetEnablePositionIncrementsDefault(true);
			QueryParser qp = new QueryParser("field", analyzer);
			qp.SetEnablePositionIncrements(true);
			PhraseQuery q = (PhraseQuery) qp.Parse("\"this hi this is a test is\"");
			Assert.AreEqual("field:\"? hi ? ? ? test\"", q.ToString());
			q.Add(new Term("field", "hello"), 1);
			Assert.AreEqual("field:\"? hi|hello ? ? ? test\"", q.ToString());
		}
		
		[Test]
		public virtual void  TestWrappedPhrase()
		{
			query.Add(new Term("repeated", "first"));
			query.Add(new Term("repeated", "part"));
			query.Add(new Term("repeated", "second"));
			query.Add(new Term("repeated", "part"));
			query.SetSlop(100);
			
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "slop of 100 just right");
			QueryUtils.Check(query, searcher);
			
			query.SetSlop(99);
			
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length, "slop of 99 not enough");
			QueryUtils.Check(query, searcher);
		}
		
		// work on two docs like this: "phrase exist notexist exist found"
		[Test]
		public virtual void  TestNonExistingPhrase()
		{
			// phrase without repetitions that exists in 2 docs
			query.Add(new Term("nonexist", "phrase"));
			query.Add(new Term("nonexist", "notexist"));
			query.Add(new Term("nonexist", "found"));
			query.SetSlop(2); // would be found this way
			
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length, "phrase without repetitions exists in 2 docs");
			QueryUtils.Check(query, searcher);
			
			// phrase with repetitions that exists in 2 docs
			query = new PhraseQuery();
			query.Add(new Term("nonexist", "phrase"));
			query.Add(new Term("nonexist", "exist"));
			query.Add(new Term("nonexist", "exist"));
			query.SetSlop(1); // would be found 
			
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length, "phrase with repetitions exists in two docs");
			QueryUtils.Check(query, searcher);
			
			// phrase I with repetitions that does not exist in any doc
			query = new PhraseQuery();
			query.Add(new Term("nonexist", "phrase"));
			query.Add(new Term("nonexist", "notexist"));
			query.Add(new Term("nonexist", "phrase"));
			query.SetSlop(1000); // would not be found no matter how high the slop is
			
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length, "nonexisting phrase with repetitions does not exist in any doc");
			QueryUtils.Check(query, searcher);
			
			// phrase II with repetitions that does not exist in any doc
			query = new PhraseQuery();
			query.Add(new Term("nonexist", "phrase"));
			query.Add(new Term("nonexist", "exist"));
			query.Add(new Term("nonexist", "exist"));
			query.Add(new Term("nonexist", "exist"));
			query.SetSlop(1000); // would not be found no matter how high the slop is
			
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length, "nonexisting phrase with repetitions does not exist in any doc");
			QueryUtils.Check(query, searcher);
		}
		
		/// <summary> Working on a 2 fields like this:
		/// Field("field", "one two three four five")
		/// Field("palindrome", "one two three two one")
		/// Phrase of size 2 occuriong twice, once in order and once in reverse, 
		/// because doc is a palyndrome, is counted twice. 
		/// Also, in this case order in query does not matter. 
		/// Also, when an exact match is found, both sloppy scorer and exact scorer scores the same.   
		/// </summary>
		[Test]
		public virtual void  TestPalyndrome2()
		{
			
			// search on non palyndrome, find phrase with no slop, using exact phrase scorer
			query.SetSlop(0); // to use exact phrase scorer
			query.Add(new Term("field", "two"));
			query.Add(new Term("field", "three"));
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "phrase found with exact phrase scorer");
			float score0 = hits[0].score;
			//System.out.println("(exact) field: two three: "+score0);
			QueryUtils.Check(query, searcher);
			
			// search on non palyndrome, find phrase with slop 2, though no slop required here.
			query.SetSlop(2); // to use sloppy scorer 
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "just sloppy enough");
			float score1 = hits[0].score;
			//System.out.println("(sloppy) field: two three: "+score1);
			Assert.AreEqual(score0, score1, SCORE_COMP_THRESH, "exact scorer and sloppy scorer score the same when slop does not matter");
			QueryUtils.Check(query, searcher);
			
			// search ordered in palyndrome, find it twice
			query = new PhraseQuery();
			query.SetSlop(2); // must be at least two for both ordered and reversed to match
			query.Add(new Term("palindrome", "two"));
			query.Add(new Term("palindrome", "three"));
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "just sloppy enough");
			float score2 = hits[0].score;
			//System.out.println("palindrome: two three: "+score2);
			QueryUtils.Check(query, searcher);
			
			//commented out for sloppy-phrase efficiency (issue 736) - see SloppyPhraseScorer.phraseFreq(). 
            //Assert.IsTrue(score1+SCORE_COMP_THRESH<score2, "ordered scores higher in palindrome");
			
			// search reveresed in palyndrome, find it twice
			query = new PhraseQuery();
			query.SetSlop(2); // must be at least two for both ordered and reversed to match
			query.Add(new Term("palindrome", "three"));
			query.Add(new Term("palindrome", "two"));
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "just sloppy enough");
			float score3 = hits[0].score;
			//System.out.println("palindrome: three two: "+score3);
			QueryUtils.Check(query, searcher);
			
			//commented out for sloppy-phrase efficiency (issue 736) - see SloppyPhraseScorer.phraseFreq(). 
            //Assert.IsTrue(score1+SCORE_COMP_THRESH<score3,"reversed scores higher in palindrome");
            //Assert.AreEqual(score2, score3, SCORE_COMP_THRESH,"dered or reversed does not matter");
		}
		
		/// <summary> Working on a 2 fields like this:
		/// Field("field", "one two three four five")
		/// Field("palindrome", "one two three two one")
		/// Phrase of size 3 occuriong twice, once in order and once in reverse, 
		/// because doc is a palyndrome, is counted twice. 
		/// Also, in this case order in query does not matter. 
		/// Also, when an exact match is found, both sloppy scorer and exact scorer scores the same.   
		/// </summary>
		[Test]
		public virtual void  TestPalyndrome3()
		{
			
			// search on non palyndrome, find phrase with no slop, using exact phrase scorer
			query.SetSlop(0); // to use exact phrase scorer
			query.Add(new Term("field", "one"));
			query.Add(new Term("field", "two"));
			query.Add(new Term("field", "three"));
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "phrase found with exact phrase scorer");
			float score0 = hits[0].score;
			//System.out.println("(exact) field: one two three: "+score0);
			QueryUtils.Check(query, searcher);
			
			// search on non palyndrome, find phrase with slop 3, though no slop required here.
			query.SetSlop(4); // to use sloppy scorer 
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "just sloppy enough");
			float score1 = hits[0].score;
			//System.out.println("(sloppy) field: one two three: "+score1);
			Assert.AreEqual(score0, score1, SCORE_COMP_THRESH, "exact scorer and sloppy scorer score the same when slop does not matter");
			QueryUtils.Check(query, searcher);
			
			// search ordered in palyndrome, find it twice
			query = new PhraseQuery();
			query.SetSlop(4); // must be at least four for both ordered and reversed to match
			query.Add(new Term("palindrome", "one"));
			query.Add(new Term("palindrome", "two"));
			query.Add(new Term("palindrome", "three"));
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "just sloppy enough");
			float score2 = hits[0].score;
			//System.out.println("palindrome: one two three: "+score2);
			QueryUtils.Check(query, searcher);
			
			//commented out for sloppy-phrase efficiency (issue 736) - see SloppyPhraseScorer.phraseFreq(). 
            //Assert.IsTrue(score1+SCORE_COMP_THRESH<score2,"ordered scores higher in palindrome");
			
			// search reveresed in palyndrome, find it twice
			query = new PhraseQuery();
			query.SetSlop(4); // must be at least four for both ordered and reversed to match
			query.Add(new Term("palindrome", "three"));
			query.Add(new Term("palindrome", "two"));
			query.Add(new Term("palindrome", "one"));
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "just sloppy enough");
			float score3 = hits[0].score;
			//System.out.println("palindrome: three two one: "+score3);
			QueryUtils.Check(query, searcher);
			
			//commented out for sloppy-phrase efficiency (issue 736) - see SloppyPhraseScorer.phraseFreq(). 
            //Assert.IsTrue(score1+SCORE_COMP_THRESH<score3,"reversed scores higher in palindrome");
            //Assert.AreEqual(score2, score3, SCORE_COMP_THRESH, "ordered or reversed does not matter");
		}
		
		// LUCENE-1280
		[Test]
		public virtual void  TestEmptyPhraseQuery()
		{
			BooleanQuery q2 = new BooleanQuery();
			q2.Add(new PhraseQuery(), BooleanClause.Occur.MUST);
			q2.ToString();
		}
	}
}