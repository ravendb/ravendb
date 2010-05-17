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
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using CheckHits = Lucene.Net.Search.CheckHits;
using DefaultSimilarity = Lucene.Net.Search.DefaultSimilarity;
using DocIdSetIterator = Lucene.Net.Search.DocIdSetIterator;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using Scorer = Lucene.Net.Search.Scorer;
using Searcher = Lucene.Net.Search.Searcher;
using Similarity = Lucene.Net.Search.Similarity;
using TermQuery = Lucene.Net.Search.TermQuery;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search.Spans
{
	
    [TestFixture]
	public class TestSpans:LuceneTestCase
	{
		[Serializable]
		private class AnonymousClassDefaultSimilarity:DefaultSimilarity
		{
			public AnonymousClassDefaultSimilarity(TestSpans enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestSpans enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestSpans enclosingInstance;
			public TestSpans Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override float SloppyFreq(int distance)
			{
				return 0.0f;
			}
		}
		private class AnonymousClassSpanNearQuery:SpanNearQuery
		{
			private void  InitBlock(Lucene.Net.Search.Similarity sim, TestSpans enclosingInstance)
			{
				this.sim = sim;
				this.enclosingInstance = enclosingInstance;
			}
			private Lucene.Net.Search.Similarity sim;
			private TestSpans enclosingInstance;
			public TestSpans Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassSpanNearQuery(Lucene.Net.Search.Similarity sim, TestSpans enclosingInstance, Lucene.Net.Search.Spans.SpanQuery[] Param1, int Param2, bool Param3):base(Param1, Param2, Param3)
			{
				InitBlock(sim, enclosingInstance);
			}
			public override Similarity GetSimilarity(Searcher s)
			{
				return sim;
			}
		}
		private IndexSearcher searcher;
		
		public const System.String field = "field";
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < docFields.Length; i++)
			{
				Document doc = new Document();
				doc.Add(new Field(field, docFields[i], Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
			writer.Close();
			searcher = new IndexSearcher(directory);
		}
		
		private System.String[] docFields = new System.String[]{"w1 w2 w3 w4 w5", "w1 w3 w2 w3", "w1 xx w2 yy w3", "w1 w3 xx w2 yy w3", "u2 u2 u1", "u2 xx u2 u1", "u2 u2 xx u1", "u2 xx u2 yy u1", "u2 xx u1 u2", "u2 u1 xx u2", "u1 u2 xx u2", "t1 t2 t1 t3 t2 t3"};
		
		public virtual SpanTermQuery MakeSpanTermQuery(System.String text)
		{
			return new SpanTermQuery(new Term(field, text));
		}
		
		private void  CheckHits(Query query, int[] results)
		{
            Lucene.Net.Search.CheckHits.CheckHits_Renamed_Method(query, field, searcher, results);
		}
		
		private void  OrderedSlopTest3SQ(SpanQuery q1, SpanQuery q2, SpanQuery q3, int slop, int[] expectedDocs)
		{
			bool ordered = true;
			SpanNearQuery snq = new SpanNearQuery(new SpanQuery[]{q1, q2, q3}, slop, ordered);
			CheckHits(snq, expectedDocs);
		}
		
		public virtual void  OrderedSlopTest3(int slop, int[] expectedDocs)
		{
			OrderedSlopTest3SQ(MakeSpanTermQuery("w1"), MakeSpanTermQuery("w2"), MakeSpanTermQuery("w3"), slop, expectedDocs);
		}
		
		public virtual void  OrderedSlopTest3Equal(int slop, int[] expectedDocs)
		{
			OrderedSlopTest3SQ(MakeSpanTermQuery("w1"), MakeSpanTermQuery("w3"), MakeSpanTermQuery("w3"), slop, expectedDocs);
		}
		
		public virtual void  OrderedSlopTest1Equal(int slop, int[] expectedDocs)
		{
			OrderedSlopTest3SQ(MakeSpanTermQuery("u2"), MakeSpanTermQuery("u2"), MakeSpanTermQuery("u1"), slop, expectedDocs);
		}
		
		[Test]
		public virtual void  TestSpanNearOrdered01()
		{
			OrderedSlopTest3(0, new int[]{0});
		}
		
		[Test]
		public virtual void  TestSpanNearOrdered02()
		{
			OrderedSlopTest3(1, new int[]{0, 1});
		}
		
		[Test]
		public virtual void  TestSpanNearOrdered03()
		{
			OrderedSlopTest3(2, new int[]{0, 1, 2});
		}
		
		[Test]
		public virtual void  TestSpanNearOrdered04()
		{
			OrderedSlopTest3(3, new int[]{0, 1, 2, 3});
		}
		
		[Test]
		public virtual void  TestSpanNearOrdered05()
		{
			OrderedSlopTest3(4, new int[]{0, 1, 2, 3});
		}
		
		[Test]
		public virtual void  TestSpanNearOrderedEqual01()
		{
			OrderedSlopTest3Equal(0, new int[]{});
		}
		
		[Test]
		public virtual void  TestSpanNearOrderedEqual02()
		{
			OrderedSlopTest3Equal(1, new int[]{1});
		}
		
		[Test]
		public virtual void  TestSpanNearOrderedEqual03()
		{
			OrderedSlopTest3Equal(2, new int[]{1});
		}
		
		[Test]
		public virtual void  TestSpanNearOrderedEqual04()
		{
			OrderedSlopTest3Equal(3, new int[]{1, 3});
		}
		
		[Test]
		public virtual void  TestSpanNearOrderedEqual11()
		{
			OrderedSlopTest1Equal(0, new int[]{4});
		}
		
		[Test]
		public virtual void  TestSpanNearOrderedEqual12()
		{
			OrderedSlopTest1Equal(0, new int[]{4});
		}
		
		[Test]
		public virtual void  TestSpanNearOrderedEqual13()
		{
			OrderedSlopTest1Equal(1, new int[]{4, 5, 6});
		}
		
		[Test]
		public virtual void  TestSpanNearOrderedEqual14()
		{
			OrderedSlopTest1Equal(2, new int[]{4, 5, 6, 7});
		}
		
		[Test]
		public virtual void  TestSpanNearOrderedEqual15()
		{
			OrderedSlopTest1Equal(3, new int[]{4, 5, 6, 7});
		}
		
		[Test]
		public virtual void  TestSpanNearOrderedOverlap()
		{
			bool ordered = true;
			int slop = 1;
			SpanNearQuery snq = new SpanNearQuery(new SpanQuery[]{MakeSpanTermQuery("t1"), MakeSpanTermQuery("t2"), MakeSpanTermQuery("t3")}, slop, ordered);
			Spans spans = snq.GetSpans(searcher.GetIndexReader());
			
			Assert.IsTrue(spans.Next(), "first range");
			Assert.AreEqual(11, spans.Doc(), "first doc");
			Assert.AreEqual(0, spans.Start(), "first start");
			Assert.AreEqual(4, spans.End(), "first end");
			
			Assert.IsTrue(spans.Next(), "second range");
			Assert.AreEqual(11, spans.Doc(), "second doc");
			Assert.AreEqual(2, spans.Start(), "second start");
			Assert.AreEqual(6, spans.End(), "second end");
			
			Assert.IsFalse(spans.Next(), "third range");
		}
		
		
		[Test]
		public virtual void  TestSpanNearUnOrdered()
		{
			
			//See http://www.gossamer-threads.com/lists/lucene/java-dev/52270 for discussion about this test
			SpanNearQuery snq;
			snq = new SpanNearQuery(new SpanQuery[]{MakeSpanTermQuery("u1"), MakeSpanTermQuery("u2")}, 0, false);
			Spans spans = snq.GetSpans(searcher.GetIndexReader());
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(4, spans.Doc(), "doc");
			Assert.AreEqual(1, spans.Start(), "start");
			Assert.AreEqual(3, spans.End(), "end");
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(5, spans.Doc(), "doc");
			Assert.AreEqual(2, spans.Start(), "start");
			Assert.AreEqual(4, spans.End(), "end");
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(8, spans.Doc(), "doc");
			Assert.AreEqual(2, spans.Start(), "start");
			Assert.AreEqual(4, spans.End(), "end");
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(9, spans.Doc(), "doc");
			Assert.AreEqual(0, spans.Start(), "start");
			Assert.AreEqual(2, spans.End(), "end");
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(10, spans.Doc(), "doc");
			Assert.AreEqual(0, spans.Start(), "start");
			Assert.AreEqual(2, spans.End(), "end");
			Assert.IsTrue(spans.Next() == false, "Has next and it shouldn't: " + spans.Doc());
			
			SpanNearQuery u1u2 = new SpanNearQuery(new SpanQuery[]{MakeSpanTermQuery("u1"), MakeSpanTermQuery("u2")}, 0, false);
			snq = new SpanNearQuery(new SpanQuery[]{u1u2, MakeSpanTermQuery("u2")}, 1, false);
			spans = snq.GetSpans(searcher.GetIndexReader());
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(4, spans.Doc(), "doc");
			Assert.AreEqual(0, spans.Start(), "start");
			Assert.AreEqual(3, spans.End(), "end");
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			//unordered spans can be subsets
			Assert.AreEqual(4, spans.Doc(), "doc");
			Assert.AreEqual(1, spans.Start(), "start");
			Assert.AreEqual(3, spans.End(), "end");
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(5, spans.Doc(), "doc");
			Assert.AreEqual(0, spans.Start(), "start");
			Assert.AreEqual(4, spans.End(), "end");
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(5, spans.Doc(), "doc");
			Assert.AreEqual(2, spans.Start(), "start");
			Assert.AreEqual(4, spans.End(), "end");
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(8, spans.Doc(), "doc");
			Assert.AreEqual(0, spans.Start(), "start");
			Assert.AreEqual(4, spans.End(), "end");
			
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(8, spans.Doc(), "doc");
			Assert.AreEqual(2, spans.Start(), "start");
			Assert.AreEqual(4, spans.End(), "end");
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(9, spans.Doc(), "doc");
			Assert.AreEqual(0, spans.Start(), "start");
			Assert.AreEqual(2, spans.End(), "end");
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(9, spans.Doc(), "doc");
			Assert.AreEqual(0, spans.Start(), "start");
			Assert.AreEqual(4, spans.End(), "end");
			
			Assert.IsTrue(spans.Next(), "Does not have next and it should");
			Assert.AreEqual(10, spans.Doc(), "doc");
			Assert.AreEqual(0, spans.Start(), "start");
			Assert.AreEqual(2, spans.End(), "end");
			
			Assert.IsTrue(spans.Next() == false, "Has next and it shouldn't");
		}
		
		
		
		private Spans OrSpans(System.String[] terms)
		{
			SpanQuery[] sqa = new SpanQuery[terms.Length];
			for (int i = 0; i < terms.Length; i++)
			{
				sqa[i] = MakeSpanTermQuery(terms[i]);
			}
			return (new SpanOrQuery(sqa)).GetSpans(searcher.GetIndexReader());
		}
		
		private void  TstNextSpans(Spans spans, int doc, int start, int end)
		{
			Assert.IsTrue(spans.Next(), "next");
			Assert.AreEqual(doc, spans.Doc(), "doc");
			Assert.AreEqual(start, spans.Start(), "start");
			Assert.AreEqual(end, spans.End(), "end");
		}
		
		[Test]
		public virtual void  TestSpanOrEmpty()
		{
			Spans spans = OrSpans(new System.String[0]);
			Assert.IsFalse(spans.Next(), "empty next");
			
			SpanOrQuery a = new SpanOrQuery(new SpanQuery[0]);
			SpanOrQuery b = new SpanOrQuery(new SpanQuery[0]);
			Assert.IsTrue(a.Equals(b), "empty should equal");
		}
		
		[Test]
		public virtual void  TestSpanOrSingle()
		{
			Spans spans = OrSpans(new System.String[]{"w5"});
			TstNextSpans(spans, 0, 4, 5);
			Assert.IsFalse(spans.Next(), "final next");
		}
		
		[Test]
		public virtual void  TestSpanOrMovesForward()
		{
			Spans spans = OrSpans(new System.String[]{"w1", "xx"});
			
			spans.Next();
			int doc = spans.Doc();
			Assert.AreEqual(0, doc);
			
			spans.SkipTo(0);
			doc = spans.Doc();
			
			// LUCENE-1583:
			// according to Spans, a skipTo to the same doc or less
			// should still call next() on the underlying Spans
			Assert.AreEqual(1, doc);
		}
		
		[Test]
		public virtual void  TestSpanOrDouble()
		{
			Spans spans = OrSpans(new System.String[]{"w5", "yy"});
			TstNextSpans(spans, 0, 4, 5);
			TstNextSpans(spans, 2, 3, 4);
			TstNextSpans(spans, 3, 4, 5);
			TstNextSpans(spans, 7, 3, 4);
			Assert.IsFalse(spans.Next(), "final next");
		}
		
		[Test]
		public virtual void  TestSpanOrDoubleSkip()
		{
			Spans spans = OrSpans(new System.String[]{"w5", "yy"});
			Assert.IsTrue(spans.SkipTo(3), "initial skipTo");
			Assert.AreEqual(3, spans.Doc(), "doc");
			Assert.AreEqual(4, spans.Start(), "start");
			Assert.AreEqual(5, spans.End(), "end");
			TstNextSpans(spans, 7, 3, 4);
			Assert.IsFalse(spans.Next(), "final next");
		}
		
		[Test]
		public virtual void  TestSpanOrUnused()
		{
			Spans spans = OrSpans(new System.String[]{"w5", "unusedTerm", "yy"});
			TstNextSpans(spans, 0, 4, 5);
			TstNextSpans(spans, 2, 3, 4);
			TstNextSpans(spans, 3, 4, 5);
			TstNextSpans(spans, 7, 3, 4);
			Assert.IsFalse(spans.Next(), "final next");
		}
		
		[Test]
		public virtual void  TestSpanOrTripleSameDoc()
		{
			Spans spans = OrSpans(new System.String[]{"t1", "t2", "t3"});
			TstNextSpans(spans, 11, 0, 1);
			TstNextSpans(spans, 11, 1, 2);
			TstNextSpans(spans, 11, 2, 3);
			TstNextSpans(spans, 11, 3, 4);
			TstNextSpans(spans, 11, 4, 5);
			TstNextSpans(spans, 11, 5, 6);
			Assert.IsFalse(spans.Next(), "final next");
		}
		
		[Test]
		public virtual void  TestSpanScorerZeroSloppyFreq()
		{
			bool ordered = true;
			int slop = 1;
			
			Similarity sim = new AnonymousClassDefaultSimilarity(this);
			
			SpanNearQuery snq = new AnonymousClassSpanNearQuery(sim, this, new SpanQuery[]{MakeSpanTermQuery("t1"), MakeSpanTermQuery("t2")}, slop, ordered);
			
			Scorer spanScorer = snq.Weight(searcher).Scorer(searcher.GetIndexReader(), true, false);
			
			Assert.IsTrue(spanScorer.NextDoc() != DocIdSetIterator.NO_MORE_DOCS, "first doc");
			Assert.AreEqual(spanScorer.DocID(), 11, "first doc number");
			float score = spanScorer.Score();
			Assert.IsTrue(score == 0.0f, "first doc score should be zero, " + score);
			Assert.IsTrue(spanScorer.NextDoc() == DocIdSetIterator.NO_MORE_DOCS, "no second doc");
		}
		
		// LUCENE-1404
		private void  AddDoc(IndexWriter writer, System.String id, System.String text)
		{
			Document doc = new Document();
			doc.Add(new Field("id", id, Field.Store.YES, Field.Index.UN_TOKENIZED));
			doc.Add(new Field("text", text, Field.Store.YES, Field.Index.TOKENIZED));
			writer.AddDocument(doc);
		}
		
		// LUCENE-1404
		private int HitCount(Searcher searcher, System.String word)
		{
			return searcher.Search(new TermQuery(new Term("text", word)), 10).totalHits;
		}
		
		// LUCENE-1404
		private SpanQuery CreateSpan(System.String value_Renamed)
		{
			return new SpanTermQuery(new Term("text", value_Renamed));
		}
		
		// LUCENE-1404
		private SpanQuery CreateSpan(int slop, bool ordered, SpanQuery[] clauses)
		{
			return new SpanNearQuery(clauses, slop, ordered);
		}
		
		// LUCENE-1404
		private SpanQuery CreateSpan(int slop, bool ordered, System.String term1, System.String term2)
		{
			return CreateSpan(slop, ordered, new SpanQuery[]{CreateSpan(term1), CreateSpan(term2)});
		}
		
		// LUCENE-1404
		[Test]
		public virtual void  TestNPESpanQuery()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(new System.Collections.Hashtable(0)), IndexWriter.MaxFieldLength.LIMITED);
			
			// Add documents
			AddDoc(writer, "1", "the big dogs went running to the market");
			AddDoc(writer, "2", "the cat chased the mouse, then the cat ate the mouse quickly");
			
			// Commit
			writer.Close();
			
			// Get searcher
			IndexReader reader = IndexReader.Open(dir);
			IndexSearcher searcher = new IndexSearcher(reader);
			
			// Control (make sure docs indexed)
			Assert.AreEqual(2, HitCount(searcher, "the"));
			Assert.AreEqual(1, HitCount(searcher, "cat"));
			Assert.AreEqual(1, HitCount(searcher, "dogs"));
			Assert.AreEqual(0, HitCount(searcher, "rabbit"));
			
			// This throws exception (it shouldn't)
			Assert.AreEqual(1, searcher.Search(CreateSpan(0, true, new SpanQuery[]{CreateSpan(4, false, "chased", "cat"), CreateSpan("ate")}), 10).totalHits);
			reader.Close();
			dir.Close();
		}
	}
}