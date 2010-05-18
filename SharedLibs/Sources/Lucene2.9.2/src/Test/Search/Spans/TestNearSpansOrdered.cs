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
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using CheckHits = Lucene.Net.Search.CheckHits;
using Explanation = Lucene.Net.Search.Explanation;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Scorer = Lucene.Net.Search.Scorer;
using Weight = Lucene.Net.Search.Weight;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search.Spans
{
	
    [TestFixture]
	public class TestNearSpansOrdered:LuceneTestCase
	{
		protected internal IndexSearcher searcher;
		
		public const System.String FIELD = "field";
		public static readonly QueryParser qp = new QueryParser(FIELD, new WhitespaceAnalyzer());
		
		[TearDown]
		public override void  TearDown()
		{
			base.TearDown();
			searcher.Close();
		}
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < docFields.Length; i++)
			{
				Document doc = new Document();
				doc.Add(new Field(FIELD, docFields[i], Field.Store.NO, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
			writer.Close();
			searcher = new IndexSearcher(directory);
		}
		
		protected internal System.String[] docFields = new System.String[]{"w1 w2 w3 w4 w5", "w1 w3 w2 w3 zz", "w1 xx w2 yy w3", "w1 w3 xx w2 yy w3 zz"};
		
		protected internal virtual SpanNearQuery MakeQuery(System.String s1, System.String s2, System.String s3, int slop, bool inOrder)
		{
			return new SpanNearQuery(new SpanQuery[]{new SpanTermQuery(new Term(FIELD, s1)), new SpanTermQuery(new Term(FIELD, s2)), new SpanTermQuery(new Term(FIELD, s3))}, slop, inOrder);
		}
		protected internal virtual SpanNearQuery MakeQuery()
		{
			return MakeQuery("w1", "w2", "w3", 1, true);
		}
		
		[Test]
		public virtual void  TestSpanNearQuery()
		{
			SpanNearQuery q = MakeQuery();
			CheckHits.CheckHits_Renamed_Method(q, FIELD, searcher, new int[]{0, 1});
		}
		
		public virtual System.String S(Spans span)
		{
			return S(span.Doc(), span.Start(), span.End());
		}
		public virtual System.String S(int doc, int start, int end)
		{
			return "s(" + doc + "," + start + "," + end + ")";
		}
		
		[Test]
		public virtual void  TestNearSpansNext()
		{
			SpanNearQuery q = MakeQuery();
			Spans span = q.GetSpans(searcher.GetIndexReader());
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(0, 0, 3), S(span));
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(1, 0, 4), S(span));
			Assert.AreEqual(false, span.Next());
		}
		
		/// <summary> test does not imply that skipTo(doc+1) should work exactly the
		/// same as next -- it's only applicable in this case since we know doc
		/// does not contain more than one span
		/// </summary>
		[Test]
		public virtual void  TestNearSpansSkipToLikeNext()
		{
			SpanNearQuery q = MakeQuery();
			Spans span = q.GetSpans(searcher.GetIndexReader());
			Assert.AreEqual(true, span.SkipTo(0));
			Assert.AreEqual(S(0, 0, 3), S(span));
			Assert.AreEqual(true, span.SkipTo(1));
			Assert.AreEqual(S(1, 0, 4), S(span));
			Assert.AreEqual(false, span.SkipTo(2));
		}
		
		[Test]
		public virtual void  TestNearSpansNextThenSkipTo()
		{
			SpanNearQuery q = MakeQuery();
			Spans span = q.GetSpans(searcher.GetIndexReader());
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(0, 0, 3), S(span));
			Assert.AreEqual(true, span.SkipTo(1));
			Assert.AreEqual(S(1, 0, 4), S(span));
			Assert.AreEqual(false, span.Next());
		}
		
		[Test]
		public virtual void  TestNearSpansNextThenSkipPast()
		{
			SpanNearQuery q = MakeQuery();
			Spans span = q.GetSpans(searcher.GetIndexReader());
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(0, 0, 3), S(span));
			Assert.AreEqual(false, span.SkipTo(2));
		}
		
		[Test]
		public virtual void  TestNearSpansSkipPast()
		{
			SpanNearQuery q = MakeQuery();
			Spans span = q.GetSpans(searcher.GetIndexReader());
			Assert.AreEqual(false, span.SkipTo(2));
		}
		
		[Test]
		public virtual void  TestNearSpansSkipTo0()
		{
			SpanNearQuery q = MakeQuery();
			Spans span = q.GetSpans(searcher.GetIndexReader());
			Assert.AreEqual(true, span.SkipTo(0));
			Assert.AreEqual(S(0, 0, 3), S(span));
		}
		
		[Test]
		public virtual void  TestNearSpansSkipTo1()
		{
			SpanNearQuery q = MakeQuery();
			Spans span = q.GetSpans(searcher.GetIndexReader());
			Assert.AreEqual(true, span.SkipTo(1));
			Assert.AreEqual(S(1, 0, 4), S(span));
		}
		
		/// <summary> not a direct test of NearSpans, but a demonstration of how/when
		/// this causes problems
		/// </summary>
		[Test]
		public virtual void  TestSpanNearScorerSkipTo1()
		{
			SpanNearQuery q = MakeQuery();
			Weight w = q.Weight(searcher);
			Scorer s = w.Scorer(searcher.GetIndexReader(), true, false);
			Assert.AreEqual(1, s.Advance(1));
		}
		/// <summary> not a direct test of NearSpans, but a demonstration of how/when
		/// this causes problems
		/// </summary>
		[Test]
		public virtual void  TestSpanNearScorerExplain()
		{
			SpanNearQuery q = MakeQuery();
			Weight w = q.Weight(searcher);
			Scorer s = w.Scorer(searcher.GetIndexReader(), true, false);
			Explanation e = s.Explain(1);
			Assert.IsTrue(0.0f < e.GetValue(), "Scorer explanation value for doc#1 isn't positive: " + e.ToString());
		}
	}
}