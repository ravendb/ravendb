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
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using CheckHits = Lucene.Net.Search.CheckHits;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using QueryUtils = Lucene.Net.Search.QueryUtils;
using Scorer = Lucene.Net.Search.Scorer;
using Weight = Lucene.Net.Search.Weight;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search.Spans
{
	
    [TestFixture]
	public class TestFieldMaskingSpanQuery:LuceneTestCase
	{
		[Serializable]
		private class AnonymousClassSpanTermQuery:SpanTermQuery
		{
			private void  InitBlock(TestFieldMaskingSpanQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestFieldMaskingSpanQuery enclosingInstance;
			public TestFieldMaskingSpanQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassSpanTermQuery(TestFieldMaskingSpanQuery enclosingInstance, Lucene.Net.Index.Term Param1):base(Param1)
			{
				InitBlock(enclosingInstance);
			}
			public override Query Rewrite(IndexReader reader)
			{
				return new SpanOrQuery(new SpanQuery[]{new SpanTermQuery(new Term("first", "sally")), new SpanTermQuery(new Term("first", "james"))});
			}
		}
		
		protected internal static Document Doc(Field[] fields)
		{
			Document doc = new Document();
			for (int i = 0; i < fields.Length; i++)
			{
				doc.Add(fields[i]);
			}
			return doc;
		}
		
		protected internal static Field Field(System.String name, System.String value_Renamed)
		{
			return new Field(name, value_Renamed, Lucene.Net.Documents.Field.Store.NO, Lucene.Net.Documents.Field.Index.ANALYZED);
		}
		
		protected internal IndexSearcher searcher;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			writer.AddDocument(Doc(new Field[]{Field("id", "0"), Field("gender", "male"), Field("first", "james"), Field("last", "jones")}));
			
			writer.AddDocument(Doc(new Field[]{Field("id", "1"), Field("gender", "male"), Field("first", "james"), Field("last", "smith"), Field("gender", "female"), Field("first", "sally"), Field("last", "jones")}));
			
			writer.AddDocument(Doc(new Field[]{Field("id", "2"), Field("gender", "female"), Field("first", "greta"), Field("last", "jones"), Field("gender", "female"), Field("first", "sally"), Field("last", "smith"), Field("gender", "male"), Field("first", "james"), Field("last", "jones")}));
			
			writer.AddDocument(Doc(new Field[]{Field("id", "3"), Field("gender", "female"), Field("first", "lisa"), Field("last", "jones"), Field("gender", "male"), Field("first", "bob"), Field("last", "costas")}));
			
			writer.AddDocument(Doc(new Field[]{Field("id", "4"), Field("gender", "female"), Field("first", "sally"), Field("last", "smith"), Field("gender", "female"), Field("first", "linda"), Field("last", "dixit"), Field("gender", "male"), Field("first", "bubba"), Field("last", "jones")}));
			
			writer.Close();
			searcher = new IndexSearcher(directory);
		}
		
		[TearDown]
		public override void  TearDown()
		{
			base.TearDown();
			searcher.Close();
		}
		
		protected internal virtual void  Check(SpanQuery q, int[] docs)
		{
			CheckHits.CheckHitCollector(q, null, searcher, docs);
		}
		
        [Test]
		public virtual void  TestRewrite0()
		{
			SpanQuery q = new FieldMaskingSpanQuery(new SpanTermQuery(new Term("last", "sally")), "first");
			q.SetBoost(8.7654321f);
			SpanQuery qr = (SpanQuery) searcher.Rewrite(q);
			
			QueryUtils.CheckEqual(q, qr);
			
			Assert.AreEqual(1, qr.GetTerms().Count);
		}
		
        [Test]
		public virtual void  TestRewrite1()
		{
			// mask an anon SpanQuery class that rewrites to something else.
			SpanQuery q = new FieldMaskingSpanQuery(new AnonymousClassSpanTermQuery(this, new Term("last", "sally")), "first");
			
			SpanQuery qr = (SpanQuery) searcher.Rewrite(q);
			
			QueryUtils.CheckUnequal(q, qr);
			
			Assert.AreEqual(2, qr.GetTerms().Count);
		}
		
        [Test]
		public virtual void  TestRewrite2()
		{
			SpanQuery q1 = new SpanTermQuery(new Term("last", "smith"));
			SpanQuery q2 = new SpanTermQuery(new Term("last", "jones"));
			SpanQuery q = new SpanNearQuery(new SpanQuery[]{q1, new FieldMaskingSpanQuery(q2, "last")}, 1, true);
			Query qr = searcher.Rewrite(q);
			
			QueryUtils.CheckEqual(q, qr);
			
			System.Collections.Hashtable set_Renamed = new System.Collections.Hashtable();
			qr.ExtractTerms(set_Renamed);
			Assert.AreEqual(2, set_Renamed.Count);
		}
		
        [Test]
		public virtual void  TestEquality1()
		{
			SpanQuery q1 = new FieldMaskingSpanQuery(new SpanTermQuery(new Term("last", "sally")), "first");
			SpanQuery q2 = new FieldMaskingSpanQuery(new SpanTermQuery(new Term("last", "sally")), "first");
			SpanQuery q3 = new FieldMaskingSpanQuery(new SpanTermQuery(new Term("last", "sally")), "XXXXX");
			SpanQuery q4 = new FieldMaskingSpanQuery(new SpanTermQuery(new Term("last", "XXXXX")), "first");
			SpanQuery q5 = new FieldMaskingSpanQuery(new SpanTermQuery(new Term("xXXX", "sally")), "first");
			QueryUtils.CheckEqual(q1, q2);
			QueryUtils.CheckUnequal(q1, q3);
			QueryUtils.CheckUnequal(q1, q4);
			QueryUtils.CheckUnequal(q1, q5);
			
			SpanQuery qA = new FieldMaskingSpanQuery(new SpanTermQuery(new Term("last", "sally")), "first");
			qA.SetBoost(9f);
			SpanQuery qB = new FieldMaskingSpanQuery(new SpanTermQuery(new Term("last", "sally")), "first");
			QueryUtils.CheckUnequal(qA, qB);
			qB.SetBoost(9f);
			QueryUtils.CheckEqual(qA, qB);
		}
		
        [Test]
		public virtual void  TestNoop0()
		{
			SpanQuery q1 = new SpanTermQuery(new Term("last", "sally"));
			SpanQuery q = new FieldMaskingSpanQuery(q1, "first");
			Check(q, new int[]{});
		}
        [Test]
		public virtual void  TestNoop1()
		{
			SpanQuery q1 = new SpanTermQuery(new Term("last", "smith"));
			SpanQuery q2 = new SpanTermQuery(new Term("last", "jones"));
			SpanQuery q = new SpanNearQuery(new SpanQuery[]{q1, new FieldMaskingSpanQuery(q2, "last")}, 0, true);
			Check(q, new int[]{1, 2});
			q = new SpanNearQuery(new SpanQuery[]{new FieldMaskingSpanQuery(q1, "last"), new FieldMaskingSpanQuery(q2, "last")}, 0, true);
			Check(q, new int[]{1, 2});
		}
		
        [Test]
		public virtual void  TestSimple1()
		{
			SpanQuery q1 = new SpanTermQuery(new Term("first", "james"));
			SpanQuery q2 = new SpanTermQuery(new Term("last", "jones"));
			SpanQuery q = new SpanNearQuery(new SpanQuery[]{q1, new FieldMaskingSpanQuery(q2, "first")}, - 1, false);
			Check(q, new int[]{0, 2});
			q = new SpanNearQuery(new SpanQuery[]{new FieldMaskingSpanQuery(q2, "first"), q1}, - 1, false);
			Check(q, new int[]{0, 2});
			q = new SpanNearQuery(new SpanQuery[]{q2, new FieldMaskingSpanQuery(q1, "last")}, - 1, false);
			Check(q, new int[]{0, 2});
			q = new SpanNearQuery(new SpanQuery[]{new FieldMaskingSpanQuery(q1, "last"), q2}, - 1, false);
			Check(q, new int[]{0, 2});
		}
		
        [Test]
		public virtual void  TestSimple2()
		{
			SpanQuery q1 = new SpanTermQuery(new Term("gender", "female"));
			SpanQuery q2 = new SpanTermQuery(new Term("last", "smith"));
			SpanQuery q = new SpanNearQuery(new SpanQuery[]{q1, new FieldMaskingSpanQuery(q2, "gender")}, - 1, false);
			Check(q, new int[]{2, 4});
			q = new SpanNearQuery(new SpanQuery[]{new FieldMaskingSpanQuery(q1, "id"), new FieldMaskingSpanQuery(q2, "id")}, - 1, false);
			Check(q, new int[]{2, 4});
		}
		
        [Test]
		public virtual void  TestSpans0()
		{
			SpanQuery q1 = new SpanTermQuery(new Term("gender", "female"));
			SpanQuery q2 = new SpanTermQuery(new Term("first", "james"));
			SpanQuery q = new SpanOrQuery(new SpanQuery[]{q1, new FieldMaskingSpanQuery(q2, "gender")});
			Check(q, new int[]{0, 1, 2, 3, 4});
			
			Spans span = q.GetSpans(searcher.GetIndexReader());
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(0, 0, 1), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(1, 0, 1), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(1, 1, 2), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(2, 0, 1), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(2, 1, 2), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(2, 2, 3), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(3, 0, 1), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(4, 0, 1), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(4, 1, 2), S(span));
			
			Assert.AreEqual(false, span.Next());
		}
		
        [Test]
		public virtual void  TestSpans1()
		{
			SpanQuery q1 = new SpanTermQuery(new Term("first", "sally"));
			SpanQuery q2 = new SpanTermQuery(new Term("first", "james"));
			SpanQuery qA = new SpanOrQuery(new SpanQuery[]{q1, q2});
			SpanQuery qB = new FieldMaskingSpanQuery(qA, "id");
			
			Check(qA, new int[]{0, 1, 2, 4});
			Check(qB, new int[]{0, 1, 2, 4});
			
			Spans spanA = qA.GetSpans(searcher.GetIndexReader());
			Spans spanB = qB.GetSpans(searcher.GetIndexReader());
			
			while (spanA.Next())
			{
				Assert.IsTrue(spanB.Next(), "spanB not still going");
				Assert.AreEqual(S(spanA), S(spanB), "spanA not equal spanB");
			}
			Assert.IsTrue(!(spanB.Next()), "spanB still going even tough spanA is done");
		}
		
        [Test]
		public virtual void  TestSpans2()
		{
			SpanQuery qA1 = new SpanTermQuery(new Term("gender", "female"));
			SpanQuery qA2 = new SpanTermQuery(new Term("first", "james"));
			SpanQuery qA = new SpanOrQuery(new SpanQuery[]{qA1, new FieldMaskingSpanQuery(qA2, "gender")});
			SpanQuery qB = new SpanTermQuery(new Term("last", "jones"));
			SpanQuery q = new SpanNearQuery(new SpanQuery[]{new FieldMaskingSpanQuery(qA, "id"), new FieldMaskingSpanQuery(qB, "id")}, - 1, false);
			Check(q, new int[]{0, 1, 2, 3});
			
			Spans span = q.GetSpans(searcher.GetIndexReader());
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(0, 0, 1), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(1, 1, 2), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(2, 0, 1), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(2, 2, 3), S(span));
			
			Assert.AreEqual(true, span.Next());
			Assert.AreEqual(S(3, 0, 1), S(span));
			
			Assert.AreEqual(false, span.Next());
		}
		
		public virtual System.String S(Spans span)
		{
			return S(span.Doc(), span.Start(), span.End());
		}
		public virtual System.String S(int doc, int start, int end)
		{
			return "s(" + doc + "," + start + "," + end + ")";
		}
	}
}