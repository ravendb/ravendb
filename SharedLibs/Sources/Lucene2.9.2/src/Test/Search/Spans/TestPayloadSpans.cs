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

using Analyzer = Lucene.Net.Analysis.Analyzer;
using LowerCaseTokenizer = Lucene.Net.Analysis.LowerCaseTokenizer;
using TokenFilter = Lucene.Net.Analysis.TokenFilter;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using PayloadAttribute = Lucene.Net.Analysis.Tokenattributes.PayloadAttribute;
using PositionIncrementAttribute = Lucene.Net.Analysis.Tokenattributes.PositionIncrementAttribute;
using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using CorruptIndexException = Lucene.Net.Index.CorruptIndexException;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Payload = Lucene.Net.Index.Payload;
using Term = Lucene.Net.Index.Term;
using LockObtainFailedException = Lucene.Net.Store.LockObtainFailedException;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using DefaultSimilarity = Lucene.Net.Search.DefaultSimilarity;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Similarity = Lucene.Net.Search.Similarity;
using TermQuery = Lucene.Net.Search.TermQuery;
using TopDocs = Lucene.Net.Search.TopDocs;
using PayloadHelper = Lucene.Net.Search.Payloads.PayloadHelper;
using PayloadSpanUtil = Lucene.Net.Search.Payloads.PayloadSpanUtil;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search.Spans
{
	
    [TestFixture]
	public class TestPayloadSpans:LuceneTestCase
	{
		private const bool DEBUG = true;
		private IndexSearcher searcher;
		private Similarity similarity = new DefaultSimilarity();
		protected internal IndexReader indexReader;
						
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			PayloadHelper helper = new PayloadHelper();
			searcher = helper.SetUp(similarity, 1000);
			indexReader = searcher.GetIndexReader();
		}
		
		[Test]
		public virtual void  TestSpanTermQuery()
		{
			SpanTermQuery stq;
			Spans spans;
			stq = new SpanTermQuery(new Term(PayloadHelper.FIELD, "seventy"));
			spans = stq.GetSpans(indexReader);
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			CheckSpans(spans, 100, 1, 1, 1);
			
			stq = new SpanTermQuery(new Term(PayloadHelper.NO_PAYLOAD_FIELD, "seventy"));
			spans = stq.GetSpans(indexReader);
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			CheckSpans(spans, 100, 0, 0, 0);
		}
		
		[Test]
		public virtual void  TestSpanFirst()
		{
			
			SpanQuery match;
			SpanFirstQuery sfq;
			match = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
			sfq = new SpanFirstQuery(match, 2);
			Spans spans = sfq.GetSpans(indexReader);
			CheckSpans(spans, 109, 1, 1, 1);
			//Test more complicated subclause
			SpanQuery[] clauses = new SpanQuery[2];
			clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
			clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "hundred"));
			match = new SpanNearQuery(clauses, 0, true);
			sfq = new SpanFirstQuery(match, 2);
			CheckSpans(sfq.GetSpans(indexReader), 100, 2, 1, 1);
			
			match = new SpanNearQuery(clauses, 0, false);
			sfq = new SpanFirstQuery(match, 2);
			CheckSpans(sfq.GetSpans(indexReader), 100, 2, 1, 1);
		}
		
		[Test]
		public virtual void  TestSpanNot()
		{
			SpanQuery[] clauses = new SpanQuery[2];
			clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
			clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "three"));
			SpanQuery spq = new SpanNearQuery(clauses, 5, true);
			SpanNotQuery snq = new SpanNotQuery(spq, new SpanTermQuery(new Term(PayloadHelper.FIELD, "two")));
			CheckSpans(snq.GetSpans(GetSpanNotSearcher().GetIndexReader()), 1, new int[]{2});
		}
		
		public virtual IndexSearcher GetSpanNotSearcher()
		{
			RAMDirectory directory = new RAMDirectory();
			PayloadAnalyzer analyzer = new PayloadAnalyzer(this);
			IndexWriter writer = new IndexWriter(directory, analyzer, true);
			writer.SetSimilarity(similarity);
			
			Document doc = new Document();
			doc.Add(new Field(PayloadHelper.FIELD, "one two three one four three", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			writer.Close();
			
			IndexSearcher searcher = new IndexSearcher(directory);
			searcher.SetSimilarity(similarity);
			return searcher;
		}
		
		[Test]
		public virtual void  TestNestedSpans()
		{
			SpanTermQuery stq;
			Spans spans;
			IndexSearcher searcher = GetSearcher();
			stq = new SpanTermQuery(new Term(PayloadHelper.FIELD, "mark"));
			spans = stq.GetSpans(searcher.GetIndexReader());
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			CheckSpans(spans, 0, null);
			
			
			SpanQuery[] clauses = new SpanQuery[3];
			clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "rr"));
			clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "yy"));
			clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "xx"));
			SpanNearQuery spanNearQuery = new SpanNearQuery(clauses, 12, false);
			
			spans = spanNearQuery.GetSpans(searcher.GetIndexReader());
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			CheckSpans(spans, 2, new int[]{3, 3});
			
			
			clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "xx"));
			clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "rr"));
			clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "yy"));
			
			spanNearQuery = new SpanNearQuery(clauses, 6, true);
			
			
			spans = spanNearQuery.GetSpans(searcher.GetIndexReader());
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			CheckSpans(spans, 1, new int[]{3});
			
			clauses = new SpanQuery[2];
			
			clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "xx"));
			clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "rr"));
			
			spanNearQuery = new SpanNearQuery(clauses, 6, true);
			
			// xx within 6 of rr
			
			SpanQuery[] clauses2 = new SpanQuery[2];
			
			clauses2[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "yy"));
			clauses2[1] = spanNearQuery;
			
			SpanNearQuery nestedSpanNearQuery = new SpanNearQuery(clauses2, 6, false);
			
			// yy within 6 of xx within 6 of rr
			
			spans = nestedSpanNearQuery.GetSpans(searcher.GetIndexReader());
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			CheckSpans(spans, 2, new int[]{3, 3});
		}
		
		[Test]
		public virtual void  TestFirstClauseWithoutPayload()
		{
			Spans spans;
			IndexSearcher searcher = GetSearcher();
			
			SpanQuery[] clauses = new SpanQuery[3];
			clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "nopayload"));
			clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "qq"));
			clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "ss"));
			
			SpanNearQuery spanNearQuery = new SpanNearQuery(clauses, 6, true);
			
			SpanQuery[] clauses2 = new SpanQuery[2];
			
			clauses2[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "pp"));
			clauses2[1] = spanNearQuery;
			
			SpanNearQuery snq = new SpanNearQuery(clauses2, 6, false);
			
			SpanQuery[] clauses3 = new SpanQuery[2];
			
			clauses3[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "np"));
			clauses3[1] = snq;
			
			SpanNearQuery nestedSpanNearQuery = new SpanNearQuery(clauses3, 6, false);
			
			spans = nestedSpanNearQuery.GetSpans(searcher.GetIndexReader());
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			CheckSpans(spans, 1, new int[]{3});
		}
		
		[Test]
		public virtual void  TestHeavilyNestedSpanQuery()
		{
			Spans spans;
			IndexSearcher searcher = GetSearcher();
			
			SpanQuery[] clauses = new SpanQuery[3];
			clauses[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "one"));
			clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "two"));
			clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "three"));
			
			SpanNearQuery spanNearQuery = new SpanNearQuery(clauses, 5, true);
			
			clauses = new SpanQuery[3];
			clauses[0] = spanNearQuery;
			clauses[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "five"));
			clauses[2] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "six"));
			
			SpanNearQuery spanNearQuery2 = new SpanNearQuery(clauses, 6, true);
			
			SpanQuery[] clauses2 = new SpanQuery[2];
			clauses2[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "eleven"));
			clauses2[1] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "ten"));
			SpanNearQuery spanNearQuery3 = new SpanNearQuery(clauses2, 2, false);
			
			SpanQuery[] clauses3 = new SpanQuery[3];
			clauses3[0] = new SpanTermQuery(new Term(PayloadHelper.FIELD, "nine"));
			clauses3[1] = spanNearQuery2;
			clauses3[2] = spanNearQuery3;
			
			SpanNearQuery nestedSpanNearQuery = new SpanNearQuery(clauses3, 6, false);
			
			spans = nestedSpanNearQuery.GetSpans(searcher.GetIndexReader());
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			CheckSpans(spans, 2, new int[]{8, 8});
		}
		
		[Test]
		public virtual void  TestShrinkToAfterShortestMatch()
		{
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new TestPayloadAnalyzer(this), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("content", new System.IO.StreamReader( new System.IO.MemoryStream( System.Text.Encoding.ASCII.GetBytes( "a b c d e f g h i j a k")))));
			writer.AddDocument(doc);
			writer.Close();
			
			IndexSearcher is_Renamed = new IndexSearcher(directory);
			
			SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
			SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
			SpanQuery[] sqs = new SpanQuery[]{stq1, stq2};
			SpanNearQuery snq = new SpanNearQuery(sqs, 1, true);
			Spans spans = snq.GetSpans(is_Renamed.GetIndexReader());
			
			TopDocs topDocs = is_Renamed.Search(snq, 1);
			System.Collections.Hashtable payloadSet = new System.Collections.Hashtable();
			for (int i = 0; i < topDocs.scoreDocs.Length; i++)
			{
				while (spans.Next())
				{
					System.Collections.Generic.ICollection<byte[]> payloads = spans.GetPayload();
					
					for (System.Collections.IEnumerator it = payloads.GetEnumerator(); it.MoveNext(); )
					{
						SupportClass.CollectionsHelper.AddIfNotContains(payloadSet, new System.String(System.Text.UTF8Encoding.UTF8.GetChars((byte[]) it.Current)));
					}
				}
			}
			Assert.AreEqual(2, payloadSet.Count);
			Assert.IsTrue(payloadSet.Contains("a:Noise:10"));
			Assert.IsTrue(payloadSet.Contains("k:Noise:11"));
		}
		
		[Test]
		public virtual void  TestShrinkToAfterShortestMatch2()
		{
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new TestPayloadAnalyzer(this), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
            doc.Add(new Field("content", new System.IO.StreamReader(new System.IO.MemoryStream(System.Text.Encoding.ASCII.GetBytes("a b a d k f a h i k a k")))));
			writer.AddDocument(doc);
			writer.Close();
			
			IndexSearcher is_Renamed = new IndexSearcher(directory);
			
			SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
			SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
			SpanQuery[] sqs = new SpanQuery[]{stq1, stq2};
			SpanNearQuery snq = new SpanNearQuery(sqs, 0, true);
			Spans spans = snq.GetSpans(is_Renamed.GetIndexReader());
			
			TopDocs topDocs = is_Renamed.Search(snq, 1);
			System.Collections.Hashtable payloadSet = new System.Collections.Hashtable();
			for (int i = 0; i < topDocs.scoreDocs.Length; i++)
			{
				while (spans.Next())
				{
					System.Collections.Generic.ICollection<byte[]> payloads = spans.GetPayload();
					int cnt = 0;
					for (System.Collections.IEnumerator it = payloads.GetEnumerator(); it.MoveNext(); )
					{
						SupportClass.CollectionsHelper.AddIfNotContains(payloadSet, new System.String(System.Text.UTF8Encoding.UTF8.GetChars((byte[]) it.Current)));
					}
				}
			}
			Assert.AreEqual(2, payloadSet.Count);
			Assert.IsTrue(payloadSet.Contains("a:Noise:10"));
			Assert.IsTrue(payloadSet.Contains("k:Noise:11"));
		}

        [Test]
        public virtual void TestShrinkToAfterShortestMatch3()
		{
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new TestPayloadAnalyzer(this), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
            doc.Add(new Field("content", new System.IO.StreamReader(new System.IO.MemoryStream(System.Text.Encoding.ASCII.GetBytes("j k a l f k k p a t a k l k t a")))));
			writer.AddDocument(doc);
			writer.Close();
			
			IndexSearcher is_Renamed = new IndexSearcher(directory);
			
			SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
			SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
			SpanQuery[] sqs = new SpanQuery[]{stq1, stq2};
			SpanNearQuery snq = new SpanNearQuery(sqs, 0, true);
			Spans spans = snq.GetSpans(is_Renamed.GetIndexReader());
			
			TopDocs topDocs = is_Renamed.Search(snq, 1);
			System.Collections.Hashtable payloadSet = new System.Collections.Hashtable();
			for (int i = 0; i < topDocs.scoreDocs.Length; i++)
			{
				while (spans.Next())
				{
					System.Collections.Generic.ICollection<byte[]> payloads = spans.GetPayload();
					
					for (System.Collections.IEnumerator it = payloads.GetEnumerator(); it.MoveNext(); )
					{
						SupportClass.CollectionsHelper.AddIfNotContains(payloadSet, new System.String(System.Text.UTF8Encoding.UTF8.GetChars((byte[]) it.Current)));
					}
				}
			}
			Assert.AreEqual(2, payloadSet.Count);
			if (DEBUG)
			{
				System.Collections.IEnumerator pit = payloadSet.GetEnumerator();
				while (pit.MoveNext())
				{
					System.Console.Out.WriteLine("match:" + pit.Current);
				}
			}
			Assert.IsTrue(payloadSet.Contains("a:Noise:10"));
			Assert.IsTrue(payloadSet.Contains("k:Noise:11"));
		}
		
		[Test]
		public virtual void  TestPayloadSpanUtil()
		{
			RAMDirectory directory = new RAMDirectory();
			PayloadAnalyzer analyzer = new PayloadAnalyzer(this);
			IndexWriter writer = new IndexWriter(directory, analyzer, true);
			writer.SetSimilarity(similarity);
			Document doc = new Document();
			doc.Add(new Field(PayloadHelper.FIELD, "xx rr yy mm  pp", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			writer.Close();
			
			IndexSearcher searcher = new IndexSearcher(directory);
			
			IndexReader reader = searcher.GetIndexReader();
			PayloadSpanUtil psu = new PayloadSpanUtil(reader);
			
			System.Collections.Generic.ICollection<byte[]> payloads = psu.GetPayloadsForQuery(new TermQuery(new Term(PayloadHelper.FIELD, "rr")));
			if (DEBUG)
				System.Console.Out.WriteLine("Num payloads:" + payloads.Count);
			System.Collections.IEnumerator it = payloads.GetEnumerator();
			while (it.MoveNext())
			{
				byte[] bytes = (byte[]) it.Current;
				if (DEBUG)
					System.Console.Out.WriteLine(new System.String(System.Text.UTF8Encoding.UTF8.GetChars(bytes)));
			}
		}
		
		private void  CheckSpans(Spans spans, int expectedNumSpans, int expectedNumPayloads, int expectedPayloadLength, int expectedFirstByte)
		{
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			//each position match should have a span associated with it, since there is just one underlying term query, there should
			//only be one entry in the span
			int seen = 0;
			while (spans.Next() == true)
			{
				//if we expect payloads, then isPayloadAvailable should be true
				if (expectedNumPayloads > 0)
				{
					Assert.IsTrue(spans.IsPayloadAvailable() == true, "isPayloadAvailable is not returning the correct value: " + spans.IsPayloadAvailable() + " and it should be: " + (expectedNumPayloads > 0));
				}
				else
				{
					Assert.IsTrue(spans.IsPayloadAvailable() == false, "isPayloadAvailable should be false");
				}
				//See payload helper, for the PayloadHelper.FIELD field, there is a single byte payload at every token
				if (spans.IsPayloadAvailable())
				{
					System.Collections.Generic.ICollection<byte[]> payload = spans.GetPayload();
					Assert.IsTrue(payload.Count == expectedNumPayloads, "payload Size: " + payload.Count + " is not: " + expectedNumPayloads);
					for (System.Collections.IEnumerator iterator = payload.GetEnumerator(); iterator.MoveNext(); )
					{
						byte[] thePayload = (byte[]) iterator.Current;
						Assert.IsTrue(thePayload.Length == expectedPayloadLength, "payload[0] Size: " + thePayload.Length + " is not: " + expectedPayloadLength);
						Assert.IsTrue(thePayload[0] == expectedFirstByte, thePayload[0] + " does not equal: " + expectedFirstByte);
					}
				}
				seen++;
			}
			Assert.IsTrue(seen == expectedNumSpans, seen + " does not equal: " + expectedNumSpans);
		}
		
		private IndexSearcher GetSearcher()
		{
			RAMDirectory directory = new RAMDirectory();
			PayloadAnalyzer analyzer = new PayloadAnalyzer(this);
			System.String[] docs = new System.String[]{"xx rr yy mm  pp", "xx yy mm rr pp", "nopayload qq ss pp np", "one two three four five six seven eight nine ten eleven", "nine one two three four five six seven eight eleven ten"};
			IndexWriter writer = new IndexWriter(directory, analyzer, true);
			
			writer.SetSimilarity(similarity);
			
			Document doc = null;
			for (int i = 0; i < docs.Length; i++)
			{
				doc = new Document();
				System.String docText = docs[i];
				doc.Add(new Field(PayloadHelper.FIELD, docText, Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
			
			writer.Close();
			
			IndexSearcher searcher = new IndexSearcher(directory);
			return searcher;
		}
		
		private void  CheckSpans(Spans spans, int numSpans, int[] numPayloads)
		{
			int cnt = 0;
			
			while (spans.Next() == true)
			{
				if (DEBUG)
					System.Console.Out.WriteLine("\nSpans Dump --");
				if (spans.IsPayloadAvailable())
				{
					System.Collections.Generic.ICollection<byte[]> payload = spans.GetPayload();
					if (DEBUG)
						System.Console.Out.WriteLine("payloads for span:" + payload.Count);
					System.Collections.IEnumerator it = payload.GetEnumerator();
					while (it.MoveNext())
					{
						byte[] bytes = (byte[]) it.Current;
						if (DEBUG)
							System.Console.Out.WriteLine("doc:" + spans.Doc() + " s:" + spans.Start() + " e:" + spans.End() + " " + new System.String(System.Text.UTF8Encoding.UTF8.GetChars(bytes)));
					}
					
					Assert.AreEqual(numPayloads[cnt], payload.Count);
				}
				else
				{
					Assert.IsFalse(numPayloads.Length > 0 && numPayloads[cnt] > 0, "Expected spans:" + numPayloads[cnt] + " found: 0");
				}
				cnt++;
			}
			
			Assert.AreEqual(numSpans, cnt);
		}
		
		internal class PayloadAnalyzer:Analyzer
		{
			public PayloadAnalyzer(TestPayloadSpans enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestPayloadSpans enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestPayloadSpans enclosingInstance;
			public TestPayloadSpans Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				TokenStream result = new LowerCaseTokenizer(reader);
				result = new PayloadFilter(enclosingInstance, result, fieldName);
				return result;
			}
		}
		
		internal class PayloadFilter:TokenFilter
		{
			private void  InitBlock(TestPayloadSpans enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestPayloadSpans enclosingInstance;
			public TestPayloadSpans Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal System.String fieldName;
			internal int numSeen = 0;
			internal System.Collections.Hashtable entities = new System.Collections.Hashtable();
			internal System.Collections.Hashtable nopayload = new System.Collections.Hashtable();
			internal int pos;
			internal PayloadAttribute payloadAtt;
			internal TermAttribute termAtt;
			internal PositionIncrementAttribute posIncrAtt;
			
			public PayloadFilter(TestPayloadSpans enclosingInstance, TokenStream input, System.String fieldName):base(input)
			{
				InitBlock(enclosingInstance);
				this.fieldName = fieldName;
				pos = 0;
				SupportClass.CollectionsHelper.AddIfNotContains(entities, "xx");
				SupportClass.CollectionsHelper.AddIfNotContains(entities, "one");
				SupportClass.CollectionsHelper.AddIfNotContains(nopayload, "nopayload");
				SupportClass.CollectionsHelper.AddIfNotContains(nopayload, "np");
				termAtt = (TermAttribute) AddAttribute(typeof(TermAttribute));
				posIncrAtt = (PositionIncrementAttribute) AddAttribute(typeof(PositionIncrementAttribute));
				payloadAtt = (PayloadAttribute) AddAttribute(typeof(PayloadAttribute));
			}
			
			public override bool IncrementToken()
			{
				if (input.IncrementToken())
				{
					System.String token = new System.String(termAtt.TermBuffer(), 0, termAtt.TermLength());
					
					if (!nopayload.Contains(token))
					{
						if (entities.Contains(token))
						{
							payloadAtt.SetPayload(new Payload(System.Text.UTF8Encoding.UTF8.GetBytes(token + ":Entity:" + pos)));
						}
						else
						{
							payloadAtt.SetPayload(new Payload(System.Text.UTF8Encoding.UTF8.GetBytes(token + ":Noise:" + pos)));
						}
					}
					pos += posIncrAtt.GetPositionIncrement();
					return true;
				}
				return false;
			}
		}
		
		public class TestPayloadAnalyzer:Analyzer
		{
			public TestPayloadAnalyzer(TestPayloadSpans enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestPayloadSpans enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestPayloadSpans enclosingInstance;
			public TestPayloadSpans Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				TokenStream result = new LowerCaseTokenizer(reader);
				result = new PayloadFilter(enclosingInstance, result, fieldName);
				return result;
			}
		}
	}
}