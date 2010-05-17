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
using StopFilter = Lucene.Net.Analysis.StopFilter;
using TokenFilter = Lucene.Net.Analysis.TokenFilter;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using OffsetAttribute = Lucene.Net.Analysis.Tokenattributes.OffsetAttribute;
using PayloadAttribute = Lucene.Net.Analysis.Tokenattributes.PayloadAttribute;
using PositionIncrementAttribute = Lucene.Net.Analysis.Tokenattributes.PositionIncrementAttribute;
using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Payload = Lucene.Net.Index.Payload;
using Term = Lucene.Net.Index.Term;
using TermPositions = Lucene.Net.Index.TermPositions;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using BaseTokenStreamTestCase = Lucene.Net.Analysis.BaseTokenStreamTestCase;
using PayloadSpanUtil = Lucene.Net.Search.Payloads.PayloadSpanUtil;
using SpanNearQuery = Lucene.Net.Search.Spans.SpanNearQuery;
using SpanQuery = Lucene.Net.Search.Spans.SpanQuery;
using SpanTermQuery = Lucene.Net.Search.Spans.SpanTermQuery;

namespace Lucene.Net.Search
{
	
	/// <summary> Term position unit test.
	/// 
	/// 
	/// </summary>
	/// <version>  $Revision: 806844 $
	/// </version>
	public class TestPositionIncrement:BaseTokenStreamTestCase
	{
		private class AnonymousClassAnalyzer:Analyzer
		{
			public AnonymousClassAnalyzer(TestPositionIncrement enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private class AnonymousClassTokenStream:TokenStream
			{
				public AnonymousClassTokenStream(AnonymousClassAnalyzer enclosingInstance)
				{
					InitBlock(enclosingInstance);
				}
				private void  InitBlock(AnonymousClassAnalyzer enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
					posIncrAtt = (PositionIncrementAttribute) AddAttribute(typeof(PositionIncrementAttribute));
					termAtt = (TermAttribute) AddAttribute(typeof(TermAttribute));
					offsetAtt = (OffsetAttribute) AddAttribute(typeof(OffsetAttribute));
				}
				private AnonymousClassAnalyzer enclosingInstance;
				public AnonymousClassAnalyzer Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				private System.String[] TOKENS = new System.String[]{"1", "2", "3", "4", "5"};
				private int[] INCREMENTS = new int[]{0, 2, 1, 0, 1};
				private int i = 0;
				
				internal PositionIncrementAttribute posIncrAtt;
				internal TermAttribute termAtt;
				internal OffsetAttribute offsetAtt;
				
				public override bool IncrementToken()
				{
					if (i == TOKENS.Length)
						return false;
                    ClearAttributes();
					termAtt.SetTermBuffer(TOKENS[i]);
					offsetAtt.SetOffset(i, i);
					posIncrAtt.SetPositionIncrement(INCREMENTS[i]);
					i++;
					return true;
				}
			}
			private void  InitBlock(TestPositionIncrement enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestPositionIncrement enclosingInstance;
			public TestPositionIncrement Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				return new AnonymousClassTokenStream(this);
			}
		}
		
		[Test]
		public virtual void  TestSetPosition()
		{
			Analyzer analyzer = new AnonymousClassAnalyzer(this);
			Directory store = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(store, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			Document d = new Document();
			d.Add(new Field("field", "bogus", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(d);
			writer.Optimize();
			writer.Close();
			
			
			IndexSearcher searcher = new IndexSearcher(store);
			
			TermPositions pos = searcher.GetIndexReader().TermPositions(new Term("field", "1"));
			pos.Next();
			// first token should be at position 0
			Assert.AreEqual(0, pos.NextPosition());
			
			pos = searcher.GetIndexReader().TermPositions(new Term("field", "2"));
			pos.Next();
			// second token should be at position 2
			Assert.AreEqual(2, pos.NextPosition());
			
			PhraseQuery q;
			ScoreDoc[] hits;
			
			q = new PhraseQuery();
			q.Add(new Term("field", "1"));
			q.Add(new Term("field", "2"));
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// same as previous, just specify positions explicitely.
			q = new PhraseQuery();
			q.Add(new Term("field", "1"), 0);
			q.Add(new Term("field", "2"), 1);
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// specifying correct positions should find the phrase.
			q = new PhraseQuery();
			q.Add(new Term("field", "1"), 0);
			q.Add(new Term("field", "2"), 2);
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			q = new PhraseQuery();
			q.Add(new Term("field", "2"));
			q.Add(new Term("field", "3"));
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			q = new PhraseQuery();
			q.Add(new Term("field", "3"));
			q.Add(new Term("field", "4"));
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// phrase query would find it when correct positions are specified. 
			q = new PhraseQuery();
			q.Add(new Term("field", "3"), 0);
			q.Add(new Term("field", "4"), 0);
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			// phrase query should fail for non existing searched term 
			// even if there exist another searched terms in the same searched position. 
			q = new PhraseQuery();
			q.Add(new Term("field", "3"), 0);
			q.Add(new Term("field", "9"), 0);
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// multi-phrase query should succed for non existing searched term
			// because there exist another searched terms in the same searched position. 
			MultiPhraseQuery mq = new MultiPhraseQuery();
			mq.Add(new Term[]{new Term("field", "3"), new Term("field", "9")}, 0);
			hits = searcher.Search(mq, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			q = new PhraseQuery();
			q.Add(new Term("field", "2"));
			q.Add(new Term("field", "4"));
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			q = new PhraseQuery();
			q.Add(new Term("field", "3"));
			q.Add(new Term("field", "5"));
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			q = new PhraseQuery();
			q.Add(new Term("field", "4"));
			q.Add(new Term("field", "5"));
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			q = new PhraseQuery();
			q.Add(new Term("field", "2"));
			q.Add(new Term("field", "5"));
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// should not find "1 2" because there is a gap of 1 in the index
			QueryParser qp = new QueryParser("field", new StopWhitespaceAnalyzer(false));
			q = (PhraseQuery) qp.Parse("\"1 2\"");
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// omitted stop word cannot help because stop filter swallows the increments. 
			q = (PhraseQuery) qp.Parse("\"1 stop 2\"");
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// query parser alone won't help, because stop filter swallows the increments. 
			qp.SetEnablePositionIncrements(true);
			q = (PhraseQuery) qp.Parse("\"1 stop 2\"");
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// stop filter alone won't help, because query parser swallows the increments. 
			qp.SetEnablePositionIncrements(false);
			q = (PhraseQuery) qp.Parse("\"1 stop 2\"");
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length);
			
			// when both qp qnd stopFilter propagate increments, we should find the doc.
			qp = new QueryParser("field", new StopWhitespaceAnalyzer(true));
			qp.SetEnablePositionIncrements(true);
			q = (PhraseQuery) qp.Parse("\"1 stop 2\"");
			hits = searcher.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
		}
		
		private class StopWhitespaceAnalyzer:Analyzer
		{
			internal bool enablePositionIncrements;
			internal WhitespaceAnalyzer a = new WhitespaceAnalyzer();
			public StopWhitespaceAnalyzer(bool enablePositionIncrements)
			{
				this.enablePositionIncrements = enablePositionIncrements;
			}
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				TokenStream ts = a.TokenStream(fieldName, reader);
				return new StopFilter(enablePositionIncrements, ts, new System.String[]{"stop"});
			}
		}
		
		[Test]
		public virtual void  TestPayloadsPos0()
		{
			for (int x = 0; x < 2; x++)
			{
				Directory dir = new MockRAMDirectory();
				IndexWriter writer = new IndexWriter(dir, new TestPayloadAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				if (x == 1)
				{
					writer.SetAllowMinus1Position();
				}
				Document doc = new Document();
                System.IO.MemoryStream ms = new System.IO.MemoryStream();
                System.IO.StreamWriter sw = new System.IO.StreamWriter(ms);
                sw.Write("a a b c d e a f g h i j a b k k");
                // flush to stream & reset it's position so it can be read
                sw.Flush();
                ms.Position = 0;
                doc.Add(new Field("content", new System.IO.StreamReader(ms)));
				writer.AddDocument(doc);
				
				IndexReader r = writer.GetReader();
				
				TermPositions tp = r.TermPositions(new Term("content", "a"));
				int count = 0;
				Assert.IsTrue(tp.Next());
				// "a" occurs 4 times
				Assert.AreEqual(4, tp.Freq());
				int expected;
				if (x == 1)
				{
					expected = System.Int32.MaxValue;
				}
				else
				{
					expected = 0;
				}
				Assert.AreEqual(expected, tp.NextPosition());
				if (x == 1)
				{
					continue;
				}
				Assert.AreEqual(1, tp.NextPosition());
				Assert.AreEqual(3, tp.NextPosition());
				Assert.AreEqual(6, tp.NextPosition());
				
				// only one doc has "a"
				Assert.IsFalse(tp.Next());
				
				IndexSearcher is_Renamed = new IndexSearcher(r);
				
				SpanTermQuery stq1 = new SpanTermQuery(new Term("content", "a"));
				SpanTermQuery stq2 = new SpanTermQuery(new Term("content", "k"));
				SpanQuery[] sqs = new SpanQuery[]{stq1, stq2};
				SpanNearQuery snq = new SpanNearQuery(sqs, 30, false);
				
				count = 0;
				bool sawZero = false;
				//System.out.println("\ngetPayloadSpans test");
				Lucene.Net.Search.Spans.Spans pspans = snq.GetSpans(is_Renamed.GetIndexReader());
				while (pspans.Next())
				{
					//System.out.println(pspans.doc() + " - " + pspans.start() + " - "+ pspans.end());
					System.Collections.Generic.ICollection<byte[]> payloads = pspans.GetPayload();
					sawZero |= pspans.Start() == 0;
					for (System.Collections.IEnumerator it = payloads.GetEnumerator(); it.MoveNext(); )
					{
						count++;
						System.Object generatedAux2 = it.Current;
						//System.out.println(new String((byte[]) it.next()));
					}
				}
				Assert.AreEqual(5, count);
				Assert.IsTrue(sawZero);
				
				//System.out.println("\ngetSpans test");
				Lucene.Net.Search.Spans.Spans spans = snq.GetSpans(is_Renamed.GetIndexReader());
				count = 0;
				sawZero = false;
				while (spans.Next())
				{
					count++;
					sawZero |= spans.Start() == 0;
					//System.out.println(spans.doc() + " - " + spans.start() + " - " + spans.end());
				}
				Assert.AreEqual(4, count);
				Assert.IsTrue(sawZero);
				
				//System.out.println("\nPayloadSpanUtil test");
				
				sawZero = false;
				PayloadSpanUtil psu = new PayloadSpanUtil(is_Renamed.GetIndexReader());
				System.Collections.Generic.ICollection<byte[]> pls = psu.GetPayloadsForQuery(snq);
				count = pls.Count;
				for (System.Collections.IEnumerator it = pls.GetEnumerator(); it.MoveNext(); )
				{
					System.String s = new System.String(System.Text.UTF8Encoding.UTF8.GetChars((byte[]) it.Current));
					//System.out.println(s);
					sawZero |= s.Equals("pos: 0");
				}
				Assert.AreEqual(5, count);
				Assert.IsTrue(sawZero);
				writer.Close();
				is_Renamed.GetIndexReader().Close();
				dir.Close();
			}
		}
	}
	
	class TestPayloadAnalyzer:Analyzer
	{
		
		public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
		{
			TokenStream result = new LowerCaseTokenizer(reader);
			return new PayloadFilter(result, fieldName);
		}
	}
	
	class PayloadFilter:TokenFilter
	{
		internal System.String fieldName;
		
		internal int pos;
		
		internal int i;
		
		internal PositionIncrementAttribute posIncrAttr;
		internal PayloadAttribute payloadAttr;
		internal TermAttribute termAttr;
		
		public PayloadFilter(TokenStream input, System.String fieldName):base(input)
		{
			this.fieldName = fieldName;
			pos = 0;
			i = 0;
			posIncrAttr = (PositionIncrementAttribute) input.AddAttribute(typeof(PositionIncrementAttribute));
			payloadAttr = (PayloadAttribute) input.AddAttribute(typeof(PayloadAttribute));
			termAttr = (TermAttribute) input.AddAttribute(typeof(TermAttribute));
		}
		
		public override bool IncrementToken()
		{
			if (input.IncrementToken())
			{
				payloadAttr.SetPayload(new Payload(System.Text.UTF8Encoding.UTF8.GetBytes("pos: " + pos)));
				int posIncr;
				if (i % 2 == 1)
				{
					posIncr = 1;
				}
				else
				{
					posIncr = 0;
				}
				posIncrAttr.SetPositionIncrement(posIncr);
				pos += posIncr;
				// System.out.println("term=" + termAttr.term() + " pos=" + pos);
				i++;
				return true;
			}
			else
			{
				return false;
			}
		}
	}
}