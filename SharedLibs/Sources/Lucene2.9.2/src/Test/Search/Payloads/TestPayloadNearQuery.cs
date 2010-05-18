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
using Token = Lucene.Net.Analysis.Token;
using TokenFilter = Lucene.Net.Analysis.TokenFilter;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using PayloadAttribute = Lucene.Net.Analysis.Tokenattributes.PayloadAttribute;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Payload = Lucene.Net.Index.Payload;
using Term = Lucene.Net.Index.Term;
using DefaultSimilarity = Lucene.Net.Search.DefaultSimilarity;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using QueryUtils = Lucene.Net.Search.QueryUtils;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using Searcher = Lucene.Net.Search.Searcher;
using TopDocs = Lucene.Net.Search.TopDocs;
using SpanQuery = Lucene.Net.Search.Spans.SpanQuery;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using SpanNearQuery = Lucene.Net.Search.Spans.SpanNearQuery;
using English = Lucene.Net.Util.English;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search.Payloads
{
	
	
    [TestFixture]
	public class TestPayloadNearQuery:LuceneTestCase
	{
		private void  InitBlock()
		{
			similarity = new BoostingSimilarity();
		}
		private IndexSearcher searcher;
		private BoostingSimilarity similarity;
		private byte[] payload2 = new byte[]{2};
		private byte[] payload4 = new byte[]{4};
		
		public TestPayloadNearQuery():base()
		{
			InitBlock();
		}
		
		private class PayloadAnalyzer:Analyzer
		{
			public PayloadAnalyzer(TestPayloadNearQuery enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestPayloadNearQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestPayloadNearQuery enclosingInstance;
			public TestPayloadNearQuery Enclosing_Instance
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
		
		private class PayloadFilter:TokenFilter
		{
			private void  InitBlock(TestPayloadNearQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestPayloadNearQuery enclosingInstance;
			public TestPayloadNearQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal System.String fieldName;
			internal int numSeen = 0;
			protected internal PayloadAttribute payAtt;
			
			public PayloadFilter(TestPayloadNearQuery enclosingInstance, TokenStream input, System.String fieldName):base(input)
			{
				InitBlock(enclosingInstance);
				this.fieldName = fieldName;
				payAtt = (PayloadAttribute) AddAttribute(typeof(PayloadAttribute));
			}
			
			public override bool IncrementToken()
			{
				bool result = false;
				if (input.IncrementToken() == true)
				{
					if (numSeen % 2 == 0)
					{
						payAtt.SetPayload(new Payload(Enclosing_Instance.payload2));
					}
					else
					{
						payAtt.SetPayload(new Payload(Enclosing_Instance.payload4));
					}
					numSeen++;
					result = true;
				}
				return result;
			}
		}
		
		private PayloadNearQuery NewPhraseQuery(System.String fieldName, System.String phrase, bool inOrder)
		{
			int n;
			System.String[] words = System.Text.RegularExpressions.Regex.Split(phrase, "[\\s]+");
			SpanQuery[] clauses = new SpanQuery[words.Length];
			for (int i = 0; i < clauses.Length; i++)
			{
				clauses[i] = new PayloadTermQuery(new Term(fieldName, words[i]), new AveragePayloadFunction());
			}
			return new PayloadNearQuery(clauses, 0, inOrder);
		}
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			RAMDirectory directory = new RAMDirectory();
			PayloadAnalyzer analyzer = new PayloadAnalyzer(this);
			IndexWriter writer = new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetSimilarity(similarity);
			//writer.infoStream = System.out;
			for (int i = 0; i < 1000; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("field", English.IntToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
				System.String txt = English.IntToEnglish(i) + ' ' + English.IntToEnglish(i + 1);
				doc.Add(new Field("field2", txt, Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
			writer.Optimize();
			writer.Close();
			
			searcher = new IndexSearcher(directory, true);
			searcher.SetSimilarity(similarity);
		}
		
        [Test]
		public virtual void  Test()
		{
			PayloadNearQuery query;
			TopDocs hits;
			
			query = NewPhraseQuery("field", "twenty two", true);
			QueryUtils.Check(query);
			
			// all 10 hits should have score = 3 because adjacent terms have payloads of 2,4
			// and all the similarity factors are set to 1
			hits = searcher.Search(query, null, 100);
			Assert.IsTrue(hits != null, "hits is null and it shouldn't be");
			Assert.IsTrue(hits.totalHits == 10, "should be 10 hits");
			for (int j = 0; j < hits.scoreDocs.Length; j++)
			{
				ScoreDoc doc = hits.scoreDocs[j];
				Assert.IsTrue(doc.score == 3, doc.score + " does not equal: " + 3);
			}
			for (int i = 1; i < 10; i++)
			{
				query = NewPhraseQuery("field", English.IntToEnglish(i) + " hundred", true);
				// all should have score = 3 because adjacent terms have payloads of 2,4
				// and all the similarity factors are set to 1
				hits = searcher.Search(query, null, 100);
				Assert.IsTrue(hits != null, "hits is null and it shouldn't be");
				Assert.IsTrue(hits.totalHits == 100, "should be 100 hits");
				for (int j = 0; j < hits.scoreDocs.Length; j++)
				{
					ScoreDoc doc = hits.scoreDocs[j];
					//				System.out.println("Doc: " + doc.toString());
					//				System.out.println("Explain: " + searcher.explain(query, doc.doc));
					Assert.IsTrue(doc.score == 3, doc.score + " does not equal: " + 3);
				}
			}
		}
		
        [Test]
		public virtual void  TestPayloadNear()
		{
			SpanNearQuery q1, q2;
			PayloadNearQuery query;
			TopDocs hits;
			// SpanNearQuery(clauses, 10000, false)
            q1 = SpanNearQuery_Renamed("field2", "twenty two");
            q2 = SpanNearQuery_Renamed("field2", "twenty three");
			SpanQuery[] clauses = new SpanQuery[2];
			clauses[0] = q1;
			clauses[1] = q2;
			query = new PayloadNearQuery(clauses, 10, false);
			// System.out.println(query.toString());
			Assert.AreEqual(12, searcher.Search(query, null, 100).totalHits);
			/*
			* System.out.println(hits.totalHits); for (int j = 0; j <
			* hits.scoreDocs.length; j++) { ScoreDoc doc = hits.scoreDocs[j];
			* System.out.println("doc: "+doc.doc+", score: "+doc.score); }
			*/
		}
		
		private SpanNearQuery SpanNearQuery_Renamed(System.String fieldName, System.String words)
		{
			System.String[] wordList = System.Text.RegularExpressions.Regex.Split(words, "[\\s]+");
			SpanQuery[] clauses = new SpanQuery[wordList.Length];
			for (int i = 0; i < clauses.Length; i++)
			{
				clauses[i] = new PayloadTermQuery(new Term(fieldName, wordList[i]), new AveragePayloadFunction());
			}
			return new SpanNearQuery(clauses, 10000, false);
		}
		
        [Test]
		public virtual void  TestLongerSpan()
		{
			PayloadNearQuery query;
			TopDocs hits;
			query = NewPhraseQuery("field", "nine hundred ninety nine", true);
			hits = searcher.Search(query, null, 100);
			ScoreDoc doc = hits.scoreDocs[0];
			//		System.out.println("Doc: " + doc.toString());
			//		System.out.println("Explain: " + searcher.explain(query, doc.doc));
			Assert.IsTrue(hits != null, "hits is null and it shouldn't be");
			Assert.IsTrue(hits.totalHits == 1, "there should only be one hit");
			// should have score = 3 because adjacent terms have payloads of 2,4
			Assert.IsTrue(doc.score == 3, doc.score + " does not equal: " + 3);
		}
		
        [Test]
		public virtual void  TestComplexNested()
		{
			PayloadNearQuery query;
			TopDocs hits;
			
			// combine ordered and unordered spans with some nesting to make sure all payloads are counted
			
			SpanQuery q1 = NewPhraseQuery("field", "nine hundred", true);
			SpanQuery q2 = NewPhraseQuery("field", "ninety nine", true);
			SpanQuery q3 = NewPhraseQuery("field", "nine ninety", false);
			SpanQuery q4 = NewPhraseQuery("field", "hundred nine", false);
			SpanQuery[] clauses = new SpanQuery[]{new PayloadNearQuery(new SpanQuery[]{q1, q2}, 0, true), new PayloadNearQuery(new SpanQuery[]{q3, q4}, 0, false)};
			query = new PayloadNearQuery(clauses, 0, false);
			hits = searcher.Search(query, null, 100);
			Assert.IsTrue(hits != null, "hits is null and it shouldn't be");
			// should be only 1 hit - doc 999
			Assert.IsTrue(hits.scoreDocs.Length == 1, "should only be one hit");
			// the score should be 3 - the average of all the underlying payloads
			ScoreDoc doc = hits.scoreDocs[0];
			//		System.out.println("Doc: " + doc.toString());
			//		System.out.println("Explain: " + searcher.explain(query, doc.doc));
			Assert.IsTrue(doc.score == 3, doc.score + " does not equal: " + 3);
		}
		// must be static for weight serialization tests 
		[Serializable]
		internal class BoostingSimilarity:DefaultSimilarity
		{
			
			// TODO: Remove warning after API has been finalized
			public override float ScorePayload(int docId, System.String fieldName, int start, int end, byte[] payload, int offset, int length)
			{
				//we know it is size 4 here, so ignore the offset/length
				return payload[0];
			}
			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			//Make everything else 1 so we see the effect of the payload
			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			public override float LengthNorm(System.String fieldName, int numTerms)
			{
				return 1;
			}
			
			public override float QueryNorm(float sumOfSquaredWeights)
			{
				return 1;
			}
			
			public override float SloppyFreq(int distance)
			{
				return 1;
			}
			
			public override float Coord(int overlap, int maxOverlap)
			{
				return 1;
			}
			public override float Tf(float freq)
			{
				return 1;
			}
			// idf used for phrase queries
			public override float Idf(System.Collections.ICollection terms, Searcher searcher)
			{
				return 1;
			}
		}
	}
}