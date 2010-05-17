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
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Payload = Lucene.Net.Index.Payload;
using Term = Lucene.Net.Index.Term;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using BooleanClause = Lucene.Net.Search.BooleanClause;
using BooleanQuery = Lucene.Net.Search.BooleanQuery;
using CheckHits = Lucene.Net.Search.CheckHits;
using DefaultSimilarity = Lucene.Net.Search.DefaultSimilarity;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using QueryUtils = Lucene.Net.Search.QueryUtils;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using TopDocs = Lucene.Net.Search.TopDocs;
using SpanTermQuery = Lucene.Net.Search.Spans.SpanTermQuery;
using TermSpans = Lucene.Net.Search.Spans.TermSpans;
using English = Lucene.Net.Util.English;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search.Payloads
{
	
	
	/// <summary> 
	/// 
	/// 
	/// </summary>
    [TestFixture]
	public class TestPayloadTermQuery:LuceneTestCase
	{
		private void  InitBlock()
		{
			similarity = new BoostingSimilarity();
		}
		private IndexSearcher searcher;
		private BoostingSimilarity similarity;
		private byte[] payloadField = new byte[]{1};
		private byte[] payloadMultiField1 = new byte[]{2};
		private byte[] payloadMultiField2 = new byte[]{4};
		protected internal RAMDirectory directory;
		
		public TestPayloadTermQuery():base()
		{
			InitBlock();
		}
		
		private class PayloadAnalyzer:Analyzer
		{
			public PayloadAnalyzer(TestPayloadTermQuery enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestPayloadTermQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestPayloadTermQuery enclosingInstance;
			public TestPayloadTermQuery Enclosing_Instance
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
			private void  InitBlock(TestPayloadTermQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestPayloadTermQuery enclosingInstance;
			public TestPayloadTermQuery Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal System.String fieldName;
			internal int numSeen = 0;
			
			internal PayloadAttribute payloadAtt;
			
			public PayloadFilter(TestPayloadTermQuery enclosingInstance, TokenStream input, System.String fieldName):base(input)
			{
				InitBlock(enclosingInstance);
				this.fieldName = fieldName;
				payloadAtt = (PayloadAttribute) AddAttribute(typeof(PayloadAttribute));
			}
			
			public override bool IncrementToken()
			{
				bool hasNext = input.IncrementToken();
				if (hasNext)
				{
					if (fieldName.Equals("field"))
					{
						payloadAtt.SetPayload(new Payload(Enclosing_Instance.payloadField));
					}
					else if (fieldName.Equals("multiField"))
					{
						if (numSeen % 2 == 0)
						{
							payloadAtt.SetPayload(new Payload(Enclosing_Instance.payloadMultiField1));
						}
						else
						{
							payloadAtt.SetPayload(new Payload(Enclosing_Instance.payloadMultiField2));
						}
						numSeen++;
					}
					return true;
				}
				else
				{
					return false;
				}
			}
		}
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			directory = new RAMDirectory();
			PayloadAnalyzer analyzer = new PayloadAnalyzer(this);
			IndexWriter writer = new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetSimilarity(similarity);
			//writer.infoStream = System.out;
			for (int i = 0; i < 1000; i++)
			{
				Document doc = new Document();
				Field noPayloadField = new Field(PayloadHelper.NO_PAYLOAD_FIELD, English.IntToEnglish(i), Field.Store.YES, Field.Index.ANALYZED);
				//noPayloadField.setBoost(0);
				doc.Add(noPayloadField);
				doc.Add(new Field("field", English.IntToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
				doc.Add(new Field("multiField", English.IntToEnglish(i) + "  " + English.IntToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
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
			PayloadTermQuery query = new PayloadTermQuery(new Term("field", "seventy"), new MaxPayloadFunction());
			TopDocs hits = searcher.Search(query, null, 100);
			Assert.IsTrue(hits != null, "hits is null and it shouldn't be");
			Assert.IsTrue(hits.totalHits == 100, "hits Size: " + hits.totalHits + " is not: " + 100);
			
			//they should all have the exact same score, because they all contain seventy once, and we set
			//all the other similarity factors to be 1
			
			Assert.IsTrue(hits.GetMaxScore() == 1, hits.GetMaxScore() + " does not equal: " + 1);
			for (int i = 0; i < hits.scoreDocs.Length; i++)
			{
				ScoreDoc doc = hits.scoreDocs[i];
				Assert.IsTrue(doc.score == 1, doc.score + " does not equal: " + 1);
			}
			CheckHits.CheckExplanations(query, PayloadHelper.FIELD, searcher, true);
			Lucene.Net.Search.Spans.Spans spans = query.GetSpans(searcher.GetIndexReader());
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			Assert.IsTrue(spans is TermSpans, "spans is not an instanceof " + typeof(TermSpans));
            /*float score = hits.score(0);
            for (int i =1; i < hits.length(); i++)
            {
            Assert.IsTrue(score == hits.score(i), "scores are not equal and they should be");
            }*/
        }
		
        [Test]
		public virtual void  TestQuery()
		{
			PayloadTermQuery boostingFuncTermQuery = new PayloadTermQuery(new Term(PayloadHelper.MULTI_FIELD, "seventy"), new MaxPayloadFunction());
			QueryUtils.Check(boostingFuncTermQuery);
			
			SpanTermQuery spanTermQuery = new SpanTermQuery(new Term(PayloadHelper.MULTI_FIELD, "seventy"));
			
			Assert.IsTrue(boostingFuncTermQuery.Equals(spanTermQuery) == spanTermQuery.Equals(boostingFuncTermQuery));
			
			PayloadTermQuery boostingFuncTermQuery2 = new PayloadTermQuery(new Term(PayloadHelper.MULTI_FIELD, "seventy"), new AveragePayloadFunction());
			
			QueryUtils.CheckUnequal(boostingFuncTermQuery, boostingFuncTermQuery2);
		}
		
        [Test]
		public virtual void  TestMultipleMatchesPerDoc()
		{
			PayloadTermQuery query = new PayloadTermQuery(new Term(PayloadHelper.MULTI_FIELD, "seventy"), new MaxPayloadFunction());
			TopDocs hits = searcher.Search(query, null, 100);
			Assert.IsTrue(hits != null, "hits is null and it shouldn't be");
			Assert.IsTrue(hits.totalHits == 100, "hits Size: " + hits.totalHits + " is not: " + 100);
			
			//they should all have the exact same score, because they all contain seventy once, and we set
			//all the other similarity factors to be 1
			
			//System.out.println("Hash: " + seventyHash + " Twice Hash: " + 2*seventyHash);
			Assert.IsTrue(hits.GetMaxScore() == 4.0, hits.GetMaxScore() + " does not equal: " + 4.0);
			//there should be exactly 10 items that score a 4, all the rest should score a 2
			//The 10 items are: 70 + i*100 where i in [0-9]
			int numTens = 0;
			for (int i = 0; i < hits.scoreDocs.Length; i++)
			{
				ScoreDoc doc = hits.scoreDocs[i];
				if (doc.doc % 10 == 0)
				{
					numTens++;
					Assert.IsTrue(doc.score == 4.0, doc.score + " does not equal: " + 4.0);
				}
				else
				{
					Assert.IsTrue(doc.score == 2, doc.score + " does not equal: " + 2);
				}
			}
			Assert.IsTrue(numTens == 10, numTens + " does not equal: " + 10);
			CheckHits.CheckExplanations(query, "field", searcher, true);
			Lucene.Net.Search.Spans.Spans spans = query.GetSpans(searcher.GetIndexReader());
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			Assert.IsTrue(spans is TermSpans, "spans is not an instanceof " + typeof(TermSpans));
			//should be two matches per document
			int count = 0;
			//100 hits times 2 matches per hit, we should have 200 in count
			while (spans.Next())
			{
				count++;
			}
			Assert.IsTrue(count == 200, count + " does not equal: " + 200);
		}
		
		//Set includeSpanScore to false, in which case just the payload score comes through.
        [Test]
		public virtual void  TestIgnoreSpanScorer()
		{
			PayloadTermQuery query = new PayloadTermQuery(new Term(PayloadHelper.MULTI_FIELD, "seventy"), new MaxPayloadFunction(), false);
			
			IndexSearcher theSearcher = new IndexSearcher(directory, true);
			theSearcher.SetSimilarity(new FullSimilarity());
			TopDocs hits = searcher.Search(query, null, 100);
			Assert.IsTrue(hits != null, "hits is null and it shouldn't be");
			Assert.IsTrue(hits.totalHits == 100, "hits Size: " + hits.totalHits + " is not: " + 100);
			
			//they should all have the exact same score, because they all contain seventy once, and we set
			//all the other similarity factors to be 1
			
			//System.out.println("Hash: " + seventyHash + " Twice Hash: " + 2*seventyHash);
			Assert.IsTrue(hits.GetMaxScore() == 4.0, hits.GetMaxScore() + " does not equal: " + 4.0);
			//there should be exactly 10 items that score a 4, all the rest should score a 2
			//The 10 items are: 70 + i*100 where i in [0-9]
			int numTens = 0;
			for (int i = 0; i < hits.scoreDocs.Length; i++)
			{
				ScoreDoc doc = hits.scoreDocs[i];
				if (doc.doc % 10 == 0)
				{
					numTens++;
					Assert.IsTrue(doc.score == 4.0, doc.score + " does not equal: " + 4.0);
				}
				else
				{
					Assert.IsTrue(doc.score == 2, doc.score + " does not equal: " + 2);
				}
			}
			Assert.IsTrue(numTens == 10, numTens + " does not equal: " + 10);
			CheckHits.CheckExplanations(query, "field", searcher, true);
			Lucene.Net.Search.Spans.Spans spans = query.GetSpans(searcher.GetIndexReader());
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			Assert.IsTrue(spans is TermSpans, "spans is not an instanceof " + typeof(TermSpans));
			//should be two matches per document
			int count = 0;
			//100 hits times 2 matches per hit, we should have 200 in count
			while (spans.Next())
			{
				count++;
			}
		}
		
        [Test]
		public virtual void  TestNoMatch()
		{
			PayloadTermQuery query = new PayloadTermQuery(new Term(PayloadHelper.FIELD, "junk"), new MaxPayloadFunction());
			TopDocs hits = searcher.Search(query, null, 100);
			Assert.IsTrue(hits != null, "hits is null and it shouldn't be");
			Assert.IsTrue(hits.totalHits == 0, "hits Size: " + hits.totalHits + " is not: " + 0);
		}
		
        [Test]
		public virtual void  TestNoPayload()
		{
			PayloadTermQuery q1 = new PayloadTermQuery(new Term(PayloadHelper.NO_PAYLOAD_FIELD, "zero"), new MaxPayloadFunction());
			PayloadTermQuery q2 = new PayloadTermQuery(new Term(PayloadHelper.NO_PAYLOAD_FIELD, "foo"), new MaxPayloadFunction());
			BooleanClause c1 = new BooleanClause(q1, BooleanClause.Occur.MUST);
			BooleanClause c2 = new BooleanClause(q2, BooleanClause.Occur.MUST_NOT);
			BooleanQuery query = new BooleanQuery();
			query.Add(c1);
			query.Add(c2);
			TopDocs hits = searcher.Search(query, null, 100);
			Assert.IsTrue(hits != null, "hits is null and it shouldn't be");
			Assert.IsTrue(hits.totalHits == 1, "hits Size: " + hits.totalHits + " is not: " + 1);
			int[] results = new int[1];
			results[0] = 0; //hits.scoreDocs[0].doc;
			CheckHits.CheckHitCollector(query, PayloadHelper.NO_PAYLOAD_FIELD, searcher, results);
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
			
			public override float Idf(int docFreq, int numDocs)
			{
				return 1;
			}
			
			public override float Tf(float freq)
			{
				return freq == 0?0:1;
			}
		}
		
		[Serializable]
		internal class FullSimilarity:DefaultSimilarity
		{
			public virtual float ScorePayload(int docId, System.String fieldName, byte[] payload, int offset, int length)
			{
				//we know it is size 4 here, so ignore the offset/length
				return payload[0];
			}
		}
	}
}