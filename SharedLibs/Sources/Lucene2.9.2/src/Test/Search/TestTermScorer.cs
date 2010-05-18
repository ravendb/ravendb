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
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestTermScorer:LuceneTestCase
	{
		private class AnonymousClassCollector:Collector
		{
			public AnonymousClassCollector(System.Collections.IList docs, TestTermScorer enclosingInstance)
			{
				InitBlock(docs, enclosingInstance);
			}
			private void  InitBlock(System.Collections.IList docs, TestTermScorer enclosingInstance)
			{
				this.docs = docs;
				this.enclosingInstance = enclosingInstance;
			}
			private System.Collections.IList docs;
			private TestTermScorer enclosingInstance;
			public TestTermScorer Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private int base_Renamed = 0;
			private Scorer scorer;
			public override void  SetScorer(Scorer scorer)
			{
				this.scorer = scorer;
			}
			
			public override void  Collect(int doc)
			{
				float score = scorer.Score();
				doc = doc + base_Renamed;
				docs.Add(new TestHit(enclosingInstance, doc, score));
				Assert.IsTrue(score > 0, "score " + score + " is not greater than 0");
				Assert.IsTrue(doc == 0 || doc == 5, "Doc: " + doc + " does not equal 0 or doc does not equal 5");
			}
			public override void  SetNextReader(IndexReader reader, int docBase)
			{
				base_Renamed = docBase;
			}
			public override bool AcceptsDocsOutOfOrder()
			{
				return true;
			}
		}
		protected internal RAMDirectory directory;
		private const System.String FIELD = "field";
		
		protected internal System.String[] values = new System.String[]{"all", "dogs dogs", "like", "playing", "fetch", "all"};
		protected internal IndexSearcher indexSearcher;
		protected internal IndexReader indexReader;
		
				
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			directory = new RAMDirectory();
			
			
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < values.Length; i++)
			{
				Document doc = new Document();
				doc.Add(new Field(FIELD, values[i], Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
			writer.Close();
			indexSearcher = new IndexSearcher(directory);
			indexReader = indexSearcher.GetIndexReader();
		}
		
		[Test]
		public virtual void  Test()
		{
			
			Term allTerm = new Term(FIELD, "all");
			TermQuery termQuery = new TermQuery(allTerm);
			
			Weight weight = termQuery.Weight(indexSearcher);
			
			TermScorer ts = new TermScorer(weight, indexReader.TermDocs(allTerm), indexSearcher.GetSimilarity(), indexReader.Norms(FIELD));
			//we have 2 documents with the term all in them, one document for all the other values
			System.Collections.IList docs = new System.Collections.ArrayList();
			//must call next first
			
			
			ts.Score(new AnonymousClassCollector(docs, this));
			Assert.IsTrue(docs.Count == 2, "docs Size: " + docs.Count + " is not: " + 2);
			TestHit doc0 = (TestHit) docs[0];
			TestHit doc5 = (TestHit) docs[1];
			//The scores should be the same
			Assert.IsTrue(doc0.score == doc5.score, doc0.score + " does not equal: " + doc5.score);
			/*
			Score should be (based on Default Sim.:
			All floats are approximate
			tf = 1
			numDocs = 6
			docFreq(all) = 2
			idf = ln(6/3) + 1 = 1.693147
			idf ^ 2 = 2.8667
			boost = 1
			lengthNorm = 1 //there is 1 term in every document
			coord = 1
			sumOfSquaredWeights = (idf * boost) ^ 2 = 1.693147 ^ 2 = 2.8667
			queryNorm = 1 / (sumOfSquaredWeights)^0.5 = 1 /(1.693147) = 0.590
			
			score = 1 * 2.8667 * 1 * 1 * 0.590 = 1.69
			
			*/
			Assert.IsTrue(doc0.score == 1.6931472f, doc0.score + " does not equal: " + 1.6931472f);
		}
		
		[Test]
		public virtual void  TestNext()
		{
			
			Term allTerm = new Term(FIELD, "all");
			TermQuery termQuery = new TermQuery(allTerm);
			
			Weight weight = termQuery.Weight(indexSearcher);
			
			TermScorer ts = new TermScorer(weight, indexReader.TermDocs(allTerm), indexSearcher.GetSimilarity(), indexReader.Norms(FIELD));
			Assert.IsTrue(ts.NextDoc() != DocIdSetIterator.NO_MORE_DOCS, "next did not return a doc");
			Assert.IsTrue(ts.Score() == 1.6931472f, "score is not correct");
			Assert.IsTrue(ts.NextDoc() != DocIdSetIterator.NO_MORE_DOCS, "next did not return a doc");
			Assert.IsTrue(ts.Score() == 1.6931472f, "score is not correct");
			Assert.IsTrue(ts.NextDoc() == DocIdSetIterator.NO_MORE_DOCS, "next returned a doc and it should not have");
		}
		
		[Test]
		public virtual void  TestSkipTo()
		{
			
			Term allTerm = new Term(FIELD, "all");
			TermQuery termQuery = new TermQuery(allTerm);
			
			Weight weight = termQuery.Weight(indexSearcher);
			
			TermScorer ts = new TermScorer(weight, indexReader.TermDocs(allTerm), indexSearcher.GetSimilarity(), indexReader.Norms(FIELD));
			Assert.IsTrue(ts.Advance(3) != DocIdSetIterator.NO_MORE_DOCS, "Didn't skip");
			//The next doc should be doc 5
			Assert.IsTrue(ts.DocID() == 5, "doc should be number 5");
		}
		
		[Test]
		public virtual void  TestExplain()
		{
			Term allTerm = new Term(FIELD, "all");
			TermQuery termQuery = new TermQuery(allTerm);
			
			Weight weight = termQuery.Weight(indexSearcher);
			
			TermScorer ts = new TermScorer(weight, indexReader.TermDocs(allTerm), indexSearcher.GetSimilarity(), indexReader.Norms(FIELD));
			Explanation explanation = ts.Explain(0);
			Assert.IsTrue(explanation != null, "explanation is null and it shouldn't be");
			//System.out.println("Explanation: " + explanation.toString());
			//All this Explain does is return the term frequency
			Assert.IsTrue(explanation.GetValue() == 1, "term frq is not 1");
			explanation = ts.Explain(1);
			Assert.IsTrue(explanation != null, "explanation is null and it shouldn't be");
			//System.out.println("Explanation: " + explanation.toString());
			//All this Explain does is return the term frequency
			Assert.IsTrue(explanation.GetValue() == 0, "term frq is not 0");
			
			Term dogsTerm = new Term(FIELD, "dogs");
			termQuery = new TermQuery(dogsTerm);
			weight = termQuery.Weight(indexSearcher);
			
			ts = new TermScorer(weight, indexReader.TermDocs(dogsTerm), indexSearcher.GetSimilarity(), indexReader.Norms(FIELD));
			explanation = ts.Explain(1);
			Assert.IsTrue(explanation != null, "explanation is null and it shouldn't be");
			//System.out.println("Explanation: " + explanation.toString());
			//All this Explain does is return the term frequency
			float sqrtTwo = (float) System.Math.Sqrt(2.0f);
			Assert.IsTrue(explanation.GetValue() == sqrtTwo, "term frq: " + explanation.GetValue() + " is not the square root of 2");
			
			explanation = ts.Explain(10); //try a doc out of range
			Assert.IsTrue(explanation != null, "explanation is null and it shouldn't be");
			//System.out.println("Explanation: " + explanation.toString());
			//All this Explain does is return the term frequency
			
			Assert.IsTrue(explanation.GetValue() == 0, "term frq: " + explanation.GetValue() + " is not 0");
		}
		
		private class TestHit
		{
			private void  InitBlock(TestTermScorer enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTermScorer enclosingInstance;
			public TestTermScorer Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public int doc;
			public float score;
			
			public TestHit(TestTermScorer enclosingInstance, int doc, float score)
			{
				InitBlock(enclosingInstance);
				this.doc = doc;
				this.score = score;
			}
			
			public override System.String ToString()
			{
				return "TestHit{" + "doc=" + doc + ", score=" + score + "}";
			}
		}
	}
}