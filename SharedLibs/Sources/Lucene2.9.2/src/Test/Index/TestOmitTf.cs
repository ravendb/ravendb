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
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using BooleanQuery = Lucene.Net.Search.BooleanQuery;
using Collector = Lucene.Net.Search.Collector;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Scorer = Lucene.Net.Search.Scorer;
using Searcher = Lucene.Net.Search.Searcher;
using Similarity = Lucene.Net.Search.Similarity;
using TermQuery = Lucene.Net.Search.TermQuery;
using Occur = Lucene.Net.Search.BooleanClause.Occur;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Index
{
	
	
    [TestFixture]
	public class TestOmitTf:LuceneTestCase
	{
		private class AnonymousClassCountingHitCollector:CountingHitCollector
		{
			public AnonymousClassCountingHitCollector(TestOmitTf enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestOmitTf enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestOmitTf enclosingInstance;
			public TestOmitTf Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private Scorer scorer;
			public override void  SetScorer(Scorer scorer)
			{
				this.scorer = scorer;
			}
			public override void  Collect(int doc)
			{
				//System.out.println("Q1: Doc=" + doc + " score=" + score);
				float score = scorer.Score();
				Assert.IsTrue(score == 1.0f);
				base.Collect(doc);
			}
		}
		
		private class AnonymousClassCountingHitCollector1:CountingHitCollector
		{
			public AnonymousClassCountingHitCollector1(TestOmitTf enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestOmitTf enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestOmitTf enclosingInstance;
			public TestOmitTf Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private Scorer scorer;
			public override void  SetScorer(Scorer scorer)
			{
				this.scorer = scorer;
			}
			public override void  Collect(int doc)
			{
				//System.out.println("Q2: Doc=" + doc + " score=" + score);
				float score = scorer.Score();
				Assert.IsTrue(score == 1.0f + doc);
				base.Collect(doc);
			}
		}
		
		private class AnonymousClassCountingHitCollector2:CountingHitCollector
		{
			public AnonymousClassCountingHitCollector2(TestOmitTf enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestOmitTf enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestOmitTf enclosingInstance;
			public TestOmitTf Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private Scorer scorer;
			public override void  SetScorer(Scorer scorer)
			{
				this.scorer = scorer;
			}
			public override void  Collect(int doc)
			{
				//System.out.println("Q1: Doc=" + doc + " score=" + score);
				float score = scorer.Score();
				Assert.IsTrue(score == 1.0f);
				Assert.IsFalse(doc % 2 == 0);
				base.Collect(doc);
			}
		}
		
		private class AnonymousClassCountingHitCollector3:CountingHitCollector
		{
			public AnonymousClassCountingHitCollector3(TestOmitTf enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestOmitTf enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestOmitTf enclosingInstance;
			public TestOmitTf Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private Scorer scorer;
			public override void  SetScorer(Scorer scorer)
			{
				this.scorer = scorer;
			}
			public override void  Collect(int doc)
			{
				float score = scorer.Score();
				//System.out.println("Q1: Doc=" + doc + " score=" + score);
				Assert.IsTrue(score == 1.0f);
				Assert.IsTrue(doc % 2 == 0);
				base.Collect(doc);
			}
		}
		
		private class AnonymousClassCountingHitCollector4:CountingHitCollector
		{
			public AnonymousClassCountingHitCollector4(TestOmitTf enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestOmitTf enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestOmitTf enclosingInstance;
			public TestOmitTf Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override void  Collect(int doc)
			{
				//System.out.println("BQ: Doc=" + doc + " score=" + score);
				base.Collect(doc);
			}
		}
		
		[Serializable]
		public class SimpleSimilarity:Similarity
		{
			public override float LengthNorm(System.String field, int numTerms)
			{
				return 1.0f;
			}
			public override float QueryNorm(float sumOfSquaredWeights)
			{
				return 1.0f;
			}
			
			public override float Tf(float freq)
			{
				return freq;
			}
			
			public override float SloppyFreq(int distance)
			{
				return 2.0f;
			}
			public override float Idf(System.Collections.ICollection terms, Searcher searcher)
			{
				return 1.0f;
			}
			public override float Idf(int docFreq, int numDocs)
			{
				return 1.0f;
			}
			public override float Coord(int overlap, int maxOverlap)
			{
				return 1.0f;
			}
		}
		
		
		// Tests whether the DocumentWriter correctly enable the
		// omitTermFreqAndPositions bit in the FieldInfo
		public virtual void  TestOmitTermFreqAndPositions()
		{
			Directory ram = new MockRAMDirectory();
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriter writer = new IndexWriter(ram, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			Document d = new Document();
			
			// this field will have Tf
			Field f1 = new Field("f1", "This field has term freqs", Field.Store.NO, Field.Index.ANALYZED);
			d.Add(f1);
			
			// this field will NOT have Tf
			Field f2 = new Field("f2", "This field has NO Tf in all docs", Field.Store.NO, Field.Index.ANALYZED);
			f2.SetOmitTermFreqAndPositions(true);
			d.Add(f2);
			
			writer.AddDocument(d);
			writer.Optimize();
			// now we add another document which has term freq for field f2 and not for f1 and verify if the SegmentMerger
			// keep things constant
			d = new Document();
			
			// Reverese
			f1.SetOmitTermFreqAndPositions(true);
			d.Add(f1);
			
			f2.SetOmitTermFreqAndPositions(false);
			d.Add(f2);
			
			writer.AddDocument(d);
			// force merge
			writer.Optimize();
			// flush
			writer.Close();
			_TestUtil.CheckIndex(ram);
			
			SegmentReader reader = SegmentReader.GetOnlySegmentReader(ram);
			FieldInfos fi = reader.FieldInfos();
			Assert.IsTrue(fi.FieldInfo("f1").omitTermFreqAndPositions_ForNUnit, "OmitTermFreqAndPositions field bit should be set.");
			Assert.IsTrue(fi.FieldInfo("f2").omitTermFreqAndPositions_ForNUnit, "OmitTermFreqAndPositions field bit should be set.");
			
			reader.Close();
			ram.Close();
		}
		
		// Tests whether merging of docs that have different
		// omitTermFreqAndPositions for the same field works
		[Test]
		public virtual void  TestMixedMerge()
		{
			Directory ram = new MockRAMDirectory();
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriter writer = new IndexWriter(ram, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(3);
			writer.SetMergeFactor(2);
			Document d = new Document();
			
			// this field will have Tf
			Field f1 = new Field("f1", "This field has term freqs", Field.Store.NO, Field.Index.ANALYZED);
			d.Add(f1);
			
			// this field will NOT have Tf
			Field f2 = new Field("f2", "This field has NO Tf in all docs", Field.Store.NO, Field.Index.ANALYZED);
			f2.SetOmitTermFreqAndPositions(true);
			d.Add(f2);
			
			for (int i = 0; i < 30; i++)
				writer.AddDocument(d);
			
			// now we add another document which has term freq for field f2 and not for f1 and verify if the SegmentMerger
			// keep things constant
			d = new Document();
			
			// Reverese
			f1.SetOmitTermFreqAndPositions(true);
			d.Add(f1);
			
			f2.SetOmitTermFreqAndPositions(false);
			d.Add(f2);
			
			for (int i = 0; i < 30; i++)
				writer.AddDocument(d);
			
			// force merge
			writer.Optimize();
			// flush
			writer.Close();
			
			_TestUtil.CheckIndex(ram);
			
			SegmentReader reader = SegmentReader.GetOnlySegmentReader(ram);
			FieldInfos fi = reader.FieldInfos();
			Assert.IsTrue(fi.FieldInfo("f1").omitTermFreqAndPositions_ForNUnit, "OmitTermFreqAndPositions field bit should be set.");
			Assert.IsTrue(fi.FieldInfo("f2").omitTermFreqAndPositions_ForNUnit, "OmitTermFreqAndPositions field bit should be set.");
			
			reader.Close();
			ram.Close();
		}
		
		// Make sure first adding docs that do not omitTermFreqAndPositions for
		// field X, then adding docs that do omitTermFreqAndPositions for that same
		// field, 
		[Test]
		public virtual void  TestMixedRAM()
		{
			Directory ram = new MockRAMDirectory();
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriter writer = new IndexWriter(ram, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(10);
			writer.SetMergeFactor(2);
			Document d = new Document();
			
			// this field will have Tf
			Field f1 = new Field("f1", "This field has term freqs", Field.Store.NO, Field.Index.ANALYZED);
			d.Add(f1);
			
			// this field will NOT have Tf
			Field f2 = new Field("f2", "This field has NO Tf in all docs", Field.Store.NO, Field.Index.ANALYZED);
			d.Add(f2);
			
			for (int i = 0; i < 5; i++)
				writer.AddDocument(d);
			
			f2.SetOmitTermFreqAndPositions(true);
			
			for (int i = 0; i < 20; i++)
				writer.AddDocument(d);
			
			// force merge
			writer.Optimize();
			
			// flush
			writer.Close();
			
			_TestUtil.CheckIndex(ram);
			
			SegmentReader reader = SegmentReader.GetOnlySegmentReader(ram);
			FieldInfos fi = reader.FieldInfos();
			Assert.IsTrue(!fi.FieldInfo("f1").omitTermFreqAndPositions_ForNUnit, "OmitTermFreqAndPositions field bit should not be set.");
			Assert.IsTrue(fi.FieldInfo("f2").omitTermFreqAndPositions_ForNUnit, "OmitTermFreqAndPositions field bit should be set.");
			
			reader.Close();
			ram.Close();
		}
		
		private void  AssertNoPrx(Directory dir)
		{
			System.String[] files = dir.ListAll();
			for (int i = 0; i < files.Length; i++)
				Assert.IsFalse(files[i].EndsWith(".prx"));
		}
		
		// Verifies no *.prx exists when all fields omit term freq:
		[Test]
		public virtual void  TestNoPrxFile()
		{
			Directory ram = new MockRAMDirectory();
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriter writer = new IndexWriter(ram, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(3);
			writer.SetMergeFactor(2);
			writer.SetUseCompoundFile(false);
			Document d = new Document();
			
			Field f1 = new Field("f1", "This field has term freqs", Field.Store.NO, Field.Index.ANALYZED);
			f1.SetOmitTermFreqAndPositions(true);
			d.Add(f1);
			
			for (int i = 0; i < 30; i++)
				writer.AddDocument(d);
			
			writer.Commit();
			
			AssertNoPrx(ram);
			
			// force merge
			writer.Optimize();
			// flush
			writer.Close();
			
			AssertNoPrx(ram);
			_TestUtil.CheckIndex(ram);
			ram.Close();
		}
		
		// Test scores with one field with Term Freqs and one without, otherwise with equal content 
		[Test]
		public virtual void  TestBasic()
		{
			Directory dir = new MockRAMDirectory();
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMergeFactor(2);
			writer.SetMaxBufferedDocs(2);
			writer.SetSimilarity(new SimpleSimilarity());
			
			
			System.Text.StringBuilder sb = new System.Text.StringBuilder(265);
			System.String term = "term";
			for (int i = 0; i < 30; i++)
			{
				Document d = new Document();
				sb.Append(term).Append(" ");
				System.String content = sb.ToString();
				Field noTf = new Field("noTf", content + (i % 2 == 0?"":" notf"), Field.Store.NO, Field.Index.ANALYZED);
				noTf.SetOmitTermFreqAndPositions(true);
				d.Add(noTf);
				
				Field tf = new Field("tf", content + (i % 2 == 0?" tf":""), Field.Store.NO, Field.Index.ANALYZED);
				d.Add(tf);
				
				writer.AddDocument(d);
				//System.out.println(d);
			}
			
			writer.Optimize();
			// flush
			writer.Close();
			_TestUtil.CheckIndex(dir);
			
			/*
			* Verify the index
			*/
			Searcher searcher = new IndexSearcher(dir);
			searcher.SetSimilarity(new SimpleSimilarity());
			
			Term a = new Term("noTf", term);
			Term b = new Term("tf", term);
			Term c = new Term("noTf", "notf");
			Term d2 = new Term("tf", "tf");
			TermQuery q1 = new TermQuery(a);
			TermQuery q2 = new TermQuery(b);
			TermQuery q3 = new TermQuery(c);
			TermQuery q4 = new TermQuery(d2);
			
			
			searcher.Search(q1, new AnonymousClassCountingHitCollector(this));
			//System.out.println(CountingHitCollector.getCount());
			
			
			searcher.Search(q2, new AnonymousClassCountingHitCollector1(this));
			//System.out.println(CountingHitCollector.getCount());
			
			
			
			
			
			searcher.Search(q3, new AnonymousClassCountingHitCollector2(this));
			//System.out.println(CountingHitCollector.getCount());
			
			
			searcher.Search(q4, new AnonymousClassCountingHitCollector3(this));
			//System.out.println(CountingHitCollector.getCount());
			
			
			
			BooleanQuery bq = new BooleanQuery();
			bq.Add(q1, Occur.MUST);
			bq.Add(q4, Occur.MUST);
			
			searcher.Search(bq, new AnonymousClassCountingHitCollector4(this));
			Assert.IsTrue(15 == CountingHitCollector.GetCount());
			
			searcher.Close();
			dir.Close();
		}
		
		public class CountingHitCollector:Collector
		{
			internal static int count = 0;
			internal static int sum = 0;
			private int docBase = - 1;
			internal CountingHitCollector()
			{
				count = 0; sum = 0;
			}
			public override void  SetScorer(Scorer scorer)
			{
			}
			public override void  Collect(int doc)
			{
				count++;
				sum += doc + docBase; // use it to avoid any possibility of being optimized away
			}
			
			public static int GetCount()
			{
				return count;
			}
			public static int GetSum()
			{
				return sum;
			}
			
			public override void  SetNextReader(IndexReader reader, int docBase)
			{
				this.docBase = docBase;
			}
			public override bool AcceptsDocsOutOfOrder()
			{
				return true;
			}
		}
	}
}