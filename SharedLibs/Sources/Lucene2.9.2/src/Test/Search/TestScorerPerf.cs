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
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using DocIdBitSet = Lucene.Net.Util.DocIdBitSet;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> </summary>
	/// <version>  $Id$
	/// </version>
    [TestFixture]
	public class TestScorerPerf:LuceneTestCase
	{
		[Serializable]
		private class AnonymousClassFilter:Filter
		{
			public AnonymousClassFilter(System.Collections.BitArray rnd, TestScorerPerf enclosingInstance)
			{
				InitBlock(rnd, enclosingInstance);
			}
			private void  InitBlock(System.Collections.BitArray rnd, TestScorerPerf enclosingInstance)
			{
				this.rnd = rnd;
				this.enclosingInstance = enclosingInstance;
			}
			private System.Collections.BitArray rnd;
			private TestScorerPerf enclosingInstance;
			public TestScorerPerf Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override DocIdSet GetDocIdSet(IndexReader reader)
			{
				return new DocIdBitSet(rnd);
			}
			
		}
		internal System.Random r;
		internal bool validate = true; // set to false when doing performance testing
		
		internal System.Collections.BitArray[] sets;
		internal Term[] terms;
		internal IndexSearcher s;
		
		public virtual void  CreateDummySearcher()
		{
			// Create a dummy index with nothing in it.
			// This could possibly fail if Lucene starts checking for docid ranges...
			RAMDirectory rd = new RAMDirectory();
			IndexWriter iw = new IndexWriter(rd, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			iw.AddDocument(new Document());
			iw.Close();
			s = new IndexSearcher(rd);
		}
		
		public virtual void  CreateRandomTerms(int nDocs, int nTerms, double power, Directory dir)
		{
			int[] freq = new int[nTerms];
			terms = new Term[nTerms];
			for (int i = 0; i < nTerms; i++)
			{
				int f = (nTerms + 1) - i; // make first terms less frequent
				freq[i] = (int) System.Math.Ceiling(System.Math.Pow(f, power));
				terms[i] = new Term("f", System.Convert.ToString((char) ('A' + i)));
			}
			
			IndexWriter iw = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < nDocs; i++)
			{
				Document d = new Document();
				for (int j = 0; j < nTerms; j++)
				{
					if (r.Next(freq[j]) == 0)
					{
						d.Add(new Field("f", terms[j].Text(), Field.Store.NO, Field.Index.NOT_ANALYZED));
						//System.out.println(d);
					}
				}
				iw.AddDocument(d);
			}
			iw.Optimize();
			iw.Close();
		}
		
		
		public virtual System.Collections.BitArray RandBitSet(int sz, int numBitsToSet)
		{
			System.Collections.BitArray set_Renamed = new System.Collections.BitArray((sz % 64 == 0?sz / 64:sz / 64 + 1) * 64);
			for (int i = 0; i < numBitsToSet; i++)
			{
				set_Renamed.Set(r.Next(sz), true);
			}
			return set_Renamed;
		}
		
		public virtual System.Collections.BitArray[] RandBitSets(int numSets, int setSize)
		{
			System.Collections.BitArray[] sets = new System.Collections.BitArray[numSets];
			for (int i = 0; i < sets.Length; i++)
			{
				sets[i] = RandBitSet(setSize, r.Next(setSize));
			}
			return sets;
		}
		
		public class CountingHitCollector:Collector
		{
			internal int count = 0;
			internal int sum = 0;
			protected internal int docBase = 0;
			
			public override void  SetScorer(Scorer scorer)
			{
			}
			
			public override void  Collect(int doc)
			{
				count++;
				sum += docBase + doc; // use it to avoid any possibility of being optimized away
			}
			
			public virtual int GetCount()
			{
				return count;
			}
			public virtual int GetSum()
			{
				return sum;
			}
			
			public override void  SetNextReader(IndexReader reader, int base_Renamed)
			{
				docBase = base_Renamed;
			}
			public override bool AcceptsDocsOutOfOrder()
			{
				return true;
			}
		}
		
		
		public class MatchingHitCollector:CountingHitCollector
		{
			internal System.Collections.BitArray answer;
			internal int pos = - 1;
			public MatchingHitCollector(System.Collections.BitArray answer)
			{
				this.answer = answer;
			}
			
			public virtual void  Collect(int doc, float score)
			{
				
				pos = SupportClass.BitSetSupport.NextSetBit(answer, pos + 1);
				if (pos != doc + docBase)
				{
					throw new System.SystemException("Expected doc " + pos + " but got " + doc + docBase);
				}
				base.Collect(doc);
			}
		}
		
		
		internal virtual System.Collections.BitArray AddClause(BooleanQuery bq, System.Collections.BitArray result)
		{
			System.Collections.BitArray rnd = sets[r.Next(sets.Length)];
			Query q = new ConstantScoreQuery(new AnonymousClassFilter(rnd, this));
			bq.Add(q, BooleanClause.Occur.MUST);
			if (validate)
			{
				if (result == null)
					result = (System.Collections.BitArray) rnd.Clone();
				else
				{
					result.And(rnd);
				}
			}
			return result;
		}
		
		
		public virtual int DoConjunctions(int iter, int maxClauses)
		{
			int ret = 0;
			
			for (int i = 0; i < iter; i++)
			{
				int nClauses = r.Next(maxClauses - 1) + 2; // min 2 clauses
				BooleanQuery bq = new BooleanQuery();
				System.Collections.BitArray result = null;
				for (int j = 0; j < nClauses; j++)
				{
					result = AddClause(bq, result);
				}
				
				CountingHitCollector hc = validate?new MatchingHitCollector(result):new CountingHitCollector();
				s.Search(bq, hc);
				ret += hc.GetSum();
				
				if (validate)
					Assert.AreEqual(SupportClass.BitSetSupport.Cardinality(result), hc.GetCount());
				// System.out.println(hc.getCount());
			}
			
			return ret;
		}
		
		public virtual int DoNestedConjunctions(int iter, int maxOuterClauses, int maxClauses)
		{
			int ret = 0;
			long nMatches = 0;
			
			for (int i = 0; i < iter; i++)
			{
				int oClauses = r.Next(maxOuterClauses - 1) + 2;
				BooleanQuery oq = new BooleanQuery();
				System.Collections.BitArray result = null;
				
				for (int o = 0; o < oClauses; o++)
				{
					
					int nClauses = r.Next(maxClauses - 1) + 2; // min 2 clauses
					BooleanQuery bq = new BooleanQuery();
					for (int j = 0; j < nClauses; j++)
					{
						result = AddClause(bq, result);
					}
					
					oq.Add(bq, BooleanClause.Occur.MUST);
				} // outer
				
				CountingHitCollector hc = validate?new MatchingHitCollector(result):new CountingHitCollector();
				s.Search(oq, hc);
				nMatches += hc.GetCount();
				ret += hc.GetSum();
				if (validate)
					Assert.AreEqual(SupportClass.BitSetSupport.Cardinality(result), hc.GetCount());
				// System.out.println(hc.getCount());
			}
			System.Console.Out.WriteLine("Average number of matches=" + (nMatches / iter));
			return ret;
		}
		
		
		public virtual int DoTermConjunctions(IndexSearcher s, int termsInIndex, int maxClauses, int iter)
		{
			int ret = 0;
			
			long nMatches = 0;
			for (int i = 0; i < iter; i++)
			{
				int nClauses = r.Next(maxClauses - 1) + 2; // min 2 clauses
				BooleanQuery bq = new BooleanQuery();
				System.Collections.BitArray termflag = new System.Collections.BitArray((termsInIndex % 64 == 0?termsInIndex / 64:termsInIndex / 64 + 1) * 64);
				for (int j = 0; j < nClauses; j++)
				{
					int tnum;
					// don't pick same clause twice
					tnum = r.Next(termsInIndex);
					if (termflag.Get(tnum))
						tnum = SupportClass.BitSetSupport.NextClearBit(termflag, tnum);
					if (tnum < 0 || tnum >= termsInIndex)
						tnum = SupportClass.BitSetSupport.NextClearBit(termflag, 0);
					termflag.Set(tnum, true);
					Query tq = new TermQuery(terms[tnum]);
					bq.Add(tq, BooleanClause.Occur.MUST);
				}
				
				CountingHitCollector hc = new CountingHitCollector();
				s.Search(bq, hc);
				nMatches += hc.GetCount();
				ret += hc.GetSum();
			}
			System.Console.Out.WriteLine("Average number of matches=" + (nMatches / iter));
			
			return ret;
		}
		
		
		public virtual int DoNestedTermConjunctions(IndexSearcher s, int termsInIndex, int maxOuterClauses, int maxClauses, int iter)
		{
			int ret = 0;
			long nMatches = 0;
			for (int i = 0; i < iter; i++)
			{
				int oClauses = r.Next(maxOuterClauses - 1) + 2;
				BooleanQuery oq = new BooleanQuery();
				for (int o = 0; o < oClauses; o++)
				{
					
					int nClauses = r.Next(maxClauses - 1) + 2; // min 2 clauses
					BooleanQuery bq = new BooleanQuery();
					System.Collections.BitArray termflag = new System.Collections.BitArray((termsInIndex % 64 == 0?termsInIndex / 64:termsInIndex / 64 + 1) * 64);
					for (int j = 0; j < nClauses; j++)
					{
						int tnum;
						// don't pick same clause twice
						tnum = r.Next(termsInIndex);
						if (termflag.Get(tnum))
							tnum = SupportClass.BitSetSupport.NextClearBit(termflag, tnum);
						if (tnum < 0 || tnum >= 25)
							tnum = SupportClass.BitSetSupport.NextClearBit(termflag, 0);
						termflag.Set(tnum, true);
						Query tq = new TermQuery(terms[tnum]);
						bq.Add(tq, BooleanClause.Occur.MUST);
					} // inner
					
					oq.Add(bq, BooleanClause.Occur.MUST);
				} // outer
				
				
				CountingHitCollector hc = new CountingHitCollector();
				s.Search(oq, hc);
				nMatches += hc.GetCount();
				ret += hc.GetSum();
			}
			System.Console.Out.WriteLine("Average number of matches=" + (nMatches / iter));
			return ret;
		}
		
		
		public virtual int DoSloppyPhrase(IndexSearcher s, int termsInIndex, int maxClauses, int iter)
		{
			int ret = 0;
			
			for (int i = 0; i < iter; i++)
			{
				int nClauses = r.Next(maxClauses - 1) + 2; // min 2 clauses
				PhraseQuery q = new PhraseQuery();
				for (int j = 0; j < nClauses; j++)
				{
					int tnum = r.Next(termsInIndex);
					q.Add(new Term("f", System.Convert.ToString((char) (tnum + 'A'))), j);
				}
				q.SetSlop(termsInIndex); // this could be random too
				
				CountingHitCollector hc = new CountingHitCollector();
				s.Search(q, hc);
				ret += hc.GetSum();
			}
			
			return ret;
		}
		
		
		[Test]
		public virtual void  TestConjunctions()
		{
			// test many small sets... the bugs will be found on boundary conditions
			r = NewRandom();
			CreateDummySearcher();
			validate = true;
			sets = RandBitSets(1000, 10);
			DoConjunctions(10000, 5);
			DoNestedConjunctions(10000, 3, 3);
			s.Close();
		}
		
		/// <summary> 
		/// int bigIter=10;
		/// public void testConjunctionPerf() throws Exception {
		/// r = newRandom();
		/// createDummySearcher();
		/// validate=false;
		/// sets=randBitSets(32,1000000);
		/// for (int i=0; i<bigIter; i++) {
		/// long start = System.currentTimeMillis();
		/// doConjunctions(500,6);
		/// long end = System.currentTimeMillis();
		/// System.out.println("milliseconds="+(end-start));
		/// }
		/// s.close();
		/// }
		/// public void testNestedConjunctionPerf() throws Exception {
		/// r = newRandom();
		/// createDummySearcher();
		/// validate=false;
		/// sets=randBitSets(32,1000000);
		/// for (int i=0; i<bigIter; i++) {
		/// long start = System.currentTimeMillis();
		/// doNestedConjunctions(500,3,3);
		/// long end = System.currentTimeMillis();
		/// System.out.println("milliseconds="+(end-start));
		/// }
		/// s.close();
		/// }
		/// public void testConjunctionTerms() throws Exception {
		/// r = newRandom();
		/// validate=false;
		/// RAMDirectory dir = new RAMDirectory();
		/// System.out.println("Creating index");
		/// createRandomTerms(100000,25,.5, dir);
		/// s = new IndexSearcher(dir);
		/// System.out.println("Starting performance test");
		/// for (int i=0; i<bigIter; i++) {
		/// long start = System.currentTimeMillis();
		/// doTermConjunctions(s,25,5,1000);
		/// long end = System.currentTimeMillis();
		/// System.out.println("milliseconds="+(end-start));
		/// }
		/// s.close();
		/// }
		/// public void testNestedConjunctionTerms() throws Exception {
		/// r = newRandom();
		/// validate=false;    
		/// RAMDirectory dir = new RAMDirectory();
		/// System.out.println("Creating index");
		/// createRandomTerms(100000,25,.2, dir);
		/// s = new IndexSearcher(dir);
		/// System.out.println("Starting performance test");
		/// for (int i=0; i<bigIter; i++) {
		/// long start = System.currentTimeMillis();
		/// doNestedTermConjunctions(s,25,3,3,200);
		/// long end = System.currentTimeMillis();
		/// System.out.println("milliseconds="+(end-start));
		/// }
		/// s.close();
		/// }
		/// public void testSloppyPhrasePerf() throws Exception {
		/// r = newRandom();
		/// validate=false;    
		/// RAMDirectory dir = new RAMDirectory();
		/// System.out.println("Creating index");
		/// createRandomTerms(100000,25,2,dir);
		/// s = new IndexSearcher(dir);
		/// System.out.println("Starting performance test");
		/// for (int i=0; i<bigIter; i++) {
		/// long start = System.currentTimeMillis();
		/// doSloppyPhrase(s,25,2,1000);
		/// long end = System.currentTimeMillis();
		/// System.out.println("milliseconds="+(end-start));
		/// }
		/// s.close();
		/// }
		/// *
		/// </summary>
	}
}