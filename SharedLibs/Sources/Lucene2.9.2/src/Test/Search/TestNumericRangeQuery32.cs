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
using NumericField = Lucene.Net.Documents.NumericField;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using MaxFieldLength = Lucene.Net.Index.IndexWriter.MaxFieldLength;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using NumericUtils = Lucene.Net.Util.NumericUtils;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestNumericRangeQuery32:LuceneTestCase
	{
		// distance of entries
		private const int distance = 6666;
		// shift the starting of the values to the left, to also have negative values:
		private const int startOffset = - 1 << 15;
		// number of docs to generate for testing
		private const int noDocs = 10000;
		
		private static RAMDirectory directory;
		private static IndexSearcher searcher;
		
		/// <summary>test for both constant score and boolean query, the other tests only use the constant score mode </summary>
		private void  TestRange(int precisionStep)
		{
			System.String field = "field" + precisionStep;
			int count = 3000;
			int lower = (distance * 3 / 2) + startOffset, upper = lower + count * distance + (distance / 3);
			System.Int32 tempAux = (System.Int32) lower;
			System.Int32 tempAux2 = (System.Int32) upper;
			NumericRangeQuery q = NumericRangeQuery.NewIntRange(field, precisionStep, tempAux, tempAux2, true, true);
			System.Int32 tempAux3 = (System.Int32) lower;
			System.Int32 tempAux4 = (System.Int32) upper;
			NumericRangeFilter f = NumericRangeFilter.NewIntRange(field, precisionStep, tempAux3, tempAux4, true, true);
			int lastTerms = 0;
			for (sbyte i = 0; i < 3; i++)
			{
				TopDocs topDocs;
				int terms;
				System.String type;
				q.ClearTotalNumberOfTerms();
				f.ClearTotalNumberOfTerms();
				switch (i)
				{
					
					case 0: 
						type = " (constant score filter rewrite)";
						q.SetRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
						topDocs = searcher.Search(q, null, noDocs, Sort.INDEXORDER);
						terms = q.GetTotalNumberOfTerms();
						break;
					
					case 1: 
						type = " (constant score boolean rewrite)";
						q.SetRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
						topDocs = searcher.Search(q, null, noDocs, Sort.INDEXORDER);
						terms = q.GetTotalNumberOfTerms();
						break;
					
					case 2: 
						type = " (filter)";
						topDocs = searcher.Search(new MatchAllDocsQuery(), f, noDocs, Sort.INDEXORDER);
						terms = f.GetTotalNumberOfTerms();
						break;
					
					default: 
						return ;
					
				}
				System.Console.Out.WriteLine("Found " + terms + " distinct terms in range for field '" + field + "'" + type + ".");
				ScoreDoc[] sd = topDocs.scoreDocs;
				Assert.IsNotNull(sd);
				Assert.AreEqual(count, sd.Length, "Score doc count" + type);
				Document doc = searcher.Doc(sd[0].doc);
				Assert.AreEqual(2 * distance + startOffset, System.Int32.Parse(doc.Get(field)), "First doc" + type);
				doc = searcher.Doc(sd[sd.Length - 1].doc);
				Assert.AreEqual((1 + count) * distance + startOffset, System.Int32.Parse(doc.Get(field)), "Last doc" + type);
				if (i > 0)
				{
					Assert.AreEqual(lastTerms, terms, "Distinct term number is equal for all query types");
				}
				lastTerms = terms;
			}
		}
		
        [Test]
		public virtual void  TestRange_8bit()
		{
			TestRange(8);
		}
		
        [Test]
		public virtual void  TestRange_4bit()
		{
			TestRange(4);
		}
		
        [Test]
		public virtual void  TestRange_2bit()
		{
			TestRange(2);
		}
		
        [Test]
		public virtual void  TestInverseRange()
		{
			System.Int32 tempAux = 1000;
			System.Int32 tempAux2 = - 1000;
			NumericRangeFilter f = NumericRangeFilter.NewIntRange("field8", 8, tempAux, tempAux2, true, true);
			Assert.AreSame(DocIdSet.EMPTY_DOCIDSET, f.GetDocIdSet(searcher.GetIndexReader()), "A inverse range should return the EMPTY_DOCIDSET instance");
			//UPGRADE_TODO: The 'System.Int32' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int32 tempAux3 = (System.Int32) System.Int32.MaxValue;
			f = NumericRangeFilter.NewIntRange("field8", 8, tempAux3, null, false, false);
			Assert.AreSame(DocIdSet.EMPTY_DOCIDSET, f.GetDocIdSet(searcher.GetIndexReader()), "A exclusive range starting with Integer.MAX_VALUE should return the EMPTY_DOCIDSET instance");
			//UPGRADE_TODO: The 'System.Int32' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int32 tempAux4 = (System.Int32) System.Int32.MinValue;
			f = NumericRangeFilter.NewIntRange("field8", 8, null, tempAux4, false, false);
			Assert.AreSame(DocIdSet.EMPTY_DOCIDSET, f.GetDocIdSet(searcher.GetIndexReader()), "A exclusive range ending with Integer.MIN_VALUE should return the EMPTY_DOCIDSET instance");
		}
		
        [Test]
		public virtual void  TestOneMatchQuery()
		{
			System.Int32 tempAux = 1000;
			System.Int32 tempAux2 = 1000;
			NumericRangeQuery q = NumericRangeQuery.NewIntRange("ascfield8", 8, tempAux, tempAux2, true, true);
			Assert.AreSame(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE, q.GetRewriteMethod());
			TopDocs topDocs = searcher.Search(q, noDocs);
			ScoreDoc[] sd = topDocs.scoreDocs;
			Assert.IsNotNull(sd);
			Assert.AreEqual(1, sd.Length, "Score doc count");
		}
		
		private void  TestLeftOpenRange(int precisionStep)
		{
			System.String field = "field" + precisionStep;
			int count = 3000;
			int upper = (count - 1) * distance + (distance / 3) + startOffset;
			//UPGRADE_TODO: The 'System.Int32' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int32 tempAux = (System.Int32) upper;
			NumericRangeQuery q = NumericRangeQuery.NewIntRange(field, precisionStep, null, tempAux, true, true);
			TopDocs topDocs = searcher.Search(q, null, noDocs, Sort.INDEXORDER);
			System.Console.Out.WriteLine("Found " + q.GetTotalNumberOfTerms() + " distinct terms in left open range for field '" + field + "'.");
			ScoreDoc[] sd = topDocs.scoreDocs;
			Assert.IsNotNull(sd);
			Assert.AreEqual(count, sd.Length, "Score doc count");
			Document doc = searcher.Doc(sd[0].doc);
			Assert.AreEqual(startOffset, System.Int32.Parse(doc.Get(field)), "First doc");
			doc = searcher.Doc(sd[sd.Length - 1].doc);
			Assert.AreEqual((count - 1) * distance + startOffset, System.Int32.Parse(doc.Get(field)), "Last doc");
		}
		
        [Test]
		public virtual void  TestLeftOpenRange_8bit()
		{
			TestLeftOpenRange(8);
		}
		
        [Test]
		public virtual void  TestLeftOpenRange_4bit()
		{
			TestLeftOpenRange(4);
		}
		
        [Test]
		public virtual void  TestLeftOpenRange_2bit()
		{
			TestLeftOpenRange(2);
		}
		
		private void  TestRightOpenRange(int precisionStep)
		{
			System.String field = "field" + precisionStep;
			int count = 3000;
			int lower = (count - 1) * distance + (distance / 3) + startOffset;
			//UPGRADE_TODO: The 'System.Int32' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int32 tempAux = (System.Int32) lower;
			NumericRangeQuery q = NumericRangeQuery.NewIntRange(field, precisionStep, tempAux, null, true, true);
			TopDocs topDocs = searcher.Search(q, null, noDocs, Sort.INDEXORDER);
			System.Console.Out.WriteLine("Found " + q.GetTotalNumberOfTerms() + " distinct terms in right open range for field '" + field + "'.");
			ScoreDoc[] sd = topDocs.scoreDocs;
			Assert.IsNotNull(sd);
			Assert.AreEqual(noDocs - count, sd.Length, "Score doc count");
			Document doc = searcher.Doc(sd[0].doc);
			Assert.AreEqual(count * distance + startOffset, System.Int32.Parse(doc.Get(field)), "First doc");
			doc = searcher.Doc(sd[sd.Length - 1].doc);
			Assert.AreEqual((noDocs - 1) * distance + startOffset, System.Int32.Parse(doc.Get(field)), "Last doc");
		}
		
        [Test]
		public virtual void  TestRightOpenRange_8bit()
		{
			TestRightOpenRange(8);
		}
		
        [Test]
		public virtual void  TestRightOpenRange_4bit()
		{
			TestRightOpenRange(4);
		}
		
        [Test]
		public virtual void  TestRightOpenRange_2bit()
		{
			TestRightOpenRange(2);
		}
		
		private void  TestRandomTrieAndClassicRangeQuery(int precisionStep)
		{
			System.Random rnd = NewRandom();
			System.String field = "field" + precisionStep;
			int termCountT = 0, termCountC = 0;
			for (int i = 0; i < 50; i++)
			{
				int lower = (int) (rnd.NextDouble() * noDocs * distance) + startOffset;
				int upper = (int) (rnd.NextDouble() * noDocs * distance) + startOffset;
				if (lower > upper)
				{
					int a = lower; lower = upper; upper = a;
				}
				// test inclusive range
				System.Int32 tempAux = (System.Int32) lower;
				System.Int32 tempAux2 = (System.Int32) upper;
				NumericRangeQuery tq = NumericRangeQuery.NewIntRange(field, precisionStep, tempAux, tempAux2, true, true);
				TermRangeQuery cq = new TermRangeQuery(field, NumericUtils.IntToPrefixCoded(lower), NumericUtils.IntToPrefixCoded(upper), true, true);
				TopDocs tTopDocs = searcher.Search(tq, 1);
				TopDocs cTopDocs = searcher.Search(cq, 1);
				Assert.AreEqual(cTopDocs.totalHits, tTopDocs.totalHits, "Returned count for NumericRangeQuery and TermRangeQuery must be equal");
				termCountT += tq.GetTotalNumberOfTerms();
				termCountC += cq.GetTotalNumberOfTerms();
				// test exclusive range
				System.Int32 tempAux3 = (System.Int32) lower;
				System.Int32 tempAux4 = (System.Int32) upper;
				tq = NumericRangeQuery.NewIntRange(field, precisionStep, tempAux3, tempAux4, false, false);
				cq = new TermRangeQuery(field, NumericUtils.IntToPrefixCoded(lower), NumericUtils.IntToPrefixCoded(upper), false, false);
				tTopDocs = searcher.Search(tq, 1);
				cTopDocs = searcher.Search(cq, 1);
				Assert.AreEqual(cTopDocs.totalHits, tTopDocs.totalHits, "Returned count for NumericRangeQuery and TermRangeQuery must be equal");
				termCountT += tq.GetTotalNumberOfTerms();
				termCountC += cq.GetTotalNumberOfTerms();
				// test left exclusive range
				System.Int32 tempAux5 = (System.Int32) lower;
				System.Int32 tempAux6 = (System.Int32) upper;
				tq = NumericRangeQuery.NewIntRange(field, precisionStep, tempAux5, tempAux6, false, true);
				cq = new TermRangeQuery(field, NumericUtils.IntToPrefixCoded(lower), NumericUtils.IntToPrefixCoded(upper), false, true);
				tTopDocs = searcher.Search(tq, 1);
				cTopDocs = searcher.Search(cq, 1);
				Assert.AreEqual(cTopDocs.totalHits, tTopDocs.totalHits, "Returned count for NumericRangeQuery and TermRangeQuery must be equal");
				termCountT += tq.GetTotalNumberOfTerms();
				termCountC += cq.GetTotalNumberOfTerms();
				// test right exclusive range
				System.Int32 tempAux7 = (System.Int32) lower;
				System.Int32 tempAux8 = (System.Int32) upper;
				tq = NumericRangeQuery.NewIntRange(field, precisionStep, tempAux7, tempAux8, true, false);
				cq = new TermRangeQuery(field, NumericUtils.IntToPrefixCoded(lower), NumericUtils.IntToPrefixCoded(upper), true, false);
				tTopDocs = searcher.Search(tq, 1);
				cTopDocs = searcher.Search(cq, 1);
				Assert.AreEqual(cTopDocs.totalHits, tTopDocs.totalHits, "Returned count for NumericRangeQuery and TermRangeQuery must be equal");
				termCountT += tq.GetTotalNumberOfTerms();
				termCountC += cq.GetTotalNumberOfTerms();
			}
			if (precisionStep == System.Int32.MaxValue)
			{
				Assert.AreEqual(termCountT, termCountC, "Total number of terms should be equal for unlimited precStep");
			}
			else
			{
				System.Console.Out.WriteLine("Average number of terms during random search on '" + field + "':");
				System.Console.Out.WriteLine(" Trie query: " + (((double) termCountT) / (50 * 4)));
				System.Console.Out.WriteLine(" Classical query: " + (((double) termCountC) / (50 * 4)));
			}
		}
		
        [Test]
		public virtual void  TestRandomTrieAndClassicRangeQuery_8bit()
		{
			TestRandomTrieAndClassicRangeQuery(8);
		}
		
        [Test]
		public virtual void  TestRandomTrieAndClassicRangeQuery_4bit()
		{
			TestRandomTrieAndClassicRangeQuery(4);
		}
		
        [Test]
		public virtual void  TestRandomTrieAndClassicRangeQuery_2bit()
		{
			TestRandomTrieAndClassicRangeQuery(2);
		}
		
        [Test]
		public virtual void  TestRandomTrieAndClassicRangeQuery_NoTrie()
		{
			TestRandomTrieAndClassicRangeQuery(System.Int32.MaxValue);
		}
		
		private void  TestRangeSplit(int precisionStep)
		{
			System.Random rnd = NewRandom();
			System.String field = "ascfield" + precisionStep;
			// 50 random tests
			for (int i = 0; i < 50; i++)
			{
				int lower = (int) (rnd.NextDouble() * noDocs - noDocs / 2);
				int upper = (int) (rnd.NextDouble() * noDocs - noDocs / 2);
				if (lower > upper)
				{
					int a = lower; lower = upper; upper = a;
				}
				// test inclusive range
				System.Int32 tempAux = (System.Int32) lower;
				System.Int32 tempAux2 = (System.Int32) upper;
				Query tq = NumericRangeQuery.NewIntRange(field, precisionStep, tempAux, tempAux2, true, true);
				TopDocs tTopDocs = searcher.Search(tq, 1);
				Assert.AreEqual(upper - lower + 1, tTopDocs.totalHits, "Returned count of range query must be equal to inclusive range length");
				// test exclusive range
				System.Int32 tempAux3 = (System.Int32) lower;
				System.Int32 tempAux4 = (System.Int32) upper;
				tq = NumericRangeQuery.NewIntRange(field, precisionStep, tempAux3, tempAux4, false, false);
				tTopDocs = searcher.Search(tq, 1);
				Assert.AreEqual(System.Math.Max(upper - lower - 1, 0), tTopDocs.totalHits, "Returned count of range query must be equal to exclusive range length");
				// test left exclusive range
				System.Int32 tempAux5 = (System.Int32) lower;
				System.Int32 tempAux6 = (System.Int32) upper;
				tq = NumericRangeQuery.NewIntRange(field, precisionStep, tempAux5, tempAux6, false, true);
				tTopDocs = searcher.Search(tq, 1);
				Assert.AreEqual(upper - lower, tTopDocs.totalHits, "Returned count of range query must be equal to half exclusive range length");
				// test right exclusive range
				System.Int32 tempAux7 = (System.Int32) lower;
				System.Int32 tempAux8 = (System.Int32) upper;
				tq = NumericRangeQuery.NewIntRange(field, precisionStep, tempAux7, tempAux8, true, false);
				tTopDocs = searcher.Search(tq, 1);
				Assert.AreEqual(upper - lower, tTopDocs.totalHits, "Returned count of range query must be equal to half exclusive range length");
			}
		}
		
        [Test]
		public virtual void  TestRangeSplit_8bit()
		{
			TestRangeSplit(8);
		}
		
        [Test]
		public virtual void  TestRangeSplit_4bit()
		{
			TestRangeSplit(4);
		}
		
        [Test]
		public virtual void  TestRangeSplit_2bit()
		{
			TestRangeSplit(2);
		}
		
		/// <summary>we fake a float test using int2float conversion of NumericUtils </summary>
		private void  TestFloatRange(int precisionStep)
		{
			System.String field = "ascfield" + precisionStep;
			int lower = - 1000;
			int upper = + 2000;
			
			System.Single tempAux = (float) NumericUtils.SortableIntToFloat(lower);
			System.Single tempAux2 = (float) NumericUtils.SortableIntToFloat(upper);
			Query tq = NumericRangeQuery.NewFloatRange(field, precisionStep, tempAux, tempAux2, true, true);
			TopDocs tTopDocs = searcher.Search(tq, 1);
			Assert.AreEqual(upper - lower + 1, tTopDocs.totalHits, "Returned count of range query must be equal to inclusive range length");
			
			System.Single tempAux3 = (float) NumericUtils.SortableIntToFloat(lower);
			System.Single tempAux4 = (float) NumericUtils.SortableIntToFloat(upper);
			Filter tf = NumericRangeFilter.NewFloatRange(field, precisionStep, tempAux3, tempAux4, true, true);
			tTopDocs = searcher.Search(new MatchAllDocsQuery(), tf, 1);
			Assert.AreEqual(upper - lower + 1, tTopDocs.totalHits, "Returned count of range filter must be equal to inclusive range length");
		}
		
        [Test]
		public virtual void  TestFloatRange_8bit()
		{
			TestFloatRange(8);
		}
		
        [Test]
		public virtual void  TestFloatRange_4bit()
		{
			TestFloatRange(4);
		}
		
        [Test]
		public virtual void  TestFloatRange_2bit()
		{
			TestFloatRange(2);
		}
		
		private void  TestSorting(int precisionStep)
		{
			System.Random rnd = NewRandom();
			System.String field = "field" + precisionStep;
			// 10 random tests, the index order is ascending,
			// so using a reverse sort field should retun descending documents
			for (int i = 0; i < 10; i++)
			{
				int lower = (int) (rnd.NextDouble() * noDocs * distance) + startOffset;
				int upper = (int) (rnd.NextDouble() * noDocs * distance) + startOffset;
				if (lower > upper)
				{
					int a = lower; lower = upper; upper = a;
				}
				System.Int32 tempAux = (System.Int32) lower;
				System.Int32 tempAux2 = (System.Int32) upper;
				Query tq = NumericRangeQuery.NewIntRange(field, precisionStep, tempAux, tempAux2, true, true);
				TopDocs topDocs = searcher.Search(tq, null, noDocs, new Sort(new SortField(field, SortField.INT, true)));
				if (topDocs.totalHits == 0)
					continue;
				ScoreDoc[] sd = topDocs.scoreDocs;
				Assert.IsNotNull(sd);
				int last = System.Int32.Parse(searcher.Doc(sd[0].doc).Get(field));
				for (int j = 1; j < sd.Length; j++)
				{
					int act = System.Int32.Parse(searcher.Doc(sd[j].doc).Get(field));
					Assert.IsTrue(last > act, "Docs should be sorted backwards");
					last = act;
				}
			}
		}
		
        [Test]
		public virtual void  TestSorting_8bit()
		{
			TestSorting(8);
		}
		
        [Test]
		public virtual void  TestSorting_4bit()
		{
			TestSorting(4);
		}
		
        [Test]
		public virtual void  TestSorting_2bit()
		{
			TestSorting(2);
		}
		
        [Test]
		public virtual void  TestEqualsAndHash()
		{
			System.Int32 tempAux = 10;
			System.Int32 tempAux2 = 20;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewIntRange("test1", 4, tempAux, tempAux2, true, true));
			System.Int32 tempAux3 = 10;
			System.Int32 tempAux4 = 20;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewIntRange("test2", 4, tempAux3, tempAux4, false, true));
			System.Int32 tempAux5 = 10;
			System.Int32 tempAux6 = 20;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewIntRange("test3", 4, tempAux5, tempAux6, true, false));
			System.Int32 tempAux7 = 10;
			System.Int32 tempAux8 = 20;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewIntRange("test4", 4, tempAux7, tempAux8, false, false));
			//UPGRADE_TODO: The 'System.Int32' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int32 tempAux9 = 10;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewIntRange("test5", 4, tempAux9, null, true, true));
			//UPGRADE_TODO: The 'System.Int32' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int32 tempAux10 = 20;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewIntRange("test6", 4, null, tempAux10, true, true));
			//UPGRADE_TODO: The 'System.Int32' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewIntRange("test7", 4, null, null, true, true));
			System.Int32 tempAux11 = 10;
			System.Int32 tempAux12 = 20;
			System.Int32 tempAux13 = 10;
			System.Int32 tempAux14 = 20;
			QueryUtils.CheckEqual(NumericRangeQuery.NewIntRange("test8", 4, tempAux11, tempAux12, true, true), NumericRangeQuery.NewIntRange("test8", 4, tempAux13, tempAux14, true, true));
			System.Int32 tempAux15 = 10;
			System.Int32 tempAux16 = 20;
			System.Int32 tempAux17 = 10;
			System.Int32 tempAux18 = 20;
			QueryUtils.CheckUnequal(NumericRangeQuery.NewIntRange("test9", 4, tempAux15, tempAux16, true, true), NumericRangeQuery.NewIntRange("test9", 8, tempAux17, tempAux18, true, true));
			System.Int32 tempAux19 = 10;
			System.Int32 tempAux20 = 20;
			System.Int32 tempAux21 = 10;
			System.Int32 tempAux22 = 20;
			QueryUtils.CheckUnequal(NumericRangeQuery.NewIntRange("test10a", 4, tempAux19, tempAux20, true, true), NumericRangeQuery.NewIntRange("test10b", 4, tempAux21, tempAux22, true, true));
			System.Int32 tempAux23 = 10;
			System.Int32 tempAux24 = 20;
			System.Int32 tempAux25 = 20;
			System.Int32 tempAux26 = 10;
			QueryUtils.CheckUnequal(NumericRangeQuery.NewIntRange("test11", 4, tempAux23, tempAux24, true, true), NumericRangeQuery.NewIntRange("test11", 4, tempAux25, tempAux26, true, true));
			System.Int32 tempAux27 = 10;
			System.Int32 tempAux28 = 20;
			System.Int32 tempAux29 = 10;
			System.Int32 tempAux30 = 20;
			QueryUtils.CheckUnequal(NumericRangeQuery.NewIntRange("test12", 4, tempAux27, tempAux28, true, true), NumericRangeQuery.NewIntRange("test12", 4, tempAux29, tempAux30, false, true));
			System.Int32 tempAux31 = 10;
			System.Int32 tempAux32 = 20;
			System.Single tempAux33 = (float) 10f;
			System.Single tempAux34 = (float) 20f;
			QueryUtils.CheckUnequal(NumericRangeQuery.NewIntRange("test13", 4, tempAux31, tempAux32, true, true), NumericRangeQuery.NewFloatRange("test13", 4, tempAux33, tempAux34, true, true));
			// the following produces a hash collision, because Long and Integer have the same hashcode, so only test equality:
			System.Int32 tempAux35 = 10;
			System.Int32 tempAux36 = 20;
			Query q1 = NumericRangeQuery.NewIntRange("test14", 4, tempAux35, tempAux36, true, true);
			System.Int64 tempAux37 = 10L;
			System.Int64 tempAux38 = 20L;
			Query q2 = NumericRangeQuery.NewLongRange("test14", 4, tempAux37, tempAux38, true, true);
			Assert.IsFalse(q1.Equals(q2));
			Assert.IsFalse(q2.Equals(q1));
		}
		static TestNumericRangeQuery32()
		{
			{
				try
				{
					// set the theoretical maximum term count for 8bit (see docs for the number)
					BooleanQuery.SetMaxClauseCount(3 * 255 * 2 + 255);
					
					directory = new RAMDirectory();
					IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, MaxFieldLength.UNLIMITED);
					
					NumericField field8 = new NumericField("field8", 8, Field.Store.YES, true), field4 = new NumericField("field4", 4, Field.Store.YES, true), field2 = new NumericField("field2", 2, Field.Store.YES, true), fieldNoTrie = new NumericField("field" + System.Int32.MaxValue, System.Int32.MaxValue, Field.Store.YES, true), ascfield8 = new NumericField("ascfield8", 8, Field.Store.NO, true), ascfield4 = new NumericField("ascfield4", 4, Field.Store.NO, true), ascfield2 = new NumericField("ascfield2", 2, Field.Store.NO, true);
					
					Document doc = new Document();
					// add fields, that have a distance to test general functionality
					doc.Add(field8); doc.Add(field4); doc.Add(field2); doc.Add(fieldNoTrie);
					// add ascending fields with a distance of 1, beginning at -noDocs/2 to test the correct splitting of range and inclusive/exclusive
					doc.Add(ascfield8); doc.Add(ascfield4); doc.Add(ascfield2);
					
					// Add a series of noDocs docs with increasing int values
					for (int l = 0; l < noDocs; l++)
					{
						int val = distance * l + startOffset;
						field8.SetIntValue(val);
						field4.SetIntValue(val);
						field2.SetIntValue(val);
						fieldNoTrie.SetIntValue(val);
						
						val = l - (noDocs / 2);
						ascfield8.SetIntValue(val);
						ascfield4.SetIntValue(val);
						ascfield2.SetIntValue(val);
						writer.AddDocument(doc);
					}
					
					writer.Optimize();
					writer.Close();
					searcher = new IndexSearcher(directory, true);
				}
				catch (System.Exception e)
				{
					throw new System.SystemException("", e);
				}
			}
		}
	}
}