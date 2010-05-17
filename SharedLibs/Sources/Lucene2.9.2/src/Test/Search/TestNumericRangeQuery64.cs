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
	public class TestNumericRangeQuery64:LuceneTestCase
	{
		// distance of entries
		private const long distance = 66666L;
		// shift the starting of the values to the left, to also have negative values:
		private const long startOffset = - 1L << 31;
		// number of docs to generate for testing
		private const int noDocs = 10000;
		
		private static RAMDirectory directory;
		private static IndexSearcher searcher;
		
		/// <summary>test for constant score + boolean query + filter, the other tests only use the constant score mode </summary>
		private void  TestRange(int precisionStep)
		{
			System.String field = "field" + precisionStep;
			int count = 3000;
			long lower = (distance * 3 / 2) + startOffset, upper = lower + count * distance + (distance / 3);
			System.Int64 tempAux = (long) lower;
			System.Int64 tempAux2 = (long) upper;
			NumericRangeQuery q = NumericRangeQuery.NewLongRange(field, precisionStep, tempAux, tempAux2, true, true);
			System.Int64 tempAux3 = (long) lower;
			System.Int64 tempAux4 = (long) upper;
			NumericRangeFilter f = NumericRangeFilter.NewLongRange(field, precisionStep, tempAux3, tempAux4, true, true);
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
				Assert.AreEqual(2 * distance + startOffset, System.Int64.Parse(doc.Get(field)), "First doc" + type);
				doc = searcher.Doc(sd[sd.Length - 1].doc);
				Assert.AreEqual((1 + count) * distance + startOffset, System.Int64.Parse(doc.Get(field)), "Last doc" + type);
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
		public virtual void  TestRange_6bit()
		{
			TestRange(6);
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
			System.Int64 tempAux = 1000L;
			System.Int64 tempAux2 = - 1000L;
			NumericRangeFilter f = NumericRangeFilter.NewLongRange("field8", 8, tempAux, tempAux2, true, true);
			Assert.AreSame(DocIdSet.EMPTY_DOCIDSET, f.GetDocIdSet(searcher.GetIndexReader()), "A inverse range should return the EMPTY_DOCIDSET instance");
			//UPGRADE_TODO: The 'System.Int64' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int64 tempAux3 = (long) System.Int64.MaxValue;
			f = NumericRangeFilter.NewLongRange("field8", 8, tempAux3, null, false, false);
			Assert.AreSame(DocIdSet.EMPTY_DOCIDSET, f.GetDocIdSet(searcher.GetIndexReader()), "A exclusive range starting with Long.MAX_VALUE should return the EMPTY_DOCIDSET instance");
			//UPGRADE_TODO: The 'System.Int64' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int64 tempAux4 = (long) System.Int64.MinValue;
			f = NumericRangeFilter.NewLongRange("field8", 8, null, tempAux4, false, false);
			Assert.AreSame(DocIdSet.EMPTY_DOCIDSET, f.GetDocIdSet(searcher.GetIndexReader()), "A exclusive range ending with Long.MIN_VALUE should return the EMPTY_DOCIDSET instance");
		}
		
        [Test]
		public virtual void  TestOneMatchQuery()
		{
			System.Int64 tempAux = 1000L;
			//UPGRADE_NOTE: ref keyword was added to struct-type parameters. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1303'"
			System.Int64 tempAux2 = 1000L;
			NumericRangeQuery q = NumericRangeQuery.NewLongRange("ascfield8", 8, tempAux, tempAux2, true, true);
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
			long upper = (count - 1) * distance + (distance / 3) + startOffset;
			//UPGRADE_TODO: The 'System.Int64' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int64 tempAux = (long) upper;
			NumericRangeQuery q = NumericRangeQuery.NewLongRange(field, precisionStep, null, tempAux, true, true);
			TopDocs topDocs = searcher.Search(q, null, noDocs, Sort.INDEXORDER);
			System.Console.Out.WriteLine("Found " + q.GetTotalNumberOfTerms() + " distinct terms in left open range for field '" + field + "'.");
			ScoreDoc[] sd = topDocs.scoreDocs;
			Assert.IsNotNull(sd);
			Assert.AreEqual(count, sd.Length, "Score doc count");
			Document doc = searcher.Doc(sd[0].doc);
			Assert.AreEqual(startOffset, System.Int64.Parse(doc.Get(field)), "First doc");
			doc = searcher.Doc(sd[sd.Length - 1].doc);
			Assert.AreEqual((count - 1) * distance + startOffset, System.Int64.Parse(doc.Get(field)), "Last doc");
		}
		
        [Test]
		public virtual void  TestLeftOpenRange_8bit()
		{
			TestLeftOpenRange(8);
		}
		
        [Test]
		public virtual void  TestLeftOpenRange_6bit()
		{
			TestLeftOpenRange(6);
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
			long lower = (count - 1) * distance + (distance / 3) + startOffset;
			//UPGRADE_TODO: The 'System.Int64' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int64 tempAux = (long) lower;
			NumericRangeQuery q = NumericRangeQuery.NewLongRange(field, precisionStep, tempAux, null, true, true);
			TopDocs topDocs = searcher.Search(q, null, noDocs, Sort.INDEXORDER);
			System.Console.Out.WriteLine("Found " + q.GetTotalNumberOfTerms() + " distinct terms in right open range for field '" + field + "'.");
			ScoreDoc[] sd = topDocs.scoreDocs;
			Assert.IsNotNull(sd);
			Assert.AreEqual(noDocs - count, sd.Length, "Score doc count");
			Document doc = searcher.Doc(sd[0].doc);
			Assert.AreEqual(count * distance + startOffset, System.Int64.Parse(doc.Get(field)), "First doc");
			doc = searcher.Doc(sd[sd.Length - 1].doc);
			Assert.AreEqual((noDocs - 1) * distance + startOffset, System.Int64.Parse(doc.Get(field)), "Last doc");
		}
		
        [Test]
		public virtual void  TestRightOpenRange_8bit()
		{
			TestRightOpenRange(8);
		}
		
        [Test]
		public virtual void  TestRightOpenRange_6bit()
		{
			TestRightOpenRange(6);
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
				long lower = (long) (rnd.NextDouble() * noDocs * distance) + startOffset;
				long upper = (long) (rnd.NextDouble() * noDocs * distance) + startOffset;
				if (lower > upper)
				{
					long a = lower; lower = upper; upper = a;
				}
				// test inclusive range
				System.Int64 tempAux = (long) lower;
				System.Int64 tempAux2 = (long) upper;
				NumericRangeQuery tq = NumericRangeQuery.NewLongRange(field, precisionStep, tempAux, tempAux2, true, true);
				TermRangeQuery cq = new TermRangeQuery(field, NumericUtils.LongToPrefixCoded(lower), NumericUtils.LongToPrefixCoded(upper), true, true);
				TopDocs tTopDocs = searcher.Search(tq, 1);
				TopDocs cTopDocs = searcher.Search(cq, 1);
				Assert.AreEqual(cTopDocs.totalHits, tTopDocs.totalHits, "Returned count for NumericRangeQuery and TermRangeQuery must be equal");
				termCountT += tq.GetTotalNumberOfTerms();
				termCountC += cq.GetTotalNumberOfTerms();
				// test exclusive range
				System.Int64 tempAux3 = (long) lower;
				System.Int64 tempAux4 = (long) upper;
				tq = NumericRangeQuery.NewLongRange(field, precisionStep, tempAux3, tempAux4, false, false);
				cq = new TermRangeQuery(field, NumericUtils.LongToPrefixCoded(lower), NumericUtils.LongToPrefixCoded(upper), false, false);
				tTopDocs = searcher.Search(tq, 1);
				cTopDocs = searcher.Search(cq, 1);
				Assert.AreEqual(cTopDocs.totalHits, tTopDocs.totalHits, "Returned count for NumericRangeQuery and TermRangeQuery must be equal");
				termCountT += tq.GetTotalNumberOfTerms();
				termCountC += cq.GetTotalNumberOfTerms();
				// test left exclusive range
				System.Int64 tempAux5 = (long) lower;
				System.Int64 tempAux6 = (long) upper;
				tq = NumericRangeQuery.NewLongRange(field, precisionStep, tempAux5, tempAux6, false, true);
				cq = new TermRangeQuery(field, NumericUtils.LongToPrefixCoded(lower), NumericUtils.LongToPrefixCoded(upper), false, true);
				tTopDocs = searcher.Search(tq, 1);
				cTopDocs = searcher.Search(cq, 1);
				Assert.AreEqual(cTopDocs.totalHits, tTopDocs.totalHits, "Returned count for NumericRangeQuery and TermRangeQuery must be equal");
				termCountT += tq.GetTotalNumberOfTerms();
				termCountC += cq.GetTotalNumberOfTerms();
				// test right exclusive range
				System.Int64 tempAux7 = (long) lower;
				System.Int64 tempAux8 = (long) upper;
				tq = NumericRangeQuery.NewLongRange(field, precisionStep, tempAux7, tempAux8, true, false);
				cq = new TermRangeQuery(field, NumericUtils.LongToPrefixCoded(lower), NumericUtils.LongToPrefixCoded(upper), true, false);
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
		public virtual void  TestRandomTrieAndClassicRangeQuery_6bit()
		{
			TestRandomTrieAndClassicRangeQuery(6);
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
				long lower = (long) (rnd.NextDouble() * noDocs - noDocs / 2);
				long upper = (long) (rnd.NextDouble() * noDocs - noDocs / 2);
				if (lower > upper)
				{
					long a = lower; lower = upper; upper = a;
				}
				// test inclusive range
				System.Int64 tempAux = (long) lower;
				System.Int64 tempAux2 = (long) upper;
				Query tq = NumericRangeQuery.NewLongRange(field, precisionStep, tempAux, tempAux2, true, true);
				TopDocs tTopDocs = searcher.Search(tq, 1);
				Assert.AreEqual(upper - lower + 1, tTopDocs.totalHits, "Returned count of range query must be equal to inclusive range length");
				// test exclusive range
				System.Int64 tempAux3 = (long) lower;
				System.Int64 tempAux4 = (long) upper;
				tq = NumericRangeQuery.NewLongRange(field, precisionStep, tempAux3, tempAux4, false, false);
				tTopDocs = searcher.Search(tq, 1);
				Assert.AreEqual(System.Math.Max(upper - lower - 1, 0), tTopDocs.totalHits, "Returned count of range query must be equal to exclusive range length");
				// test left exclusive range
				System.Int64 tempAux5 = (long) lower;
				System.Int64 tempAux6 = (long) upper;
				tq = NumericRangeQuery.NewLongRange(field, precisionStep, tempAux5, tempAux6, false, true);
				tTopDocs = searcher.Search(tq, 1);
				Assert.AreEqual(upper - lower, tTopDocs.totalHits, "Returned count of range query must be equal to half exclusive range length");
				// test right exclusive range
				System.Int64 tempAux7 = (long) lower;
				System.Int64 tempAux8 = (long) upper;
				tq = NumericRangeQuery.NewLongRange(field, precisionStep, tempAux7, tempAux8, true, false);
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
		public virtual void  TestRangeSplit_6bit()
		{
			TestRangeSplit(6);
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
		
		/// <summary>we fake a double test using long2double conversion of NumericUtils </summary>
		private void  TestDoubleRange(int precisionStep)
		{
			System.String field = "ascfield" + precisionStep;
			long lower = - 1000L;
			long upper = + 2000L;
			
			System.Double tempAux = (double) NumericUtils.SortableLongToDouble(lower);
			System.Double tempAux2 = (double) NumericUtils.SortableLongToDouble(upper);
			Query tq = NumericRangeQuery.NewDoubleRange(field, precisionStep, tempAux, tempAux2, true, true);
			TopDocs tTopDocs = searcher.Search(tq, 1);
			Assert.AreEqual(upper - lower + 1, tTopDocs.totalHits, "Returned count of range query must be equal to inclusive range length");
			
			System.Double tempAux3 = (double) NumericUtils.SortableLongToDouble(lower);
			System.Double tempAux4 = (double) NumericUtils.SortableLongToDouble(upper);
			Filter tf = NumericRangeFilter.NewDoubleRange(field, precisionStep, tempAux3, tempAux4, true, true);
			tTopDocs = searcher.Search(new MatchAllDocsQuery(), tf, 1);
			Assert.AreEqual(upper - lower + 1, tTopDocs.totalHits, "Returned count of range filter must be equal to inclusive range length");
		}
		
        [Test]
		public virtual void  TestDoubleRange_8bit()
		{
			TestDoubleRange(8);
		}
		
        [Test]
		public virtual void  TestDoubleRange_6bit()
		{
			TestDoubleRange(6);
		}
		
        [Test]
		public virtual void  TestDoubleRange_4bit()
		{
			TestDoubleRange(4);
		}
		
        [Test]
		public virtual void  TestDoubleRange_2bit()
		{
			TestDoubleRange(2);
		}
		
		private void  TestSorting(int precisionStep)
		{
			System.Random rnd = NewRandom();
			System.String field = "field" + precisionStep;
			// 10 random tests, the index order is ascending,
			// so using a reverse sort field should retun descending documents
			for (int i = 0; i < 10; i++)
			{
				long lower = (long) (rnd.NextDouble() * noDocs * distance) + startOffset;
				long upper = (long) (rnd.NextDouble() * noDocs * distance) + startOffset;
				if (lower > upper)
				{
					long a = lower; lower = upper; upper = a;
				}
				System.Int64 tempAux = (long) lower;
				System.Int64 tempAux2 = (long) upper;
				Query tq = NumericRangeQuery.NewLongRange(field, precisionStep, tempAux, tempAux2, true, true);
				TopDocs topDocs = searcher.Search(tq, null, noDocs, new Sort(new SortField(field, SortField.LONG, true)));
				if (topDocs.totalHits == 0)
					continue;
				ScoreDoc[] sd = topDocs.scoreDocs;
				Assert.IsNotNull(sd);
				long last = System.Int64.Parse(searcher.Doc(sd[0].doc).Get(field));
				for (int j = 1; j < sd.Length; j++)
				{
					long act = System.Int64.Parse(searcher.Doc(sd[j].doc).Get(field));
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
		public virtual void  TestSorting_6bit()
		{
			TestSorting(6);
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
			System.Int64 tempAux = 10L;
			System.Int64 tempAux2 = 20L;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewLongRange("test1", 4, tempAux, tempAux2, true, true));
			System.Int64 tempAux3 = 10L;
			System.Int64 tempAux4 = 20L;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewLongRange("test2", 4, tempAux3, tempAux4, false, true));
			System.Int64 tempAux5 = 10L;
			System.Int64 tempAux6 = 20L;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewLongRange("test3", 4, tempAux5, tempAux6, true, false));
			System.Int64 tempAux7 = 10L;
			System.Int64 tempAux8 = 20L;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewLongRange("test4", 4, tempAux7, tempAux8, false, false));
			//UPGRADE_TODO: The 'System.Int64' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int64 tempAux9 = 10L;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewLongRange("test5", 4, tempAux9, null, true, true));
			//UPGRADE_TODO: The 'System.Int64' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			System.Int64 tempAux10 = 20L;
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewLongRange("test6", 4, null, tempAux10, true, true));
			//UPGRADE_TODO: The 'System.Int64' structure does not have an equivalent to NULL. "ms-help://MS.VSCC.v80/dv_commoner/local/redirect.htm?index='!DefaultContextWindowIndex'&keyword='jlca1291'"
			QueryUtils.CheckHashEquals(NumericRangeQuery.NewLongRange("test7", 4, null, null, true, true));
			System.Int64 tempAux11 = 10L;
			System.Int64 tempAux12 = 20L;
			System.Int64 tempAux13 = 10L;
			System.Int64 tempAux14 = 20L;
			QueryUtils.CheckEqual(NumericRangeQuery.NewLongRange("test8", 4, tempAux11, tempAux12, true, true), NumericRangeQuery.NewLongRange("test8", 4, tempAux13, tempAux14, true, true));
			System.Int64 tempAux15 = 10L;
			System.Int64 tempAux16 = 20L;
			System.Int64 tempAux17 = 10L;
			System.Int64 tempAux18 = 20L;
			QueryUtils.CheckUnequal(NumericRangeQuery.NewLongRange("test9", 4, tempAux15, tempAux16, true, true), NumericRangeQuery.NewLongRange("test9", 8, tempAux17, tempAux18, true, true));
			System.Int64 tempAux19 = 10L;
			System.Int64 tempAux20 = 20L;
			System.Int64 tempAux21 = 10L;
			System.Int64 tempAux22 = 20L;
			QueryUtils.CheckUnequal(NumericRangeQuery.NewLongRange("test10a", 4, tempAux19, tempAux20, true, true), NumericRangeQuery.NewLongRange("test10b", 4, tempAux21, tempAux22, true, true));
			System.Int64 tempAux23 = 10L;
			System.Int64 tempAux24 = 20L;
			System.Int64 tempAux25 = 20L;
			System.Int64 tempAux26 = 10L;
			QueryUtils.CheckUnequal(NumericRangeQuery.NewLongRange("test11", 4, tempAux23, tempAux24, true, true), NumericRangeQuery.NewLongRange("test11", 4, tempAux25, tempAux26, true, true));
			System.Int64 tempAux27 = 10L;
			System.Int64 tempAux28 = 20L;
			System.Int64 tempAux29 = 10L;
			System.Int64 tempAux30 = 20L;
			QueryUtils.CheckUnequal(NumericRangeQuery.NewLongRange("test12", 4, tempAux27, tempAux28, true, true), NumericRangeQuery.NewLongRange("test12", 4, tempAux29, tempAux30, false, true));
			System.Int64 tempAux31 = 10L;
			System.Int64 tempAux32 = 20L;
			System.Single tempAux33 = (float) 10f;
			System.Single tempAux34 = (float) 20f;
			QueryUtils.CheckUnequal(NumericRangeQuery.NewLongRange("test13", 4, tempAux31, tempAux32, true, true), NumericRangeQuery.NewFloatRange("test13", 4, tempAux33, tempAux34, true, true));
			// difference to int range is tested in TestNumericRangeQuery32
		}
		static TestNumericRangeQuery64()
		{
			{
				try
				{
					// set the theoretical maximum term count for 8bit (see docs for the number)
					BooleanQuery.SetMaxClauseCount(7 * 255 * 2 + 255);
					
					directory = new RAMDirectory();
					IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, MaxFieldLength.UNLIMITED);
					
					NumericField field8 = new NumericField("field8", 8, Field.Store.YES, true), field6 = new NumericField("field6", 6, Field.Store.YES, true), field4 = new NumericField("field4", 4, Field.Store.YES, true), field2 = new NumericField("field2", 2, Field.Store.YES, true), fieldNoTrie = new NumericField("field" + System.Int32.MaxValue, System.Int32.MaxValue, Field.Store.YES, true), ascfield8 = new NumericField("ascfield8", 8, Field.Store.NO, true), ascfield6 = new NumericField("ascfield6", 6, Field.Store.NO, true), ascfield4 = new NumericField("ascfield4", 4, Field.Store.NO, true), ascfield2 = new NumericField("ascfield2", 2, Field.Store.NO, true);
					
					Document doc = new Document();
					// add fields, that have a distance to test general functionality
					doc.Add(field8); doc.Add(field6); doc.Add(field4); doc.Add(field2); doc.Add(fieldNoTrie);
					// add ascending fields with a distance of 1, beginning at -noDocs/2 to test the correct splitting of range and inclusive/exclusive
					doc.Add(ascfield8); doc.Add(ascfield6); doc.Add(ascfield4); doc.Add(ascfield2);
					
					// Add a series of noDocs docs with increasing long values, by updating the fields
					for (int l = 0; l < noDocs; l++)
					{
						long val = distance * l + startOffset;
						field8.SetLongValue(val);
						field6.SetLongValue(val);
						field4.SetLongValue(val);
						field2.SetLongValue(val);
						fieldNoTrie.SetLongValue(val);
						
						val = l - (noDocs / 2);
						ascfield8.SetLongValue(val);
						ascfield6.SetLongValue(val);
						ascfield4.SetLongValue(val);
						ascfield2.SetLongValue(val);
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