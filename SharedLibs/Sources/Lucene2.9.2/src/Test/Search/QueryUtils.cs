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
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using MultiReader = Lucene.Net.Index.MultiReader;
using MaxFieldLength = Lucene.Net.Index.IndexWriter.MaxFieldLength;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using ReaderUtil = Lucene.Net.Util.ReaderUtil;

namespace Lucene.Net.Search
{
	
	
	
	public class QueryUtils
	{
		[Serializable]
		private class AnonymousClassQuery:Query
		{
			public override System.String ToString(System.String field)
			{
				return "My Whacky Query";
			}
		}
		private class AnonymousClassCollector:Collector
		{
			public AnonymousClassCollector(int[] order, int[] opidx, int skip_op, Lucene.Net.Search.Scorer scorer, int[] sdoc, float maxDiff, Lucene.Net.Search.Query q, Lucene.Net.Search.IndexSearcher s)
			{
				InitBlock(order, opidx, skip_op, scorer, sdoc, maxDiff, q, s);
			}
			private void  InitBlock(int[] order, int[] opidx, int skip_op, Lucene.Net.Search.Scorer scorer, int[] sdoc, float maxDiff, Lucene.Net.Search.Query q, Lucene.Net.Search.IndexSearcher s)
			{
				this.order = order;
				this.opidx = opidx;
				this.skip_op = skip_op;
				this.scorer = scorer;
				this.sdoc = sdoc;
				this.maxDiff = maxDiff;
				this.q = q;
				this.s = s;
			}
			private int[] order;
			private int[] opidx;
			private int skip_op;
			private Lucene.Net.Search.Scorer scorer;
			private int[] sdoc;
			private float maxDiff;
			private Lucene.Net.Search.Query q;
			private Lucene.Net.Search.IndexSearcher s;
			private int base_Renamed = 0;
			private Scorer sc;
			
			public override void  SetScorer(Scorer scorer)
			{
				this.sc = scorer;
			}
			
			public override void  Collect(int doc)
			{
				doc = doc + base_Renamed;
				float score = sc.Score();
				try
				{
					int op = order[(opidx[0]++) % order.Length];
					// System.out.println(op==skip_op ?
					// "skip("+(sdoc[0]+1)+")":"next()");
					bool more = op == skip_op?scorer.Advance(sdoc[0] + 1) != DocIdSetIterator.NO_MORE_DOCS:scorer.NextDoc() != DocIdSetIterator.NO_MORE_DOCS;
					sdoc[0] = scorer.DocID();
					float scorerScore = scorer.Score();
					float scorerScore2 = scorer.Score();
					float scoreDiff = System.Math.Abs(score - scorerScore);
					float scorerDiff = System.Math.Abs(scorerScore2 - scorerScore);
					if (!more || doc != sdoc[0] || scoreDiff > maxDiff || scorerDiff > maxDiff)
					{
						System.Text.StringBuilder sbord = new System.Text.StringBuilder();
						for (int i = 0; i < order.Length; i++)
							sbord.Append(order[i] == skip_op?" skip()":" next()");
						throw new System.SystemException("ERROR matching docs:" + "\n\t" + (doc != sdoc[0]?"--> ":"") + "doc=" + sdoc[0] + "\n\t" + (!more?"--> ":"") + "tscorer.more=" + more + "\n\t" + (scoreDiff > maxDiff?"--> ":"") + "scorerScore=" + scorerScore + " scoreDiff=" + scoreDiff + " maxDiff=" + maxDiff + "\n\t" + (scorerDiff > maxDiff?"--> ":"") + "scorerScore2=" + scorerScore2 + " scorerDiff=" + scorerDiff + "\n\thitCollector.doc=" + doc + " score=" + score + "\n\t Scorer=" + scorer + "\n\t Query=" + q + "  " + q.GetType().FullName + "\n\t Searcher=" + s + "\n\t Order=" + sbord + "\n\t Op=" + (op == skip_op?" skip()":" next()"));
					}
				}
				catch (System.IO.IOException e)
				{
					throw new System.SystemException("", e);
				}
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
		private class AnonymousClassCollector1:Collector
		{
			public AnonymousClassCollector1(int[] lastDoc, Lucene.Net.Search.Query q, Lucene.Net.Search.IndexSearcher s, float maxDiff, IndexReader[] lastReader)
			{
				InitBlock(lastDoc, q, s, maxDiff, lastReader);
			}
			private void  InitBlock(int[] lastDoc, Lucene.Net.Search.Query q, Lucene.Net.Search.IndexSearcher s, float maxDiff, IndexReader[] lastReader)
			{
				this.lastDoc = lastDoc;
				this.q = q;
				this.s = s;
				this.maxDiff = maxDiff;
                this.lastReader = lastReader;
			}
			private int[] lastDoc;
			private Lucene.Net.Search.Query q;
			private Lucene.Net.Search.IndexSearcher s;
			private float maxDiff;
			private Scorer scorer;
			private IndexReader reader;
            private IndexReader[] lastReader;

			public override void  SetScorer(Scorer scorer)
			{
				this.scorer = scorer;
			}
			public override void  Collect(int doc)
			{
				//System.out.println("doc="+doc);
				float score = this.scorer.Score();
				try
				{
					
					for (int i = lastDoc[0] + 1; i <= doc; i++)
					{
						Weight w = q.Weight(s);
						Scorer scorer = w.Scorer(reader, true, false);
						Assert.IsTrue(scorer.Advance(i) != DocIdSetIterator.NO_MORE_DOCS, "query collected " + doc + " but skipTo(" + i + ") says no more docs!");
						Assert.AreEqual(doc, scorer.DocID(), "query collected " + doc + " but skipTo(" + i + ") got to " + scorer.DocID());
						float skipToScore = scorer.Score();
						Assert.AreEqual(skipToScore, scorer.Score(), maxDiff, "unstable skipTo(" + i + ") score!");
						Assert.AreEqual(score, skipToScore, maxDiff, "query assigned doc " + doc + " a score of <" + score + "> but skipTo(" + i + ") has <" + skipToScore + ">!");
					}
					lastDoc[0] = doc;
				}
				catch (System.IO.IOException e)
				{
					throw new System.SystemException("", e);
				}
			}
			public override void  SetNextReader(IndexReader reader, int docBase)
			{
		        // confirm that skipping beyond the last doc, on the
                // previous reader, hits NO_MORE_DOCS
                if (lastReader[0] != null) 
                {
                    IndexReader previousReader = lastReader[0];
                    Weight w = q.Weight(new IndexSearcher(previousReader));
                    Scorer scorer = w.Scorer(previousReader, true, false);
                    if (scorer != null)
                    {
                        bool more = scorer.Advance(lastDoc[0] + 1) != DocIdSetIterator.NO_MORE_DOCS;
                        Assert.IsFalse(more, "query's last doc was " + lastDoc[0] + " but skipTo(" + (lastDoc[0] + 1) + ") got to " + scorer.DocID());
                    }
                }

                this.reader = lastReader[0] = reader;
                lastDoc[0] = -1;
            }

			public override bool AcceptsDocsOutOfOrder()
			{
				return false;
			}
		}
		
		/// <summary>Check the types of things query objects should be able to do. </summary>
		public static void  Check(Query q)
		{
			CheckHashEquals(q);
		}
		
		/// <summary>check very basic hashCode and equals </summary>
		public static void  CheckHashEquals(Query q)
		{
			Query q2 = (Query) q.Clone();
			CheckEqual(q, q2);
			
			Query q3 = (Query) q.Clone();
			q3.SetBoost(7.21792348f);
			CheckUnequal(q, q3);
			
			// test that a class check is done so that no exception is thrown
			// in the implementation of equals()
			Query whacky = new AnonymousClassQuery();
			whacky.SetBoost(q.GetBoost());
			CheckUnequal(q, whacky);
		}
		
		public static void  CheckEqual(Query q1, Query q2)
		{
			Assert.AreEqual(q1, q2);
			Assert.AreEqual(q1.GetHashCode(), q2.GetHashCode());
		}
		
		public static void  CheckUnequal(Query q1, Query q2)
		{
			Assert.IsTrue(!q1.Equals(q2));
			Assert.IsTrue(!q2.Equals(q1));
			
			// possible this test can fail on a hash collision... if that
			// happens, please change test to use a different example.
			Assert.IsTrue(q1.GetHashCode() != q2.GetHashCode());
		}
		
		/// <summary>deep check that explanations of a query 'score' correctly </summary>
		public static void  CheckExplanations(Query q, Searcher s)
		{
			CheckHits.CheckExplanations(q, null, s, true);
		}
		
		/// <summary> Various query sanity checks on a searcher, some checks are only done for
		/// instanceof IndexSearcher.
		/// 
		/// </summary>
		/// <seealso cref="Check(Query)">
		/// </seealso>
		/// <seealso cref="checkFirstSkipTo">
		/// </seealso>
		/// <seealso cref="checkSkipTo">
		/// </seealso>
		/// <seealso cref="checkExplanations">
		/// </seealso>
		/// <seealso cref="checkSerialization">
		/// </seealso>
		/// <seealso cref="checkEqual">
		/// </seealso>
		public static void  Check(Query q1, Searcher s)
		{
			Check(q1, s, true);
		}
		private static void  Check(Query q1, Searcher s, bool wrap)
		{
			try
			{
				Check(q1);
				if (s != null)
				{
					if (s is IndexSearcher)
					{
						IndexSearcher is_Renamed = (IndexSearcher) s;
						CheckFirstSkipTo(q1, is_Renamed);
						CheckSkipTo(q1, is_Renamed);
						if (wrap)
						{
							Check(q1, WrapUnderlyingReader(is_Renamed, - 1), false);
							Check(q1, WrapUnderlyingReader(is_Renamed, 0), false);
							Check(q1, WrapUnderlyingReader(is_Renamed, + 1), false);
						}
					}
					if (wrap)
					{
						Check(q1, WrapSearcher(s, - 1), false);
						Check(q1, WrapSearcher(s, 0), false);
						Check(q1, WrapSearcher(s, + 1), false);
					}
					CheckExplanations(q1, s);
					CheckSerialization(q1, s);
					
					Query q2 = (Query) q1.Clone();
					CheckEqual(s.Rewrite(q1), s.Rewrite(q2));
				}
			}
			catch (System.IO.IOException e)
			{
				throw new System.SystemException("", e);
			}
		}
		
		/// <summary> Given an IndexSearcher, returns a new IndexSearcher whose IndexReader 
		/// is a MultiReader containing the Reader of the original IndexSearcher, 
		/// as well as several "empty" IndexReaders -- some of which will have 
		/// deleted documents in them.  This new IndexSearcher should 
		/// behave exactly the same as the original IndexSearcher.
		/// </summary>
		/// <param name="s">the searcher to wrap
		/// </param>
		/// <param name="edge">if negative, s will be the first sub; if 0, s will be in the middle, if positive s will be the last sub
		/// </param>
		public static IndexSearcher WrapUnderlyingReader(IndexSearcher s, int edge)
		{
			
			IndexReader r = s.GetIndexReader();
			
			// we can't put deleted docs before the nested reader, because
			// it will throw off the docIds
			IndexReader[] readers = new IndexReader[]{edge < 0?r:IndexReader.Open(MakeEmptyIndex(0)), IndexReader.Open(MakeEmptyIndex(0)), new MultiReader(new IndexReader[]{IndexReader.Open(MakeEmptyIndex(edge < 0?4:0)), IndexReader.Open(MakeEmptyIndex(0)), 0 == edge?r:IndexReader.Open(MakeEmptyIndex(0))}), IndexReader.Open(MakeEmptyIndex(0 < edge?0:7)), IndexReader.Open(MakeEmptyIndex(0)), new MultiReader(new IndexReader[]{IndexReader.Open(MakeEmptyIndex(0 < edge?0:5)), IndexReader.Open(MakeEmptyIndex(0)), 0 < edge?r:IndexReader.Open(MakeEmptyIndex(0))})};
			IndexSearcher out_Renamed = new IndexSearcher(new MultiReader(readers));
			out_Renamed.SetSimilarity(s.GetSimilarity());
			return out_Renamed;
		}
		/// <summary> Given a Searcher, returns a new MultiSearcher wrapping the  
		/// the original Searcher, 
		/// as well as several "empty" IndexSearchers -- some of which will have
		/// deleted documents in them.  This new MultiSearcher 
		/// should behave exactly the same as the original Searcher.
		/// </summary>
		/// <param name="s">the Searcher to wrap
		/// </param>
		/// <param name="edge">if negative, s will be the first sub; if 0, s will be in hte middle, if positive s will be the last sub
		/// </param>
		public static MultiSearcher WrapSearcher(Searcher s, int edge)
		{
			
			// we can't put deleted docs before the nested reader, because
			// it will through off the docIds
			Searcher[] searchers = new Searcher[]{edge < 0?s:new IndexSearcher(MakeEmptyIndex(0)), new MultiSearcher(new Searcher[]{new IndexSearcher(MakeEmptyIndex(edge < 0?65:0)), new IndexSearcher(MakeEmptyIndex(0)), 0 == edge?s:new IndexSearcher(MakeEmptyIndex(0))}), new IndexSearcher(MakeEmptyIndex(0 < edge?0:3)), new IndexSearcher(MakeEmptyIndex(0)), new MultiSearcher(new Searcher[]{new IndexSearcher(MakeEmptyIndex(0 < edge?0:5)), new IndexSearcher(MakeEmptyIndex(0)), 0 < edge?s:new IndexSearcher(MakeEmptyIndex(0))})};
			MultiSearcher out_Renamed = new MultiSearcher(searchers);
			out_Renamed.SetSimilarity(s.GetSimilarity());
			return out_Renamed;
		}
		
		private static RAMDirectory MakeEmptyIndex(int numDeletedDocs)
		{
			RAMDirectory d = new RAMDirectory();
			IndexWriter w = new IndexWriter(d, new WhitespaceAnalyzer(), true, MaxFieldLength.LIMITED);
			for (int i = 0; i < numDeletedDocs; i++)
			{
				w.AddDocument(new Document());
			}
			w.Commit();
			w.DeleteDocuments(new MatchAllDocsQuery());
			w.Commit();
			
			if (0 < numDeletedDocs)
				Assert.IsTrue(w.HasDeletions(), "writer has no deletions");
			
			Assert.AreEqual(numDeletedDocs, w.MaxDoc(), "writer is missing some deleted docs");
			Assert.AreEqual(0, w.NumDocs(), "writer has non-deleted docs");
			w.Close();
			IndexReader r = IndexReader.Open(d);
			Assert.AreEqual(numDeletedDocs, r.NumDeletedDocs(), "reader has wrong number of deleted docs");
			r.Close();
			return d;
		}
		
		
		/// <summary>check that the query weight is serializable. </summary>
		/// <throws>  IOException if serialization check fail.  </throws>
		private static void  CheckSerialization(Query q, Searcher s)
		{
			Weight w = q.Weight(s);
			try
			{
				System.IO.MemoryStream bos = new System.IO.MemoryStream();
				System.IO.BinaryWriter oos = new System.IO.BinaryWriter(bos);
		        System.Runtime.Serialization.Formatters.Binary.BinaryFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
		        formatter.Serialize(oos.BaseStream, w);
				oos.Close();
				System.IO.BinaryReader ois = new System.IO.BinaryReader(new System.IO.MemoryStream(bos.ToArray()));
		        formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
		        formatter.Deserialize(ois.BaseStream);
				ois.Close();
				
				//skip equals() test for now - most weights don't override equals() and we won't add this just for the tests.
                //TestCase.Assert.AreEqual(w2,w,"writeObject(w) != w.  ("+w+")");   
			}
			catch (System.Exception e)
			{
				System.IO.IOException e2 = new System.IO.IOException("Serialization failed for " + w, e);
				throw e2;
			}
		}
		
		
		/// <summary>alternate scorer skipTo(),skipTo(),next(),next(),skipTo(),skipTo(), etc
		/// and ensure a hitcollector receives same docs and scores
		/// </summary>
		public static void  CheckSkipTo(Query q, IndexSearcher s)
		{
			//System.out.println("Checking "+q);
			
			if (BooleanQuery.GetAllowDocsOutOfOrder())
				return ; // in this case order of skipTo() might differ from that of next().
			
			int skip_op = 0;
			int next_op = 1;
			int[][] orders = new int[][]{new int[]{next_op}, new int[]{skip_op}, new int[]{skip_op, next_op}, new int[]{next_op, skip_op}, new int[]{skip_op, skip_op, next_op, next_op}, new int[]{next_op, next_op, skip_op, skip_op}, new int[]{skip_op, skip_op, skip_op, next_op, next_op}};
			for (int k = 0; k < orders.Length; k++)
			{
				
				int[] order = orders[k];
				// System.out.print("Order:");for (int i = 0; i < order.length; i++)
				// System.out.print(order[i]==skip_op ? " skip()":" next()");
				// System.out.println();
				int[] opidx = new int[]{0};
				
				Weight w = q.Weight(s);
				Scorer scorer = w.Scorer(s.GetIndexReader(), true, false);
				if (scorer == null)
				{
					continue;
				}
				
				// FUTURE: ensure scorer.doc()==-1
				
				int[] sdoc = new int[]{- 1};
				float maxDiff = 1e-5f;
				s.Search(q, new AnonymousClassCollector(order, opidx, skip_op, scorer, sdoc, maxDiff, q, s));
				
				// make sure next call to scorer is false.
				int op = order[(opidx[0]++) % order.Length];
				// System.out.println(op==skip_op ? "last: skip()":"last: next()");
				bool more = (op == skip_op?scorer.Advance(sdoc[0] + 1):scorer.NextDoc()) != DocIdSetIterator.NO_MORE_DOCS;
				Assert.IsFalse(more);
			}
		}
		
		// check that first skip on just created scorers always goes to the right doc
		private static void  CheckFirstSkipTo(Query q, IndexSearcher s)
		{
			//System.out.println("checkFirstSkipTo: "+q);
            float maxDiff = 1e-4f; //{{Lucene.Net-2.9.1}}Intentional diversion from Java Lucene
			int[] lastDoc = new int[]{- 1};
            IndexReader[] lastReader = {null};

			s.Search(q, new AnonymousClassCollector1(lastDoc, q, s, maxDiff, lastReader));
			
			if(lastReader[0] != null)
            {
                // confirm that skipping beyond the last doc, on the
                // previous reader, hits NO_MORE_DOCS
                IndexReader previousReader = lastReader[0];
                Weight w = q.Weight(new IndexSearcher(previousReader));
                Scorer scorer = w.Scorer(previousReader, true, false);

				if (scorer != null)
				{
					bool more = scorer.Advance(lastDoc[0] + 1) != DocIdSetIterator.NO_MORE_DOCS;					
					Assert.IsFalse(more, "query's last doc was " + lastDoc[0] + " but skipTo(" + (lastDoc[0] + 1) + ") got to " + scorer.DocID());
				}
			}
		}
	}
}