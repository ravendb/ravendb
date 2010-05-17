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

using IndexReader = Lucene.Net.Index.IndexReader;
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Search
{
	
	public class CheckHits
	{
		
		/// <summary> Some explains methods calculate their values though a slightly
		/// different  order of operations from the actual scoring method ...
		/// this allows for a small amount of variation
        /// 
        /// mgarski - Java uses a value of 0.00005f, which causes tests in TestCustomScoreQuery
        /// to fail when comparing the scorer and explanation scores with the wrapped
        /// indexes and query boosts of greater than 2.0.  A comparison of the Java and .Net 
        /// explanations reveal a floating point precision issue, not a hidden defect.
		/// </summary>
		public static float EXPLAIN_SCORE_TOLERANCE_DELTA = 0.00025f;   // {{See: LUCENENET-288}} Intentional diversion from Java Lucene per above comment
		
		/// <summary> Tests that all documents up to maxDoc which are *not* in the
		/// expected result set, have an explanation which indicates no match
		/// (ie: Explanation value of 0.0f)
		/// </summary>
		public static void  CheckNoMatchExplanations(Query q, System.String defaultFieldName, Searcher searcher, int[] results)
		{
			
			System.String d = q.ToString(defaultFieldName);
			System.Collections.Hashtable ignore = new System.Collections.Hashtable();
			for (int i = 0; i < results.Length; i++)
			{
				SupportClass.CollectionsHelper.AddIfNotContains(ignore, (System.Int32) results[i]);
			}
			
			int maxDoc = searcher.MaxDoc();
			for (int doc = 0; doc < maxDoc; doc++)
			{
				if (ignore.Contains((System.Int32) doc))
					continue;
				
				Explanation exp = searcher.Explain(q, doc);
				Assert.IsNotNull(exp, "Explanation of [[" + d + "]] for #" + doc + " is null");
				Assert.AreEqual(0.0f, exp.GetValue(), 0.0f, "Explanation of [[" + d + "]] for #" + doc + " doesn't indicate non-match: " + exp.ToString());
			}
		}
		
		/// <summary> Tests that a query matches the an expected set of documents using a
		/// HitCollector.
		/// 
		/// <p/>
		/// Note that when using the HitCollector API, documents will be collected
		/// if they "match" regardless of what their score is.
		/// <p/>
		/// </summary>
		/// <param name="query">the query to test
		/// </param>
		/// <param name="searcher">the searcher to test the query against
		/// </param>
		/// <param name="defaultFieldName">used for displaying the query in assertion messages
		/// </param>
		/// <param name="results">a list of documentIds that must match the query
		/// </param>
		/// <seealso cref="Searcher.Search(Query,HitCollector)">
		/// </seealso>
		/// <seealso cref="checkHits">
		/// </seealso>
		public static void  CheckHitCollector(Query query, System.String defaultFieldName, Searcher searcher, int[] results)
		{
			
			QueryUtils.Check(query, searcher);
			
			System.Collections.Hashtable correct = new System.Collections.Hashtable();
			for (int i = 0; i < results.Length; i++)
			{
				SupportClass.CollectionsHelper.AddIfNotContains(correct, (System.Int32) results[i]);
			}
			System.Collections.Hashtable actual = new System.Collections.Hashtable();
			Collector c = new SetCollector(actual);
			
			searcher.Search(query, c);
			Assert.AreEqual(correct, actual, "Simple: " + query.ToString(defaultFieldName));
			
			for (int i = - 1; i < 2; i++)
			{
				actual.Clear();
				QueryUtils.WrapSearcher(searcher, i).Search(query, c);
				Assert.AreEqual(correct, actual, "Wrap Searcher " + i + ": " + query.ToString(defaultFieldName));
			}
			
			if (!(searcher is IndexSearcher))
				return ;
			
			for (int i = - 1; i < 2; i++)
			{
				actual.Clear();
				QueryUtils.WrapUnderlyingReader((IndexSearcher) searcher, i).Search(query, c);
				Assert.AreEqual(correct, actual, "Wrap Reader " + i + ": " + query.ToString(defaultFieldName));
			}
		}
		
		public class SetCollector:Collector
		{
			internal System.Collections.Hashtable bag;
			public SetCollector(System.Collections.Hashtable bag)
			{
				this.bag = bag;
			}
			private int base_Renamed = 0;
			public override void  SetScorer(Scorer scorer)
			{
			}
			public override void  Collect(int doc)
			{
				SupportClass.CollectionsHelper.AddIfNotContains(bag, (System.Int32)(doc + base_Renamed));
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
		
		/// <summary> Tests that a query matches the an expected set of documents using Hits.
		/// 
		/// <p/>
		/// Note that when using the Hits API, documents will only be returned
		/// if they have a positive normalized score.
		/// <p/>
		/// </summary>
		/// <param name="query">the query to test
		/// </param>
		/// <param name="searcher">the searcher to test the query against
		/// </param>
		/// <param name="defaultFieldName">used for displaing the query in assertion messages
		/// </param>
		/// <param name="results">a list of documentIds that must match the query
		/// </param>
		/// <seealso cref="Searcher.Search(Query)">
		/// </seealso>
		/// <seealso cref="checkHitCollector">
		/// </seealso>
		public static void  CheckHits_Renamed_Method(Query query, System.String defaultFieldName, Searcher searcher, int[] results)
		{
			if (searcher is IndexSearcher)
			{
				QueryUtils.Check(query, searcher);
			}
			
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			
			System.Collections.ArrayList correct = new System.Collections.ArrayList();
			for (int i = 0; i < results.Length; i++)
			{
                SupportClass.CollectionsHelper.AddIfNotContains(correct, results[i]);
			}
            correct.Sort();
			
			System.Collections.ArrayList actual = new System.Collections.ArrayList();
			for (int i = 0; i < hits.Length; i++)
			{
				SupportClass.CollectionsHelper.AddIfNotContains(actual, hits[i].doc);
			}
            actual.Sort();
			
			Assert.AreEqual(correct, actual, query.ToString(defaultFieldName));
			
			QueryUtils.Check(query, searcher);
		}
		
		/// <summary>Tests that a Hits has an expected order of documents </summary>
		public static void  CheckDocIds(System.String mes, int[] results, ScoreDoc[] hits)
		{
			Assert.AreEqual(hits.Length, results.Length, mes + " nr of hits");
			for (int i = 0; i < results.Length; i++)
			{
				Assert.AreEqual(results[i], hits[i].doc, mes + " doc nrs for hit " + i);
			}
		}
		
		/// <summary>Tests that two queries have an expected order of documents,
		/// and that the two queries have the same score values.
		/// </summary>
		public static void  CheckHitsQuery(Query query, ScoreDoc[] hits1, ScoreDoc[] hits2, int[] results)
		{
			
			CheckDocIds("hits1", results, hits1);
			CheckDocIds("hits2", results, hits2);
			CheckEqual(query, hits1, hits2);
		}
		
		public static void  CheckEqual(Query query, ScoreDoc[] hits1, ScoreDoc[] hits2)
		{
			float scoreTolerance = 1.0e-6f;
			if (hits1.Length != hits2.Length)
			{
				Assert.Fail("Unequal lengths: hits1=" + hits1.Length + ",hits2=" + hits2.Length);
			}
			for (int i = 0; i < hits1.Length; i++)
			{
				if (hits1[i].doc != hits2[i].doc)
				{
					Assert.Fail("Hit " + i + " docnumbers don't match\n" + Hits2str(hits1, hits2, 0, 0) + "for query:" + query.ToString());
				}
				
				if ((hits1[i].doc != hits2[i].doc) || System.Math.Abs(hits1[i].score - hits2[i].score) > scoreTolerance)
				{
					Assert.Fail("Hit " + i + ", doc nrs " + hits1[i].doc + " and " + hits2[i].doc + "\nunequal       : " + hits1[i].score + "\n           and: " + hits2[i].score + "\nfor query:" + query.ToString());
				}
			}
		}
		
		public static System.String Hits2str(ScoreDoc[] hits1, ScoreDoc[] hits2, int start, int end)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			int len1 = hits1 == null?0:hits1.Length;
			int len2 = hits2 == null?0:hits2.Length;
			if (end <= 0)
			{
				end = System.Math.Max(len1, len2);
			}
			
			sb.Append("Hits length1=").Append(len1).Append("\tlength2=").Append(len2);
			
			sb.Append('\n');
			for (int i = start; i < end; i++)
			{
				sb.Append("hit=").Append(i).Append(':');
				if (i < len1)
				{
					sb.Append(" doc").Append(hits1[i].doc).Append('=').Append(hits1[i].score);
				}
				else
				{
					sb.Append("               ");
				}
				sb.Append(",\t");
				if (i < len2)
				{
					sb.Append(" doc").Append(hits2[i].doc).Append('=').Append(hits2[i].score);
				}
				sb.Append('\n');
			}
			return sb.ToString();
		}
		
		
		public static System.String TopdocsString(TopDocs docs, int start, int end)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			sb.Append("TopDocs totalHits=").Append(docs.totalHits).Append(" top=").Append(docs.scoreDocs.Length).Append('\n');
			if (end <= 0)
				end = docs.scoreDocs.Length;
			else
				end = System.Math.Min(end, docs.scoreDocs.Length);
			for (int i = start; i < end; i++)
			{
				sb.Append('\t');
				sb.Append(i);
				sb.Append(") doc=");
				sb.Append(docs.scoreDocs[i].doc);
				sb.Append("\tscore=");
				sb.Append(docs.scoreDocs[i].score);
				sb.Append('\n');
			}
			return sb.ToString();
		}
		
		/// <summary> Asserts that the explanation value for every document matching a
		/// query corresponds with the true score. 
		/// 
		/// </summary>
		/// <seealso cref="ExplanationAsserter">
		/// </seealso>
		/// <seealso cref="CheckExplanations(Query, String, Searcher, boolean) for a">
		/// "deep" testing of the explanation details.
		/// 
		/// </seealso>
		/// <param name="query">the query to test
		/// </param>
		/// <param name="searcher">the searcher to test the query against
		/// </param>
		/// <param name="defaultFieldName">used for displaing the query in assertion messages
		/// </param>
		public static void  CheckExplanations(Query query, System.String defaultFieldName, Searcher searcher)
		{
			CheckExplanations(query, defaultFieldName, searcher, false);
		}
		
		/// <summary> Asserts that the explanation value for every document matching a
		/// query corresponds with the true score.  Optionally does "deep" 
		/// testing of the explanation details.
		/// 
		/// </summary>
		/// <seealso cref="ExplanationAsserter">
		/// </seealso>
		/// <param name="query">the query to test
		/// </param>
		/// <param name="searcher">the searcher to test the query against
		/// </param>
		/// <param name="defaultFieldName">used for displaing the query in assertion messages
		/// </param>
		/// <param name="deep">indicates whether a deep comparison of sub-Explanation details should be executed
		/// </param>
		public static void  CheckExplanations(Query query, System.String defaultFieldName, Searcher searcher, bool deep)
		{
			
			searcher.Search(query, new ExplanationAsserter(query, defaultFieldName, searcher, deep));
		}
		
		/// <summary> Assert that an explanation has the expected score, and optionally that its
		/// sub-details max/sum/factor match to that score.
		/// 
		/// </summary>
		/// <param name="q">String representation of the query for assertion messages
		/// </param>
		/// <param name="doc">Document ID for assertion messages
		/// </param>
		/// <param name="score">Real score value of doc with query q
		/// </param>
		/// <param name="deep">indicates whether a deep comparison of sub-Explanation details should be executed
		/// </param>
		/// <param name="expl">The Explanation to match against score
		/// </param>
		public static void  VerifyExplanation(System.String q, int doc, float score, bool deep, Explanation expl)
		{
			float value_Renamed = expl.GetValue();
			Assert.AreEqual(score, value_Renamed, EXPLAIN_SCORE_TOLERANCE_DELTA, q + ": score(doc=" + doc + ")=" + score + " != explanationScore=" + value_Renamed + " Explanation: " + expl);
			
			if (!deep)
				return ;
			
			Explanation[] detail = expl.GetDetails();
			if (detail != null)
			{
				if (detail.Length == 1)
				{
					// simple containment, no matter what the description says, 
					// just verify contained expl has same score
					VerifyExplanation(q, doc, score, deep, detail[0]);
				}
				else
				{
					// explanation must either:
					// - end with one of: "product of:", "sum of:", "max of:", or
					// - have "max plus <x> times others" (where <x> is float).
					float x = 0;
					System.String descr = expl.GetDescription().ToLower();
					bool productOf = descr.EndsWith("product of:");
					bool sumOf = descr.EndsWith("sum of:");
					bool maxOf = descr.EndsWith("max of:");
					bool maxTimesOthers = false;
					if (!(productOf || sumOf || maxOf))
					{
						// maybe 'max plus x times others'
						int k1 = descr.IndexOf("max plus ");
						if (k1 >= 0)
						{
							k1 += "max plus ".Length;
							int k2 = descr.IndexOf(" ", k1);
							try
							{
                                x = SupportClass.Single.Parse(descr.Substring(k1, (k2) - (k1)).Trim());
								if (descr.Substring(k2).Trim().Equals("times others of:"))
								{
									maxTimesOthers = true;
								}
							}
							catch (System.FormatException e)
							{
							}
						}
					}
					Assert.IsTrue(productOf || sumOf || maxOf || maxTimesOthers, q + ": multi valued explanation description=\"" + descr + "\" must be 'max of plus x times others' or end with 'product of'" + " or 'sum of:' or 'max of:' - " + expl);
					float sum = 0;
					float product = 1;
					float max = 0;
					for (int i = 0; i < detail.Length; i++)
					{
						float dval = detail[i].GetValue();
						VerifyExplanation(q, doc, dval, deep, detail[i]);
						product *= dval;
						sum += dval;
						max = System.Math.Max(max, dval);
					}
					float combined = 0;
					if (productOf)
					{
						combined = product;
					}
					else if (sumOf)
					{
						combined = sum;
					}
					else if (maxOf)
					{
						combined = max;
					}
					else if (maxTimesOthers)
					{
						combined = max + x * (sum - max);
					}
					else
					{
						Assert.IsTrue(false, "should never get here!");
					}
					Assert.AreEqual(combined, value_Renamed, EXPLAIN_SCORE_TOLERANCE_DELTA, q + ": actual subDetails combined==" + combined + " != value=" + value_Renamed + " Explanation: " + expl);
				}
			}
		}
		
		/// <summary> an IndexSearcher that implicitly checks hte explanation of every match
		/// whenever it executes a search.
		/// 
		/// </summary>
		/// <seealso cref="ExplanationAsserter">
		/// </seealso>
		public class ExplanationAssertingSearcher:IndexSearcher
		{
			public ExplanationAssertingSearcher(Directory d):base(d)
			{
			}
			public ExplanationAssertingSearcher(IndexReader r):base(r)
			{
			}
			protected internal virtual void  CheckExplanations(Query q)
			{
				base.Search(q, null, new ExplanationAsserter(q, null, this));
			}
			public override TopFieldDocs Search(Query query, Filter filter, int n, Sort sort)
			{
				
				CheckExplanations(query);
				return base.Search(query, filter, n, sort);
			}
			/// <deprecated> use {@link #Search(Query, Collector)} instead. 
			/// </deprecated>
            [Obsolete("use Search(Query, Collector) instead. ")]
			public override void  Search(Query query, HitCollector results)
			{
				Search(query, new HitCollectorWrapper(results));
			}
			public override void  Search(Query query, Collector results)
			{
				CheckExplanations(query);
				base.Search(query, results);
			}
			/// <deprecated> use {@link #Search(Query, Filter, Collector)} instead. 
			/// </deprecated>
            [Obsolete("use Search(Query, Filter, Collector) instead. ")]
			public override void  Search(Query query, Filter filter, HitCollector results)
			{
				Search(query, filter, new HitCollectorWrapper(results));
			}
			public override void  Search(Query query, Filter filter, Collector results)
			{
				CheckExplanations(query);
				base.Search(query, filter, results);
			}
			public override TopDocs Search(Query query, Filter filter, int n)
			{
				
				CheckExplanations(query);
				return base.Search(query, filter, n);
			}
		}
		
		/// <summary> Asserts that the score explanation for every document matching a
		/// query corresponds with the true score.
		/// 
		/// NOTE: this HitCollector should only be used with the Query and Searcher
		/// specified at when it is constructed.
		/// 
		/// </summary>
		/// <seealso cref="CheckHits.verifyExplanation">
		/// </seealso>
		public class ExplanationAsserter:Collector
		{
			
			/// <deprecated>
			/// </deprecated>
			/// <seealso cref="CheckHits.EXPLAIN_SCORE_TOLERANCE_DELTA">
			/// </seealso>
			public static float SCORE_TOLERANCE_DELTA = 0.00005f;
			
			internal Query q;
			internal Searcher s;
			internal System.String d;
			internal bool deep;
			
			internal Scorer scorer;
			private int base_Renamed = 0;
			
			/// <summary>Constructs an instance which does shallow tests on the Explanation </summary>
			public ExplanationAsserter(Query q, System.String defaultFieldName, Searcher s):this(q, defaultFieldName, s, false)
			{
			}
			public ExplanationAsserter(Query q, System.String defaultFieldName, Searcher s, bool deep)
			{
				this.q = q;
				this.s = s;
				this.d = q.ToString(defaultFieldName);
				this.deep = deep;
			}
			
			public override void  SetScorer(Scorer scorer)
			{
				this.scorer = scorer;
			}
			
			public override void  Collect(int doc)
			{
				Explanation exp = null;
				doc = doc + base_Renamed;
				try
				{
					exp = s.Explain(q, doc);
				}
				catch (System.IO.IOException e)
				{
					throw new System.SystemException("exception in hitcollector of [[" + d + "]] for #" + doc, e);
				}
				
				Assert.IsNotNull(exp, "Explanation of [[" + d + "]] for #" + doc + " is null");
				Lucene.Net.Search.CheckHits.VerifyExplanation(d, doc, scorer.Score(), deep, exp);
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
	}
}