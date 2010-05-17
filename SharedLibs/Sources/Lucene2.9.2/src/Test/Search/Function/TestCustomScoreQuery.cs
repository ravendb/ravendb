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

using CorruptIndexException = Lucene.Net.Index.CorruptIndexException;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using Explanation = Lucene.Net.Search.Explanation;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using QueryUtils = Lucene.Net.Search.QueryUtils;
using TopDocs = Lucene.Net.Search.TopDocs;

using Lucene.Net.Search;
using Lucene.Net.Index;

namespace Lucene.Net.Search.Function
{
	
	/// <summary> Test CustomScoreQuery search.</summary>
    [TestFixture]
	public class TestCustomScoreQuery:FunctionTestSetup
	{
		
		/* @override constructor */
		public TestCustomScoreQuery(System.String name):base(name, true)
		{
		}
        public TestCustomScoreQuery()
            : base()
        {
        }
		/// <summary>Test that CustomScoreQuery of Type.BYTE returns the expected scores. </summary>
		[Test]
		public virtual void  TestCustomScoreByte()
		{
			// INT field values are small enough to be parsed as byte
			DoTestCustomScore(INT_FIELD, FieldScoreQuery.Type.BYTE, 1.0);
			DoTestCustomScore(INT_FIELD, FieldScoreQuery.Type.BYTE, 2.0);
		}
		
		/// <summary>Test that CustomScoreQuery of Type.SHORT returns the expected scores. </summary>
		[Test]
		public virtual void  TestCustomScoreShort()
		{
			// INT field values are small enough to be parsed as short
			DoTestCustomScore(INT_FIELD, FieldScoreQuery.Type.SHORT, 1.0);
			DoTestCustomScore(INT_FIELD, FieldScoreQuery.Type.SHORT, 3.0);
		}
		
		/// <summary>Test that CustomScoreQuery of Type.INT returns the expected scores. </summary>
		[Test]
		public virtual void  TestCustomScoreInt()
		{
			DoTestCustomScore(INT_FIELD, FieldScoreQuery.Type.INT, 1.0);
			DoTestCustomScore(INT_FIELD, FieldScoreQuery.Type.INT, 4.0);
		}
		
		/// <summary>Test that CustomScoreQuery of Type.FLOAT returns the expected scores. </summary>
		[Test]
		public virtual void  TestCustomScoreFloat()
		{
			// INT field can be parsed as float
			DoTestCustomScore(INT_FIELD, FieldScoreQuery.Type.FLOAT, 1.0);
			DoTestCustomScore(INT_FIELD, FieldScoreQuery.Type.FLOAT, 5.0);
			// same values, but in float format
			DoTestCustomScore(FLOAT_FIELD, FieldScoreQuery.Type.FLOAT, 1.0);
			DoTestCustomScore(FLOAT_FIELD, FieldScoreQuery.Type.FLOAT, 6.0);
		}
		
		// must have static class otherwise serialization tests fail
		[Serializable]
		private class CustomAddQuery:CustomScoreQuery
		{
			// constructor
			internal CustomAddQuery(Query q, ValueSourceQuery qValSrc):base(q, qValSrc)
			{
			}
			/*(non-Javadoc) @see Lucene.Net.Search.Function.CustomScoreQuery#name() */
			public override System.String Name()
			{
				return "customAdd";
			}
            protected override CustomScoreProvider GetCustomScoreProvider(IndexReader reader)
            {
                return new AnonymousCustomScoreProvider(reader);
            }

            class AnonymousCustomScoreProvider : CustomScoreProvider
            {
                IndexReader reader;

                public AnonymousCustomScoreProvider(IndexReader reader) : base(reader)
                {
                    this.reader = reader;
                }

                public override float CustomScore(int doc, float subQueryScore, float valSrcScore)
                {
                    return subQueryScore + valSrcScore;
                }

                public override Explanation CustomExplain(int doc, Explanation subQueryExpl, Explanation valSrcExpl)
                {
                    float valSrcScore = valSrcExpl == null ? 0 : valSrcExpl.GetValue();
                    Explanation exp = new Explanation(valSrcScore + subQueryExpl.GetValue(), "custom score: sum of:");
                    exp.AddDetail(subQueryExpl);
                    if (valSrcExpl != null)
                    {
                        exp.AddDetail(valSrcExpl);
                    }
                    return exp;
                }
            }
		}
		
		// must have static class otherwise serialization tests fail
		[Serializable]
		private class CustomMulAddQuery:CustomScoreQuery
		{
			// constructor
			internal CustomMulAddQuery(Query q, ValueSourceQuery qValSrc1, ValueSourceQuery qValSrc2):base(q, new ValueSourceQuery[]{qValSrc1, qValSrc2})
			{
			}
			/*(non-Javadoc) @see Lucene.Net.Search.Function.CustomScoreQuery#name() */
			public override System.String Name()
			{
				return "customMulAdd";
			}
			/*(non-Javadoc) @see Lucene.Net.Search.Function.CustomScoreQuery#customScore(int, float, float) */
            protected override CustomScoreProvider GetCustomScoreProvider(IndexReader reader)
            {
                return new AnonymousCustomScoreProvider(reader);
            }

            class AnonymousCustomScoreProvider : CustomScoreProvider
            {
                IndexReader reader;

                public AnonymousCustomScoreProvider(IndexReader reader) : base(reader)
                {
                    this.reader = reader;
                }

                public override float CustomScore(int doc, float subQueryScore, float[] valSrcScores)
                {
                    if (valSrcScores.Length == 0)
                    {
                        return subQueryScore;
                    }
                    if (valSrcScores.Length == 1)
                    {
                        return subQueryScore + valSrcScores[0];
                        // confirm that skipping beyond the last doc, on the
                        // previous reader, hits NO_MORE_DOCS
                    }
                    return (subQueryScore + valSrcScores[0]) * valSrcScores[1]; // we know there are two
                }

                public override Explanation CustomExplain(int doc, Explanation subQueryExpl, Explanation[] valSrcExpls)
                {
                    if (valSrcExpls.Length == 0)
                    {
                        return subQueryExpl;
                    }
                    Explanation exp = new Explanation(valSrcExpls[0].GetValue() + subQueryExpl.GetValue(), "sum of:");
                    exp.AddDetail(subQueryExpl);
                    exp.AddDetail(valSrcExpls[0]);
                    if (valSrcExpls.Length == 1)
                    {
                        exp.SetDescription("CustomMulAdd, sum of:");
                        return exp;
                    }
                    Explanation exp2 = new Explanation(valSrcExpls[1].GetValue() * exp.GetValue(), "custom score: product of:");
                    exp2.AddDetail(valSrcExpls[1]);
                    exp2.AddDetail(exp);
                    return exp2;
                }
            }
		}

        private class CustomExternalQuery : CustomScoreQuery 
        {
            protected override CustomScoreProvider GetCustomScoreProvider(IndexReader reader) 
            {
                int[] values = FieldCache_Fields.DEFAULT.GetInts(reader, INT_FIELD);
                return new AnonymousCustomScoreProvider(reader,values);
            }
            
            class AnonymousCustomScoreProvider : CustomScoreProvider 
            {
                IndexReader reader;
                int[] values = null;

                public AnonymousCustomScoreProvider(IndexReader reader, int[] values) : base(reader)
                {
                    this.reader = reader;
                    this.values = values;
                }

                public override float CustomScore(int doc, float subScore, float valSrcScore)
                {
                    Assert.IsTrue(doc <= reader.MaxDoc());
                    return (float)values[doc];
                }
            }
            
            public CustomExternalQuery(Query q) : base(q)
            {  }
        }

        [Test]
        public void TestCustomExternalQuery() 
        {
            QueryParser qp = new QueryParser(TEXT_FIELD,anlzr); 
            String qtxt = "first aid text"; // from the doc texts in FunctionQuerySetup.
            Query q1 = qp.Parse(qtxt); 
        
            Query q = new CustomExternalQuery(q1);
            Log(q);

            IndexSearcher s = new IndexSearcher(dir);
            TopDocs hits = s.Search(q, 1000);
            Assert.AreEqual(N_DOCS, hits.totalHits);
            for(int i=0;i<N_DOCS;i++) 
            {
                int doc = hits.scoreDocs[i].doc;
                float score = hits.scoreDocs[i].score;
                Assert.AreEqual(score, (float)1 + (4 * doc) % N_DOCS, 0.0001, "doc=" + doc);
            }
            s.Close();
        }

		
		// Test that FieldScoreQuery returns docs with expected score.
		private void  DoTestCustomScore(System.String field, FieldScoreQuery.Type tp, double dboost)
		{
			float boost = (float) dboost;
			IndexSearcher s = new IndexSearcher(dir);
			FieldScoreQuery qValSrc = new FieldScoreQuery(field, tp); // a query that would score by the field
			QueryParser qp = new QueryParser(TEXT_FIELD, anlzr);
			System.String qtxt = "first aid text"; // from the doc texts in FunctionQuerySetup.
			
			// regular (boolean) query.
			Query q1 = qp.Parse(qtxt);
			Log(q1);
			
			// custom query, that should score the same as q1.
			CustomScoreQuery q2CustomNeutral = new CustomScoreQuery(q1);
			q2CustomNeutral.SetBoost(boost);
			Log(q2CustomNeutral);
			
			// custom query, that should (by default) multiply the scores of q1 by that of the field
			CustomScoreQuery q3CustomMul = new CustomScoreQuery(q1, qValSrc);
			q3CustomMul.SetStrict(true);
			q3CustomMul.SetBoost(boost);
			Log(q3CustomMul);
			
			// custom query, that should add the scores of q1 to that of the field
			CustomScoreQuery q4CustomAdd = new CustomAddQuery(q1, qValSrc);
			q4CustomAdd.SetStrict(true);
			q4CustomAdd.SetBoost(boost);
			Log(q4CustomAdd);
			
			// custom query, that multiplies and adds the field score to that of q1
			CustomScoreQuery q5CustomMulAdd = new CustomMulAddQuery(q1, qValSrc, qValSrc);
			q5CustomMulAdd.SetStrict(true);
			q5CustomMulAdd.SetBoost(boost);
			Log(q5CustomMulAdd);
			
			// do al the searches 
			TopDocs td1 = s.Search(q1, null, 1000);
			TopDocs td2CustomNeutral = s.Search(q2CustomNeutral, null, 1000);
			TopDocs td3CustomMul = s.Search(q3CustomMul, null, 1000);
			TopDocs td4CustomAdd = s.Search(q4CustomAdd, null, 1000);
			TopDocs td5CustomMulAdd = s.Search(q5CustomMulAdd, null, 1000);
			
			// put results in map so we can verify the scores although they have changed
			System.Collections.Hashtable h1 = TopDocsToMap(td1);
			System.Collections.Hashtable h2CustomNeutral = TopDocsToMap(td2CustomNeutral);
			System.Collections.Hashtable h3CustomMul = TopDocsToMap(td3CustomMul);
			System.Collections.Hashtable h4CustomAdd = TopDocsToMap(td4CustomAdd);
			System.Collections.Hashtable h5CustomMulAdd = TopDocsToMap(td5CustomMulAdd);
			
			VerifyResults(boost, s, h1, h2CustomNeutral, h3CustomMul, h4CustomAdd, h5CustomMulAdd, q1, q2CustomNeutral, q3CustomMul, q4CustomAdd, q5CustomMulAdd);
		}
		
		// verify results are as expected.
		private void  VerifyResults(float boost, IndexSearcher s, System.Collections.Hashtable h1, System.Collections.Hashtable h2customNeutral, System.Collections.Hashtable h3CustomMul, System.Collections.Hashtable h4CustomAdd, System.Collections.Hashtable h5CustomMulAdd, Query q1, Query q2, Query q3, Query q4, Query q5)
		{
			
			// verify numbers of matches
			Log("#hits = " + h1.Count);
			Assert.AreEqual(h1.Count, h2customNeutral.Count, "queries should have same #hits");
			Assert.AreEqual(h1.Count, h3CustomMul.Count, "queries should have same #hits");
			Assert.AreEqual(h1.Count, h4CustomAdd.Count, "queries should have same #hits");
			Assert.AreEqual(h1.Count, h5CustomMulAdd.Count, "queries should have same #hits");
			
			// verify scores ratios
			for (System.Collections.IEnumerator it = h1.Keys.GetEnumerator(); it.MoveNext(); )
			{
				System.Int32 x = (System.Int32) it.Current;
				
				int doc = x;
				Log("doc = " + doc);
				
				float fieldScore = ExpectedFieldScore(s.GetIndexReader().Document(doc).Get(ID_FIELD));
				Log("fieldScore = " + fieldScore);
				Assert.IsTrue(fieldScore > 0, "fieldScore should not be 0");
				
				float score1 = (float) ((System.Single) h1[x]);
				LogResult("score1=", s, q1, doc, score1);
				
				float score2 = (float) ((System.Single) h2customNeutral[x]);
				LogResult("score2=", s, q2, doc, score2);
				Assert.AreEqual(boost * score1, score2, TEST_SCORE_TOLERANCE_DELTA, "same score (just boosted) for neutral");
				
				float score3 = (float) ((System.Single) h3CustomMul[x]);
				LogResult("score3=", s, q3, doc, score3);
				Assert.AreEqual(boost * fieldScore * score1, score3, TEST_SCORE_TOLERANCE_DELTA, "new score for custom mul");
				
				float score4 = (float) ((System.Single) h4CustomAdd[x]);
				LogResult("score4=", s, q4, doc, score4);
				Assert.AreEqual(boost * (fieldScore + score1), score4, TEST_SCORE_TOLERANCE_DELTA, "new score for custom add");
				
				float score5 = (float) ((System.Single) h5CustomMulAdd[x]);
				LogResult("score5=", s, q5, doc, score5);
				Assert.AreEqual(boost * fieldScore * (score1 + fieldScore), score5, TEST_SCORE_TOLERANCE_DELTA, "new score for custom mul add");
			}
		}
		
		private void  LogResult(System.String msg, IndexSearcher s, Query q, int doc, float score1)
		{
			QueryUtils.Check(q, s);
			Log(msg + " " + score1);
			Log("Explain by: " + q);
			Log(s.Explain(q, doc));
		}
		
		// since custom scoring modifies the order of docs, map results 
		// by doc ids so that we can later compare/verify them 
		private System.Collections.Hashtable TopDocsToMap(TopDocs td)
		{
			System.Collections.Hashtable h = new System.Collections.Hashtable();
			for (int i = 0; i < td.totalHits; i++)
			{
				h[(System.Int32) td.scoreDocs[i].doc] = (float) td.scoreDocs[i].score;
			}
			return h;
		}
	}
}