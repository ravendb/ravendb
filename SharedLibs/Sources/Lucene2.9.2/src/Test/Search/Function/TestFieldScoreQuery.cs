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
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using QueryUtils = Lucene.Net.Search.QueryUtils;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using TopDocs = Lucene.Net.Search.TopDocs;

namespace Lucene.Net.Search.Function
{
	
	/// <summary> Test FieldScoreQuery search.
	/// <p/>
	/// Tests here create an index with a few documents, each having
	/// an int value indexed  field and a float value indexed field.
	/// The values of these fields are later used for scoring.
	/// <p/>
	/// The rank tests use Hits to verify that docs are ordered (by score) as expected.
	/// <p/>
	/// The exact score tests use TopDocs top to verify the exact score.  
	/// </summary>
    [TestFixture]
	public class TestFieldScoreQuery:FunctionTestSetup
	{
		
		/* @override constructor */
		public TestFieldScoreQuery(System.String name):base(name, true)
		{
		}
        public TestFieldScoreQuery()
            : base()
        {
        }
		/// <summary>Test that FieldScoreQuery of Type.BYTE returns docs in expected order. </summary>
		[Test]
		public virtual void  TestRankByte()
		{
			// INT field values are small enough to be parsed as byte
			DoTestRank(INT_FIELD, FieldScoreQuery.Type.BYTE);
		}
		
		/// <summary>Test that FieldScoreQuery of Type.SHORT returns docs in expected order. </summary>
		[Test]
		public virtual void  TestRankShort()
		{
			// INT field values are small enough to be parsed as short
			DoTestRank(INT_FIELD, FieldScoreQuery.Type.SHORT);
		}
		
		/// <summary>Test that FieldScoreQuery of Type.INT returns docs in expected order. </summary>
		[Test]
		public virtual void  TestRankInt()
		{
			DoTestRank(INT_FIELD, FieldScoreQuery.Type.INT);
		}
		
		/// <summary>Test that FieldScoreQuery of Type.FLOAT returns docs in expected order. </summary>
		[Test]
		public virtual void  TestRankFloat()
		{
			// INT field can be parsed as float
			DoTestRank(INT_FIELD, FieldScoreQuery.Type.FLOAT);
			// same values, but in flot format
			DoTestRank(FLOAT_FIELD, FieldScoreQuery.Type.FLOAT);
		}
		
		// Test that FieldScoreQuery returns docs in expected order.
		private void  DoTestRank(System.String field, FieldScoreQuery.Type tp)
		{
			IndexSearcher s = new IndexSearcher(dir);
			Query q = new FieldScoreQuery(field, tp);
			Log("test: " + q);
			QueryUtils.Check(q, s);
			ScoreDoc[] h = s.Search(q, null, 1000).scoreDocs;
			Assert.AreEqual(N_DOCS, h.Length, "All docs should be matched!");
			System.String prevID = "ID" + (N_DOCS + 1); // greater than all ids of docs in this test
			for (int i = 0; i < h.Length; i++)
			{
				System.String resID = s.Doc(h[i].doc).Get(ID_FIELD);
				Log(i + ".   score=" + h[i].score + "  -  " + resID);
				Log(s.Explain(q, h[i].doc));
				Assert.IsTrue(String.CompareOrdinal(resID, prevID) < 0, "res id " + resID + " should be < prev res id " + prevID);
				prevID = resID;
			}
		}
		
		/// <summary>Test that FieldScoreQuery of Type.BYTE returns the expected scores. </summary>
		[Test]
		public virtual void  TestExactScoreByte()
		{
			// INT field values are small enough to be parsed as byte
			DoTestExactScore(INT_FIELD, FieldScoreQuery.Type.BYTE);
		}
		
		/// <summary>Test that FieldScoreQuery of Type.SHORT returns the expected scores. </summary>
		[Test]
		public virtual void  TestExactScoreShort()
		{
			// INT field values are small enough to be parsed as short
			DoTestExactScore(INT_FIELD, FieldScoreQuery.Type.SHORT);
		}
		
		/// <summary>Test that FieldScoreQuery of Type.INT returns the expected scores. </summary>
		[Test]
		public virtual void  TestExactScoreInt()
		{
			DoTestExactScore(INT_FIELD, FieldScoreQuery.Type.INT);
		}
		
		/// <summary>Test that FieldScoreQuery of Type.FLOAT returns the expected scores. </summary>
		[Test]
		public virtual void  TestExactScoreFloat()
		{
			// INT field can be parsed as float
			DoTestExactScore(INT_FIELD, FieldScoreQuery.Type.FLOAT);
			// same values, but in flot format
			DoTestExactScore(FLOAT_FIELD, FieldScoreQuery.Type.FLOAT);
		}
		
		// Test that FieldScoreQuery returns docs with expected score.
		private void  DoTestExactScore(System.String field, FieldScoreQuery.Type tp)
		{
			IndexSearcher s = new IndexSearcher(dir);
			Query q = new FieldScoreQuery(field, tp);
			TopDocs td = s.Search(q, null, 1000);
			Assert.AreEqual(N_DOCS, td.totalHits, "All docs should be matched!");
			ScoreDoc[] sd = td.scoreDocs;
			for (int i = 0; i < sd.Length; i++)
			{
				float score = sd[i].score;
				Log(s.Explain(q, sd[i].doc));
				System.String id = s.GetIndexReader().Document(sd[i].doc).Get(ID_FIELD);
				float expectedScore = ExpectedFieldScore(id); // "ID7" --> 7.0
				Assert.AreEqual(expectedScore, score, TEST_SCORE_TOLERANCE_DELTA, "score of " + id + " shuould be " + expectedScore + " != " + score);
			}
		}
		
		/// <summary>Test that FieldScoreQuery of Type.BYTE caches/reuses loaded values and consumes the proper RAM resources. </summary>
		[Test]
		public virtual void  TestCachingByte()
		{
			// INT field values are small enough to be parsed as byte
			DoTestCaching(INT_FIELD, FieldScoreQuery.Type.BYTE);
		}
		
		/// <summary>Test that FieldScoreQuery of Type.SHORT caches/reuses loaded values and consumes the proper RAM resources. </summary>
		[Test]
		public virtual void  TestCachingShort()
		{
			// INT field values are small enough to be parsed as short
			DoTestCaching(INT_FIELD, FieldScoreQuery.Type.SHORT);
		}
		
		/// <summary>Test that FieldScoreQuery of Type.INT caches/reuses loaded values and consumes the proper RAM resources. </summary>
		[Test]
		public virtual void  TestCachingInt()
		{
			DoTestCaching(INT_FIELD, FieldScoreQuery.Type.INT);
		}
		
		/// <summary>Test that FieldScoreQuery of Type.FLOAT caches/reuses loaded values and consumes the proper RAM resources. </summary>
		[Test]
		public virtual void  TestCachingFloat()
		{
			// INT field values can be parsed as float
			DoTestCaching(INT_FIELD, FieldScoreQuery.Type.FLOAT);
			// same values, but in flot format
			DoTestCaching(FLOAT_FIELD, FieldScoreQuery.Type.FLOAT);
		}
		
		// Test that values loaded for FieldScoreQuery are cached properly and consumes the proper RAM resources.
		private void  DoTestCaching(System.String field, FieldScoreQuery.Type tp)
		{
			// prepare expected array types for comparison
			System.Collections.Hashtable expectedArrayTypes = new System.Collections.Hashtable();
			expectedArrayTypes[FieldScoreQuery.Type.BYTE] = new sbyte[0];
			expectedArrayTypes[FieldScoreQuery.Type.SHORT] = new short[0];
			expectedArrayTypes[FieldScoreQuery.Type.INT] = new int[0];
			expectedArrayTypes[FieldScoreQuery.Type.FLOAT] = new float[0];
			
			IndexSearcher s = new IndexSearcher(dir);
			System.Object[] innerArray = new Object[s.GetIndexReader().GetSequentialSubReaders().Length];
			
			bool warned = false; // print warning once.
			for (int i = 0; i < 10; i++)
			{
				FieldScoreQuery q = new FieldScoreQuery(field, tp);
				ScoreDoc[] h = s.Search(q, null, 1000).scoreDocs;
				Assert.AreEqual(N_DOCS, h.Length, "All docs should be matched!");
				IndexReader[] readers = s.GetIndexReader().GetSequentialSubReaders();
				for (int j = 0; j < readers.Length; j++)
				{
					IndexReader reader = readers[j];
					try
					{
						if (i == 0)
						{
							innerArray[j] = q.valSrc_ForNUnit.GetValues(reader).GetInnerArray();
							Log(i + ".  compare: " + innerArray[j].GetType() + " to " + expectedArrayTypes[tp].GetType());
							Assert.AreEqual(innerArray[j].GetType(), expectedArrayTypes[tp].GetType(), "field values should be cached in the correct array type!");
						}
						else
						{
							Log(i + ".  compare: " + innerArray[j] + " to " + q.valSrc_ForNUnit.GetValues(reader).GetInnerArray());
							Assert.AreSame(innerArray[j], q.valSrc_ForNUnit.GetValues(reader).GetInnerArray(), "field values should be cached and reused!");
						}
					}
					catch (System.NotSupportedException e)
					{
						if (!warned)
						{
							System.Console.Error.WriteLine("WARNING: " +  TestName() + " cannot fully test values of " + q);
							warned = true;
						}
					}
				}
			}
			
			// verify new values are reloaded (not reused) for a new reader
			s = new IndexSearcher(dir);
			FieldScoreQuery q2 = new FieldScoreQuery(field, tp);
			ScoreDoc[] h2 = s.Search(q2, null, 1000).scoreDocs;
			Assert.AreEqual(N_DOCS, h2.Length, "All docs should be matched!");
			IndexReader[] readers2 = s.GetIndexReader().GetSequentialSubReaders();
			for (int j = 0; j < readers2.Length; j++)
			{
				IndexReader reader = readers2[j];
				try
				{
					Log("compare: " + innerArray + " to " + q2.valSrc_ForNUnit.GetValues(reader).GetInnerArray());
					Assert.AreNotSame(innerArray, q2.valSrc_ForNUnit.GetValues(reader).GetInnerArray(), "cached field values should not be reused if reader as changed!");
				}
				catch (System.NotSupportedException e)
				{
					if (!warned)
					{
						System.Console.Error.WriteLine("WARNING: " + TestName() + " cannot fully test values of " + q2);
						warned = true;
					}
				}
			}
		}
		
		private System.String TestName()
		{
            return Lucene.Net.TestCase.GetFullName();
		}
	}
}