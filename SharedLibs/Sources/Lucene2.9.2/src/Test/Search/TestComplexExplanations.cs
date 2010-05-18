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

using Occur = Lucene.Net.Search.BooleanClause.Occur;
using Lucene.Net.Search.Spans;

namespace Lucene.Net.Search
{
	
	/// <summary> TestExplanations subclass that builds up super crazy complex queries
	/// on the assumption that if the explanations work out right for them,
	/// they should work for anything.
	/// </summary>
    [TestFixture]
	public class TestComplexExplanations:TestExplanations
	{
		[Serializable]
		private class AnonymousClassDefaultSimilarity:DefaultSimilarity
		{
			public override float QueryNorm(float sumOfSquaredWeights)
			{
				return 1.0f; // / (float) Math.sqrt(1.0f + sumOfSquaredWeights);
			}
		}
		
		/// <summary> Override the Similarity used in our searcher with one that plays
		/// nice with boosts of 0.0
		/// </summary>
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			searcher.SetSimilarity(createQnorm1Similarity());
		}

        [TearDown]
        public override void TearDown()
        {
            base.TearDown();
        }
		
		// must be static for weight serialization tests 
		private static DefaultSimilarity createQnorm1Similarity()
		{
			return new AnonymousClassDefaultSimilarity();
		}
		
		
		[Test]
		public virtual void  Test1()
		{
			
			BooleanQuery q = new BooleanQuery();
			
			q.Add(qp.Parse("\"w1 w2\"~1"), Occur.MUST);
			q.Add(Snear(St("w2"), Sor("w5", "zz"), 4, true), Occur.SHOULD);
			q.Add(Snear(Sf("w3", 2), St("w2"), St("w3"), 5, true), Occur.SHOULD);
			
			Query t = new FilteredQuery(qp.Parse("xx"), new ItemizedFilter(new int[]{1, 3}));
			t.SetBoost(1000);
			q.Add(t, Occur.SHOULD);
			
			t = new ConstantScoreQuery(new ItemizedFilter(new int[]{0, 2}));
			t.SetBoost(30);
			q.Add(t, Occur.SHOULD);
			
			DisjunctionMaxQuery dm = new DisjunctionMaxQuery(0.2f);
			dm.Add(Snear(St("w2"), Sor("w5", "zz"), 4, true));
			dm.Add(qp.Parse("QQ"));
			dm.Add(qp.Parse("xx yy -zz"));
			dm.Add(qp.Parse("-xx -w1"));
			
			DisjunctionMaxQuery dm2 = new DisjunctionMaxQuery(0.5f);
			dm2.Add(qp.Parse("w1"));
			dm2.Add(qp.Parse("w2"));
			dm2.Add(qp.Parse("w3"));
			dm.Add(dm2);
			
			q.Add(dm, Occur.SHOULD);
			
			BooleanQuery b = new BooleanQuery();
			b.SetMinimumNumberShouldMatch(2);
			b.Add(Snear("w1", "w2", 1, true), Occur.SHOULD);
			b.Add(Snear("w2", "w3", 1, true), Occur.SHOULD);
			b.Add(Snear("w1", "w3", 3, true), Occur.SHOULD);
			
			q.Add(b, Occur.SHOULD);
			
			Qtest(q, new int[]{0, 1, 2});
		}
		
		[Test]
		public virtual void  Test2()
		{
			
			BooleanQuery q = new BooleanQuery();
			
			q.Add(qp.Parse("\"w1 w2\"~1"), Occur.MUST);
			q.Add(Snear(St("w2"), Sor("w5", "zz"), 4, true), Occur.SHOULD);
			q.Add(Snear(Sf("w3", 2), St("w2"), St("w3"), 5, true), Occur.SHOULD);
			
			Query t = new FilteredQuery(qp.Parse("xx"), new ItemizedFilter(new int[]{1, 3}));
			t.SetBoost(1000);
			q.Add(t, Occur.SHOULD);
			
			t = new ConstantScoreQuery(new ItemizedFilter(new int[]{0, 2}));
			t.SetBoost(- 20.0f);
			q.Add(t, Occur.SHOULD);
			
			DisjunctionMaxQuery dm = new DisjunctionMaxQuery(0.2f);
			dm.Add(Snear(St("w2"), Sor("w5", "zz"), 4, true));
			dm.Add(qp.Parse("QQ"));
			dm.Add(qp.Parse("xx yy -zz"));
			dm.Add(qp.Parse("-xx -w1"));
			
			DisjunctionMaxQuery dm2 = new DisjunctionMaxQuery(0.5f);
			dm2.Add(qp.Parse("w1"));
			dm2.Add(qp.Parse("w2"));
			dm2.Add(qp.Parse("w3"));
			dm.Add(dm2);
			
			q.Add(dm, Occur.SHOULD);
			
			BooleanQuery b = new BooleanQuery();
			b.SetMinimumNumberShouldMatch(2);
			b.Add(Snear("w1", "w2", 1, true), Occur.SHOULD);
			b.Add(Snear("w2", "w3", 1, true), Occur.SHOULD);
			b.Add(Snear("w1", "w3", 3, true), Occur.SHOULD);
			b.SetBoost(0.0f);
			
			q.Add(b, Occur.SHOULD);
			
			Qtest(q, new int[]{0, 1, 2});
		}
		
		// :TODO: we really need more crazy complex cases.
		
		
		// //////////////////////////////////////////////////////////////////
		
		// The rest of these aren't that complex, but they are <i>somewhat</i>
		// complex, and they expose weakness in dealing with queries that match
		// with scores of 0 wrapped in other queries
		
		[Test]
		public virtual void  TestT3()
		{
			Bqtest("w1^0.0", new int[]{0, 1, 2, 3});
		}
		
		[Test]
		public virtual void  TestMA3()
		{
			Query q = new MatchAllDocsQuery();
			q.SetBoost(0);
			Bqtest(q, new int[]{0, 1, 2, 3});
		}
		
		[Test]
		public virtual void  TestFQ5()
		{
			Bqtest(new FilteredQuery(qp.Parse("xx^0"), new ItemizedFilter(new int[]{1, 3})), new int[]{3});
		}
		
		[Test]
		public virtual void  TestCSQ4()
		{
			Query q = new ConstantScoreQuery(new ItemizedFilter(new int[]{3}));
			q.SetBoost(0);
			Bqtest(q, new int[]{3});
		}
		
		[Test]
		public virtual void  TestDMQ10()
		{
			DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
			q.Add(qp.Parse("yy w5^100"));
			q.Add(qp.Parse("xx^0"));
			q.SetBoost(0.0f);
			Bqtest(q, new int[]{0, 2, 3});
		}
		
		[Test]
		public virtual void  TestMPQ7()
		{
			MultiPhraseQuery q = new MultiPhraseQuery();
			q.Add(Ta(new System.String[]{"w1"}));
			q.Add(Ta(new System.String[]{"w2"}));
			q.SetSlop(1);
			q.SetBoost(0.0f);
			Bqtest(q, new int[]{0, 1, 2});
		}
		
		[Test]
		public virtual void  TestBQ12()
		{
			// NOTE: using qtest not bqtest
			Qtest("w1 w2^0.0", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ13()
		{
			// NOTE: using qtest not bqtest
			Qtest("w1 -w5^0.0", new int[]{1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ18()
		{
			// NOTE: using qtest not bqtest
			Qtest("+w1^0.0 w2", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ21()
		{
			Bqtest("(+w1 w2)^0.0", new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestBQ22()
		{
			Bqtest("(+w1^0.0 w2)^0.0", new int[]{0, 1, 2, 3});
		}
		
		[Test]
		public virtual void  TestST3()
		{
			SpanQuery q = St("w1");
			q.SetBoost(0);
			Bqtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestST6()
		{
			SpanQuery q = St("xx");
			q.SetBoost(0);
			Qtest(q, new int[]{2, 3});
		}
		
		[Test]
		public virtual void  TestSF3()
		{
			SpanQuery q = Sf(("w1"), 1);
			q.SetBoost(0);
			Bqtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestSF7()
		{
			SpanQuery q = Sf(("xx"), 3);
			q.SetBoost(0);
			Bqtest(q, new int[]{2, 3});
		}
		
		[Test]
		public virtual void  TestSNot3()
		{
			SpanQuery q = Snot(Sf("w1", 10), St("QQ"));
			q.SetBoost(0);
			Bqtest(q, new int[]{0, 1, 2, 3});
		}
		[Test]
		public virtual void  TestSNot6()
		{
			SpanQuery q = Snot(Sf("w1", 10), St("xx"));
			q.SetBoost(0);
			Bqtest(q, new int[]{0, 1, 2, 3});
		}
		
		[Test]
		public virtual void  TestSNot8()
		{
			// NOTE: using qtest not bqtest
			SpanQuery f = Snear("w1", "w3", 10, true);
			f.SetBoost(0);
			SpanQuery q = Snot(f, St("xx"));
			Qtest(q, new int[]{0, 1, 3});
		}
		[Test]
		public virtual void  TestSNot9()
		{
			// NOTE: using qtest not bqtest
			SpanQuery t = St("xx");
			t.SetBoost(0);
			SpanQuery q = Snot(Snear("w1", "w3", 10, true), t);
			Qtest(q, new int[]{0, 1, 3});
		}
	}
}