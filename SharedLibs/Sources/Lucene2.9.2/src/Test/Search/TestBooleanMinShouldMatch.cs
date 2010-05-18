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
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary>Test that BooleanQuery.setMinimumNumberShouldMatch works.</summary>
    [TestFixture]
	public class TestBooleanMinShouldMatch:LuceneTestCase
	{
		private class AnonymousClassCallback : TestBoolean2.Callback
		{
			public AnonymousClassCallback(System.Random rnd, TestBooleanMinShouldMatch enclosingInstance)
			{
				InitBlock(rnd, enclosingInstance);
			}
			private void  InitBlock(System.Random rnd, TestBooleanMinShouldMatch enclosingInstance)
			{
				this.rnd = rnd;
				this.enclosingInstance = enclosingInstance;
			}
			private System.Random rnd;
			private TestBooleanMinShouldMatch enclosingInstance;
			public TestBooleanMinShouldMatch Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public virtual void  PostCreate(BooleanQuery q)
			{
				BooleanClause[] c = q.GetClauses();
				int opt = 0;
				for (int i = 0; i < c.Length; i++)
				{
					if (c[i].GetOccur() == BooleanClause.Occur.SHOULD)
						opt++;
				}
				q.SetMinimumNumberShouldMatch(rnd.Next(opt + 2));
			}
		}
		
		
		public Directory index;
		public IndexReader r;
		public IndexSearcher s;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			
			
			System.String[] data = new System.String[]{"A 1 2 3 4 5 6", "Z       4 5 6", null, "B   2   4 5 6", "Y     3   5 6", null, "C     3     6", "X       4 5 6"};
			
			index = new RAMDirectory();
			IndexWriter writer = new IndexWriter(index, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			for (int i = 0; i < data.Length; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("id", System.Convert.ToString(i), Field.Store.YES, Field.Index.NOT_ANALYZED)); //Field.Keyword("id",String.valueOf(i)));
				doc.Add(new Field("all", "all", Field.Store.YES, Field.Index.NOT_ANALYZED)); //Field.Keyword("all","all"));
				if (null != data[i])
				{
					doc.Add(new Field("data", data[i], Field.Store.YES, Field.Index.ANALYZED)); //Field.Text("data",data[i]));
				}
				writer.AddDocument(doc);
			}
			
			writer.Optimize();
			writer.Close();
			
			r = IndexReader.Open(index);
			s = new IndexSearcher(r);
			
			//System.out.println("Set up " + getName());
		}
		
		public virtual void  VerifyNrHits(Query q, int expected)
		{
			ScoreDoc[] h = s.Search(q, null, 1000).scoreDocs;
			if (expected != h.Length)
			{
				PrintHits(Lucene.Net.TestCase.GetName(), h, s);  
			}
			Assert.AreEqual(expected, h.Length, "result count");
			QueryUtils.Check(q, s);
		}
		
		[Test]
		public virtual void  TestAllOptional()
		{
			
			BooleanQuery q = new BooleanQuery();
			for (int i = 1; i <= 4; i++)
			{
				q.Add(new TermQuery(new Term("data", "" + i)), BooleanClause.Occur.SHOULD); //false, false);
			}
			q.SetMinimumNumberShouldMatch(2); // match at least two of 4
			VerifyNrHits(q, 2);
		}
		
		[Test]
		public virtual void  TestOneReqAndSomeOptional()
		{
			
			/* one required, some optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("all", "all")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "5")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "4")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "3")), BooleanClause.Occur.SHOULD); //false, false);
			
			q.SetMinimumNumberShouldMatch(2); // 2 of 3 optional 
			
			VerifyNrHits(q, 5);
		}
		
		[Test]
		public virtual void  TestSomeReqAndSomeOptional()
		{
			
			/* two required, some optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("all", "all")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "6")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "5")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "4")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "3")), BooleanClause.Occur.SHOULD); //false, false);
			
			q.SetMinimumNumberShouldMatch(2); // 2 of 3 optional 
			
			VerifyNrHits(q, 5);
		}
		
		[Test]
		public virtual void  TestOneProhibAndSomeOptional()
		{
			
			/* one prohibited, some optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("data", "1")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "2")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "3")), BooleanClause.Occur.MUST_NOT); //false, true );
			q.Add(new TermQuery(new Term("data", "4")), BooleanClause.Occur.SHOULD); //false, false);
			
			q.SetMinimumNumberShouldMatch(2); // 2 of 3 optional 
			
			VerifyNrHits(q, 1);
		}
		
		[Test]
		public virtual void  TestSomeProhibAndSomeOptional()
		{
			
			/* two prohibited, some optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("data", "1")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "2")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "3")), BooleanClause.Occur.MUST_NOT); //false, true );
			q.Add(new TermQuery(new Term("data", "4")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "C")), BooleanClause.Occur.MUST_NOT); //false, true );
			
			q.SetMinimumNumberShouldMatch(2); // 2 of 3 optional 
			
			VerifyNrHits(q, 1);
		}
		
		[Test]
		public virtual void  TestOneReqOneProhibAndSomeOptional()
		{
			
			/* one required, one prohibited, some optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("data", "6")), BooleanClause.Occur.MUST); // true,  false);
			q.Add(new TermQuery(new Term("data", "5")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "4")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "3")), BooleanClause.Occur.MUST_NOT); //false, true );
			q.Add(new TermQuery(new Term("data", "2")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "1")), BooleanClause.Occur.SHOULD); //false, false);
			
			q.SetMinimumNumberShouldMatch(3); // 3 of 4 optional 
			
			VerifyNrHits(q, 1);
		}
		
		[Test]
		public virtual void  TestSomeReqOneProhibAndSomeOptional()
		{
			
			/* two required, one prohibited, some optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("all", "all")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "6")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "5")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "4")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "3")), BooleanClause.Occur.MUST_NOT); //false, true );
			q.Add(new TermQuery(new Term("data", "2")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "1")), BooleanClause.Occur.SHOULD); //false, false);
			
			q.SetMinimumNumberShouldMatch(3); // 3 of 4 optional 
			
			VerifyNrHits(q, 1);
		}
		
		[Test]
		public virtual void  TestOneReqSomeProhibAndSomeOptional()
		{
			
			/* one required, two prohibited, some optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("data", "6")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "5")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "4")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "3")), BooleanClause.Occur.MUST_NOT); //false, true );
			q.Add(new TermQuery(new Term("data", "2")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "1")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "C")), BooleanClause.Occur.MUST_NOT); //false, true );
			
			q.SetMinimumNumberShouldMatch(3); // 3 of 4 optional 
			
			VerifyNrHits(q, 1);
		}
		
		[Test]
		public virtual void  TestSomeReqSomeProhibAndSomeOptional()
		{
			
			/* two required, two prohibited, some optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("all", "all")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "6")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "5")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "4")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "3")), BooleanClause.Occur.MUST_NOT); //false, true );
			q.Add(new TermQuery(new Term("data", "2")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "1")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "C")), BooleanClause.Occur.MUST_NOT); //false, true );
			
			q.SetMinimumNumberShouldMatch(3); // 3 of 4 optional 
			
			VerifyNrHits(q, 1);
		}
		
		[Test]
		public virtual void  TestMinHigherThenNumOptional()
		{
			
			/* two required, two prohibited, some optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("all", "all")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "6")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "5")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "4")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "3")), BooleanClause.Occur.MUST_NOT); //false, true );
			q.Add(new TermQuery(new Term("data", "2")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "1")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "C")), BooleanClause.Occur.MUST_NOT); //false, true );
			
			q.SetMinimumNumberShouldMatch(90); // 90 of 4 optional ?!?!?!
			
			VerifyNrHits(q, 0);
		}
		
		[Test]
		public virtual void  TestMinEqualToNumOptional()
		{
			
			/* two required, two optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("all", "all")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "6")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "3")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "2")), BooleanClause.Occur.SHOULD); //false, false);
			
			q.SetMinimumNumberShouldMatch(2); // 2 of 2 optional 
			
			VerifyNrHits(q, 1);
		}
		
		[Test]
		public virtual void  TestOneOptionalEqualToMin()
		{
			
			/* two required, one optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("all", "all")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "3")), BooleanClause.Occur.SHOULD); //false, false);
			q.Add(new TermQuery(new Term("data", "2")), BooleanClause.Occur.MUST); //true,  false);
			
			q.SetMinimumNumberShouldMatch(1); // 1 of 1 optional 
			
			VerifyNrHits(q, 1);
		}
		
		[Test]
		public virtual void  TestNoOptionalButMin()
		{
			
			/* two required, no optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("all", "all")), BooleanClause.Occur.MUST); //true,  false);
			q.Add(new TermQuery(new Term("data", "2")), BooleanClause.Occur.MUST); //true,  false);
			
			q.SetMinimumNumberShouldMatch(1); // 1 of 0 optional 
			
			VerifyNrHits(q, 0);
		}
		
		[Test]
		public virtual void  TestNoOptionalButMin2()
		{
			
			/* one required, no optional */
			BooleanQuery q = new BooleanQuery();
			q.Add(new TermQuery(new Term("all", "all")), BooleanClause.Occur.MUST); //true,  false);
			
			q.SetMinimumNumberShouldMatch(1); // 1 of 0 optional 
			
			VerifyNrHits(q, 0);
		}
		
		[Test]
		public virtual void  TestRandomQueries()
		{
			System.Random rnd = NewRandom();
			
			System.String field = "data";
			System.String[] vals = new System.String[]{"1", "2", "3", "4", "5", "6", "A", "Z", "B", "Y", "Z", "X", "foo"};
			int maxLev = 4;
			
			// callback object to set a random setMinimumNumberShouldMatch
			TestBoolean2.Callback minNrCB = new AnonymousClassCallback(rnd, this);
			
			
			
			// increase number of iterations for more complete testing      
			for (int i = 0; i < 1000; i++)
			{
				int lev = rnd.Next(maxLev);
				long seed = rnd.Next(System.Int32.MaxValue);
				BooleanQuery q1 = TestBoolean2.RandBoolQuery(new System.Random((System.Int32) seed), lev, field, vals, null);
				// BooleanQuery q2 = TestBoolean2.randBoolQuery(new Random(seed), lev, field, vals, minNrCB);
				BooleanQuery q2 = TestBoolean2.RandBoolQuery(new System.Random((System.Int32) seed), lev, field, vals, null);
				// only set minimumNumberShouldMatch on the top level query since setting
				// at a lower level can change the score.
				minNrCB.PostCreate(q2);
				
				// Can't use Hits because normalized scores will mess things
				// up.  The non-sorting version of search() that returns TopDocs
				// will not normalize scores.
				TopDocs top1 = s.Search(q1, null, 100);
				TopDocs top2 = s.Search(q2, null, 100);
				
				QueryUtils.Check(q1, s);
				QueryUtils.Check(q2, s);
				
				// The constrained query
				// should be a superset to the unconstrained query.
				if (top2.totalHits > top1.totalHits)
				{
					//TestCase.fail("Constrained results not a subset:\n" + CheckHits.TopdocsString(top1, 0, 0) + CheckHits.TopdocsString(top2, 0, 0) + "for query:" + q2.ToString());
					Assert.Fail("Constrained results not a subset:\n" + CheckHits.TopdocsString(top1, 0, 0) + CheckHits.TopdocsString(top2, 0, 0) + "for query:" + q2.ToString());
				}
				
				for (int hit = 0; hit < top2.totalHits; hit++)
				{
					int id = top2.scoreDocs[hit].doc;
					float score = top2.scoreDocs[hit].score;
					bool found = false;
					// find this doc in other hits
					for (int other = 0; other < top1.totalHits; other++)
					{
						if (top1.scoreDocs[other].doc == id)
						{
							found = true;
							float otherScore = top1.scoreDocs[other].score;
							// check if scores match
							if (System.Math.Abs(otherScore - score) > 1.0e-6f)
							{
								//TestCase.fail("Doc " + id + " scores don't match\n" + CheckHits.TopdocsString(top1, 0, 0) + CheckHits.TopdocsString(top2, 0, 0) + "for query:" + q2.ToString());
								Assert.Fail("Doc " + id + " scores don't match\n" + CheckHits.TopdocsString(top1, 0, 0) + CheckHits.TopdocsString(top2, 0, 0) + "for query:" + q2.ToString());
							}
						}
					}
					
					// check if subset
					if (!found)
					{
						//TestCase.fail("Doc " + id + " not found\n" + CheckHits.TopdocsString(top1, 0, 0) + CheckHits.TopdocsString(top2, 0, 0) + "for query:" + q2.ToString());
						Assert.Fail("Doc " + id + " not found\n" + CheckHits.TopdocsString(top1, 0, 0) + CheckHits.TopdocsString(top2, 0, 0) + "for query:" + q2.ToString());
					}
				}
			}
			// System.out.println("Total hits:"+tot);
		}
		
		
		
		protected internal virtual void  PrintHits(System.String test, ScoreDoc[] h, Searcher searcher)
		{
			
			System.Console.Error.WriteLine("------- " + test + " -------");
			
			for (int i = 0; i < h.Length; i++)
			{
				Document d = searcher.Doc(h[i].doc);
				float score = h[i].score;
				System.Console.Error.WriteLine("#" + i + ": {0.000000}" + score + " - " + d.Get("id") + " - " + d.Get("data"));
			}
		}
	}
}