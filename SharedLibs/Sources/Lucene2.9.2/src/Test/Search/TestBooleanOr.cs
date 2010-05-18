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

using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestBooleanOr:LuceneTestCase
	{
		
		private static System.String FIELD_T = "T";
		private static System.String FIELD_C = "C";
		
		private TermQuery t1 = new TermQuery(new Term(FIELD_T, "files"));
		private TermQuery t2 = new TermQuery(new Term(FIELD_T, "deleting"));
		private TermQuery c1 = new TermQuery(new Term(FIELD_C, "production"));
		private TermQuery c2 = new TermQuery(new Term(FIELD_C, "optimize"));
		
		private IndexSearcher searcher = null;
		
		private int Search(Query q)
		{
			QueryUtils.Check(q, searcher);
			return searcher.Search(q, null, 1000).totalHits;
		}
		
		[Test]
		public virtual void  TestElements()
		{
			Assert.AreEqual(1, Search(t1));
			Assert.AreEqual(1, Search(t2));
			Assert.AreEqual(1, Search(c1));
			Assert.AreEqual(1, Search(c2));
		}
		
		/// <summary> <code>T:files T:deleting C:production C:optimize </code>
		/// it works.
		/// 
		/// </summary>
		/// <throws>  IOException </throws>
		[Test]
		public virtual void  TestFlat()
		{
			BooleanQuery q = new BooleanQuery();
			q.Add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
			q.Add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
			q.Add(new BooleanClause(c1, BooleanClause.Occur.SHOULD));
			q.Add(new BooleanClause(c2, BooleanClause.Occur.SHOULD));
			Assert.AreEqual(1, Search(q));
		}
		
		/// <summary> <code>(T:files T:deleting) (+C:production +C:optimize)</code>
		/// it works.
		/// 
		/// </summary>
		/// <throws>  IOException </throws>
		[Test]
		public virtual void  TestParenthesisMust()
		{
			BooleanQuery q3 = new BooleanQuery();
			q3.Add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
			q3.Add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
			BooleanQuery q4 = new BooleanQuery();
			q4.Add(new BooleanClause(c1, BooleanClause.Occur.MUST));
			q4.Add(new BooleanClause(c2, BooleanClause.Occur.MUST));
			BooleanQuery q2 = new BooleanQuery();
			q2.Add(q3, BooleanClause.Occur.SHOULD);
			q2.Add(q4, BooleanClause.Occur.SHOULD);
			Assert.AreEqual(1, Search(q2));
		}
		
		/// <summary> <code>(T:files T:deleting) +(C:production C:optimize)</code>
		/// not working. results NO HIT.
		/// 
		/// </summary>
		/// <throws>  IOException </throws>
		[Test]
		public virtual void  TestParenthesisMust2()
		{
			BooleanQuery q3 = new BooleanQuery();
			q3.Add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
			q3.Add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
			BooleanQuery q4 = new BooleanQuery();
			q4.Add(new BooleanClause(c1, BooleanClause.Occur.SHOULD));
			q4.Add(new BooleanClause(c2, BooleanClause.Occur.SHOULD));
			BooleanQuery q2 = new BooleanQuery();
			q2.Add(q3, BooleanClause.Occur.SHOULD);
			q2.Add(q4, BooleanClause.Occur.MUST);
			Assert.AreEqual(1, Search(q2));
		}
		
		/// <summary> <code>(T:files T:deleting) (C:production C:optimize)</code>
		/// not working. results NO HIT.
		/// 
		/// </summary>
		/// <throws>  IOException </throws>
		[Test]
		public virtual void  TestParenthesisShould()
		{
			BooleanQuery q3 = new BooleanQuery();
			q3.Add(new BooleanClause(t1, BooleanClause.Occur.SHOULD));
			q3.Add(new BooleanClause(t2, BooleanClause.Occur.SHOULD));
			BooleanQuery q4 = new BooleanQuery();
			q4.Add(new BooleanClause(c1, BooleanClause.Occur.SHOULD));
			q4.Add(new BooleanClause(c2, BooleanClause.Occur.SHOULD));
			BooleanQuery q2 = new BooleanQuery();
			q2.Add(q3, BooleanClause.Occur.SHOULD);
			q2.Add(q4, BooleanClause.Occur.SHOULD);
			Assert.AreEqual(1, Search(q2));
		}
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			base.SetUp();
			
			//
			RAMDirectory rd = new RAMDirectory();
			
			//
			IndexWriter writer = new IndexWriter(rd, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			//
			Document d = new Document();
			d.Add(new Field(FIELD_T, "Optimize not deleting all files", Field.Store.YES, Field.Index.ANALYZED));
			d.Add(new Field(FIELD_C, "Deleted When I run an optimize in our production environment.", Field.Store.YES, Field.Index.ANALYZED));
			
			//
			writer.AddDocument(d);
			writer.Close();
			
			//
			searcher = new IndexSearcher(rd);
		}
	}
}