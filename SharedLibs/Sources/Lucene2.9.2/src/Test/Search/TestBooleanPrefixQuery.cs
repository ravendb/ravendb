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
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> </summary>
	/// <version>  $Id: TestBooleanPrefixQuery.java 808519 2009-08-27 16:57:27Z mikemccand $
	/// 
	/// </version>
	
    [TestFixture]
	public class TestBooleanPrefixQuery:LuceneTestCase
	{
		
		/*[STAThread]
		public static void  Main(System.String[] args)
		{
			// TestRunner.run(suite()); // {{Aroush-2.9}} how is this done in NUnit?
		}*/
		
		/*public static Test suite()
		{
			return new TestSuite(typeof(TestBooleanPrefixQuery));
		}*/
		
		/*public TestBooleanPrefixQuery(System.String name):base(name)
		{
		}*/
		
		private int GetCount(IndexReader r, Query q)
		{
			if (q is BooleanQuery)
			{
				return ((BooleanQuery) q).GetClauses().Length;
			}
			else if (q is ConstantScoreQuery)
			{
				DocIdSetIterator iter = ((ConstantScoreQuery) q).GetFilter().GetDocIdSet(r).Iterator();
				int count = 0;
				while (iter.NextDoc() != DocIdSetIterator.NO_MORE_DOCS)
				{
					count++;
				}
				return count;
			}
			else
			{
				throw new System.SystemException("unepxected query " + q);
			}
		}
		
		[Test]
		public virtual void  TestMethod()
		{
			RAMDirectory directory = new RAMDirectory();
			
			System.String[] categories = new System.String[]{"food", "foodanddrink", "foodanddrinkandgoodtimes", "food and drink"};
			
			Query rw1 = null;
			Query rw2 = null;
			IndexReader reader = null;
			try
			{
				IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				for (int i = 0; i < categories.Length; i++)
				{
					Document doc = new Document();
					doc.Add(new Field("category", categories[i], Field.Store.YES, Field.Index.NOT_ANALYZED));
					writer.AddDocument(doc);
				}
				writer.Close();
				
				reader = IndexReader.Open(directory);
				PrefixQuery query = new PrefixQuery(new Term("category", "foo"));
				rw1 = query.Rewrite(reader);
				
				BooleanQuery bq = new BooleanQuery();
				bq.Add(query, BooleanClause.Occur.MUST);
				
				rw2 = bq.Rewrite(reader);
			}
			catch (System.IO.IOException e)
			{
				Assert.Fail(e.Message);
			}
			
			Assert.AreEqual(GetCount(reader, rw1), GetCount(reader, rw2), "Number of Clauses Mismatch");
		}
	}
}