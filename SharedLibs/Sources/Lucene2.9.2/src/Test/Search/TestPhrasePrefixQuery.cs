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

using SimpleAnalyzer = Lucene.Net.Analysis.SimpleAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using TermEnum = Lucene.Net.Index.TermEnum;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> This class tests PhrasePrefixQuery class.
	/// 
	/// 
	/// </summary>
	/// <version>  $Id: TestPhrasePrefixQuery.java 694004 2008-09-10 21:38:52Z mikemccand $
	/// </version>
    [TestFixture]
	public class TestPhrasePrefixQuery:LuceneTestCase
	{
		/*public TestPhrasePrefixQuery(System.String name):base(name)
		{
		}*/
		
		/// <summary> </summary>
		[Test]
		public virtual void  TestPhrasePrefix()
		{
			RAMDirectory indexStore = new RAMDirectory();
			IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Document doc1 = new Document();
			Document doc2 = new Document();
			Document doc3 = new Document();
			Document doc4 = new Document();
			Document doc5 = new Document();
			doc1.Add(new Field("body", "blueberry pie", Field.Store.YES, Field.Index.ANALYZED));
			doc2.Add(new Field("body", "blueberry strudel", Field.Store.YES, Field.Index.ANALYZED));
			doc3.Add(new Field("body", "blueberry pizza", Field.Store.YES, Field.Index.ANALYZED));
			doc4.Add(new Field("body", "blueberry chewing gum", Field.Store.YES, Field.Index.ANALYZED));
			doc5.Add(new Field("body", "piccadilly circus", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc1);
			writer.AddDocument(doc2);
			writer.AddDocument(doc3);
			writer.AddDocument(doc4);
			writer.AddDocument(doc5);
			writer.Optimize();
			writer.Close();
			
			IndexSearcher searcher = new IndexSearcher(indexStore);
			
			//PhrasePrefixQuery query1 = new PhrasePrefixQuery();
			MultiPhraseQuery query1 = new MultiPhraseQuery();
			//PhrasePrefixQuery query2 = new PhrasePrefixQuery();
			MultiPhraseQuery query2 = new MultiPhraseQuery();
			query1.Add(new Term("body", "blueberry"));
			query2.Add(new Term("body", "strawberry"));
			
			System.Collections.ArrayList termsWithPrefix = new System.Collections.ArrayList();
			IndexReader ir = IndexReader.Open(indexStore);
			
			// this TermEnum gives "piccadilly", "pie" and "pizza".
			System.String prefix = "pi";
			TermEnum te = ir.Terms(new Term("body", prefix + "*"));
			do 
			{
				if (te.Term().Text().StartsWith(prefix))
				{
					termsWithPrefix.Add(te.Term());
				}
			}
			while (te.Next());
			
			query1.Add((Term[]) termsWithPrefix.ToArray(typeof(Term)));
			query2.Add((Term[]) termsWithPrefix.ToArray(typeof(Term)));
			
			ScoreDoc[] result;
			result = searcher.Search(query1, null, 1000).scoreDocs;
			Assert.AreEqual(2, result.Length);
			
			result = searcher.Search(query2, null, 1000).scoreDocs;
			Assert.AreEqual(0, result.Length);
		}
	}
}