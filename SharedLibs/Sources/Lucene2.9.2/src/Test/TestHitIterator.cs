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
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using Hit = Lucene.Net.Search.Hit;
using HitIterator = Lucene.Net.Search.HitIterator;
using Hits = Lucene.Net.Search.Hits;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using TermQuery = Lucene.Net.Search.TermQuery;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;
namespace Lucene.Net
{
	
	/// <summary> This test intentionally not put in the search package in order
	/// to test HitIterator and Hit package protection.
	/// 
	/// </summary>
	/// <deprecated> Hits will be removed in Lucene 3.0 
	/// </deprecated>
	[TestFixture]
	public class TestHitIterator:LuceneTestCase
	{
		[Test]
		public virtual void  TestIterator()
		{
			RAMDirectory directory = new RAMDirectory();
			
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "iterator test doc 1", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			doc = new Document();
			doc.Add(new Field("field", "iterator test doc 2", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			writer.Close();
			
			_TestUtil.CheckIndex(directory);
			
			IndexSearcher searcher = new IndexSearcher(directory);
			Hits hits = searcher.Search(new TermQuery(new Term("field", "iterator")));
			
			HitIterator iterator = (HitIterator) hits.Iterator();
			Assert.AreEqual(2, iterator.Length());
			Assert.IsTrue(iterator.MoveNext());
			Hit hit = (Hit) iterator.Current;
			Assert.AreEqual("iterator test doc 1", hit.Get("field"));
			
			Assert.IsTrue(iterator.MoveNext());
			hit = (Hit) iterator.Current;
			Assert.AreEqual("iterator test doc 2", hit.GetDocument().Get("field"));
			
			Assert.IsFalse(iterator.MoveNext());
			
			bool caughtException = false;
			try
			{
				System.Object generatedAux = iterator.Current;
			}
			catch (System.ArgumentOutOfRangeException e)
			{
				Assert.IsTrue(true);
				caughtException = true;
			}
			
			Assert.IsTrue(caughtException);
		}
	}
}