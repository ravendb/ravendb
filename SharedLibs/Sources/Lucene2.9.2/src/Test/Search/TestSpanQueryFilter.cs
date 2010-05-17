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
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using SpanTermQuery = Lucene.Net.Search.Spans.SpanTermQuery;
using English = Lucene.Net.Util.English;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestSpanQueryFilter:LuceneTestCase
	{
				
		[Test]
		public virtual void  TestFilterWorks()
		{
			Directory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 500; i++)
			{
				Document document = new Document();
				document.Add(new Field("field", English.IntToEnglish(i) + " equals " + English.IntToEnglish(i), Field.Store.NO, Field.Index.ANALYZED));
				writer.AddDocument(document);
			}
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			
			SpanTermQuery query = new SpanTermQuery(new Term("field", English.IntToEnglish(10).Trim()));
			SpanQueryFilter filter = new SpanQueryFilter(query);
			SpanFilterResult result = filter.BitSpans(reader);
			DocIdSet docIdSet = result.GetDocIdSet();
			Assert.IsTrue(docIdSet != null, "docIdSet is null and it shouldn't be");
			AssertContainsDocId("docIdSet doesn't contain docId 10", docIdSet, 10);
			System.Collections.IList spans = result.GetPositions();
			Assert.IsTrue(spans != null, "spans is null and it shouldn't be");
			int size = GetDocIdSetSize(docIdSet);
			Assert.IsTrue(spans.Count == size, "spans Size: " + spans.Count + " is not: " + size);
			for (System.Collections.IEnumerator iterator = spans.GetEnumerator(); iterator.MoveNext(); )
			{
				SpanFilterResult.PositionInfo info = (SpanFilterResult.PositionInfo) iterator.Current;
				Assert.IsTrue(info != null, "info is null and it shouldn't be");
				//The doc should indicate the bit is on
				AssertContainsDocId("docIdSet doesn't contain docId " + info.GetDoc(), docIdSet, info.GetDoc());
				//There should be two positions in each
				Assert.IsTrue(info.GetPositions().Count == 2, "info.getPositions() Size: " + info.GetPositions().Count + " is not: " + 2);
			}
			reader.Close();
		}
		
		internal virtual int GetDocIdSetSize(DocIdSet docIdSet)
		{
			int size = 0;
			DocIdSetIterator it = docIdSet.Iterator();
			while (it.NextDoc() != DocIdSetIterator.NO_MORE_DOCS)
			{
				size++;
			}
			return size;
		}
		
		public virtual void  AssertContainsDocId(System.String msg, DocIdSet docIdSet, int docId)
		{
			DocIdSetIterator it = docIdSet.Iterator();
			Assert.IsTrue(it.Advance(docId) != DocIdSetIterator.NO_MORE_DOCS, msg);
			Assert.IsTrue(it.DocID() == docId, msg);
		}
	}
}