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
using Index = Lucene.Net.Documents.Field.Index;
using Store = Lucene.Net.Documents.Field.Store;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using MaxFieldLength = Lucene.Net.Index.IndexWriter.MaxFieldLength;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestDocIdSet:LuceneTestCase
	{
		private class AnonymousClassDocIdSet_Renamed_Class:DocIdSet
		{
			public AnonymousClassDocIdSet_Renamed_Class(int maxdoc, TestDocIdSet enclosingInstance)
			{
				InitBlock(maxdoc, enclosingInstance);
			}
			private class AnonymousClassDocIdSetIterator:DocIdSetIterator
			{
				public AnonymousClassDocIdSetIterator(int maxdoc, AnonymousClassDocIdSet_Renamed_Class enclosingInstance)
				{
					InitBlock(maxdoc, enclosingInstance);
				}
				private void  InitBlock(int maxdoc, AnonymousClassDocIdSet_Renamed_Class enclosingInstance)
				{
					this.maxdoc = maxdoc;
					this.enclosingInstance = enclosingInstance;
				}
				private int maxdoc;
				private AnonymousClassDocIdSet_Renamed_Class enclosingInstance;
				public AnonymousClassDocIdSet_Renamed_Class Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				
				internal int docid = - 1;
				
				/** @deprecated use {@link #DocID()} instead. */
				public override int Doc()
				{
					return docid;
				}
				
				public override int DocID()
				{
					return docid;
				}
				
				/// <deprecated> use {@link #NextDoc()} instead. 
				/// </deprecated>
                [Obsolete("use NextDoc() instead. ")]
				public override bool Next()
				{
					return NextDoc() != NO_MORE_DOCS;
				}
				
				//@Override
				public override int NextDoc()
				{
					docid++;
					return docid < maxdoc?docid:(docid = NO_MORE_DOCS);
				}
				
				/// <deprecated> use {@link #Advance(int)} instead. 
				/// </deprecated>
                [Obsolete("use Advance(int) instead. ")]
				public override bool SkipTo(int target)
				{
					return Advance(target) != NO_MORE_DOCS;
				}
				
				//@Override
				public override int Advance(int target)
				{
					while (NextDoc() < target)
					{
					}
					return docid;
				}
			}
			private void  InitBlock(int maxdoc, TestDocIdSet enclosingInstance)
			{
				this.maxdoc = maxdoc;
				this.enclosingInstance = enclosingInstance;
			}
			private int maxdoc;
			private TestDocIdSet enclosingInstance;
			public TestDocIdSet Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			// @Override
			public override DocIdSetIterator Iterator()
			{
				return new AnonymousClassDocIdSetIterator(maxdoc, this);
			}
		}
		private class AnonymousClassFilteredDocIdSet:FilteredDocIdSet
		{
			private void  InitBlock(TestDocIdSet enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestDocIdSet enclosingInstance;
			public TestDocIdSet Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassFilteredDocIdSet(TestDocIdSet enclosingInstance, Lucene.Net.Search.DocIdSet Param1):base(Param1)
			{
				InitBlock(enclosingInstance);
			}
			// @Override
			public /*protected internal*/ override bool Match(int docid)
			{
				return docid % 2 == 0; //validate only even docids
			}
		}
		[Serializable]
		private class AnonymousClassFilter:Filter
		{
			public AnonymousClassFilter(TestDocIdSet enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestDocIdSet enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestDocIdSet enclosingInstance;
			public TestDocIdSet Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override DocIdSet GetDocIdSet(IndexReader reader)
			{
				return null;
			}
		}
        [Test]
		public virtual void  TestFilteredDocIdSet()
		{
			int maxdoc = 10;
			DocIdSet innerSet = new AnonymousClassDocIdSet_Renamed_Class(maxdoc, this);
			
			
			DocIdSet filteredSet = new AnonymousClassFilteredDocIdSet(this, innerSet);
			
			DocIdSetIterator iter = filteredSet.Iterator();
			System.Collections.ArrayList list = new System.Collections.ArrayList();
			int doc = iter.Advance(3);
			if (doc != DocIdSetIterator.NO_MORE_DOCS)
			{
				list.Add((System.Int32) doc);
				while ((doc = iter.NextDoc()) != DocIdSetIterator.NO_MORE_DOCS)
				{
					list.Add((System.Int32) doc);
				}
			}
			
			int[] docs = new int[list.Count];
			int c = 0;
			System.Collections.IEnumerator intIter = list.GetEnumerator();
			while (intIter.MoveNext())
			{
				docs[c++] = ((System.Int32) intIter.Current);
			}
			int[] answer = new int[]{4, 6, 8};
			bool same = SupportClass.CollectionsHelper.Equals(answer, docs);
			if (!same)
			{
				System.Console.Out.WriteLine("answer: " + _TestUtil.ArrayToString(answer));
				System.Console.Out.WriteLine("gotten: " + _TestUtil.ArrayToString(docs));
				Assert.Fail();
			}
		}
		
        [Test]
		public virtual void  TestNullDocIdSet()
		{
			// Tests that if a Filter produces a null DocIdSet, which is given to
			// IndexSearcher, everything works fine. This came up in LUCENE-1754.
			Directory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			doc.Add(new Field("c", "val", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
			writer.AddDocument(doc);
			writer.Close();
			
			// First verify the document is searchable.
			IndexSearcher searcher = new IndexSearcher(dir, true);
			Assert.AreEqual(1, searcher.Search(new MatchAllDocsQuery(), 10).totalHits);
			
			// Now search w/ a Filter which returns a null DocIdSet
			Filter f = new AnonymousClassFilter(this);
			
			Assert.AreEqual(0, searcher.Search(new MatchAllDocsQuery(), f, 10).totalHits);
			searcher.Close();
		}
	}
}