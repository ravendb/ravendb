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
	
	/// <summary> Test Hits searches with interleaved deletions.
	/// 
	/// See {@link http://issues.apache.org/jira/browse/LUCENE-1096}.
	/// </summary>
	/// <deprecated> Hits will be removed in Lucene 3.0
	/// </deprecated>
    [TestFixture]
	public class TestSearchHitsWithDeletions:LuceneTestCase
	{
		
		private static bool VERBOSE = false;
		private const System.String TEXT_FIELD = "text";
		private const int N = 16100;
		
		private static Directory directory;
		
		[Test]
		public override void  SetUp()
		{
			base.SetUp();
			// Create an index writer.
			directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < N; i++)
			{
				writer.AddDocument(CreateDocument(i));
			}
			writer.Optimize();
			writer.Close();
		}
		
		/// <summary> Deletions during search should not alter previously retrieved hits.</summary>
		[Test]
		public virtual void  TestSearchHitsDeleteAll()
		{
			DoTestSearchHitsDeleteEvery(1, false);
		}
		
		/// <summary> Deletions during search should not alter previously retrieved hits.</summary>
		[Test]
		public virtual void  TestSearchHitsDeleteEvery2ndHit()
		{
			DoTestSearchHitsDeleteEvery(2, false);
		}
		
		/// <summary> Deletions during search should not alter previously retrieved hits.</summary>
		[Test]
		public virtual void  TestSearchHitsDeleteEvery4thHit()
		{
			DoTestSearchHitsDeleteEvery(4, false);
		}
		
		/// <summary> Deletions during search should not alter previously retrieved hits.</summary>
		[Test]
		public virtual void  TestSearchHitsDeleteEvery8thHit()
		{
			DoTestSearchHitsDeleteEvery(8, false);
		}
		
		/// <summary> Deletions during search should not alter previously retrieved hits.</summary>
		[Test]
		public virtual void  TestSearchHitsDeleteEvery90thHit()
		{
			DoTestSearchHitsDeleteEvery(90, false);
		}
		
		/// <summary> Deletions during search should not alter previously retrieved hits,
		/// and deletions that affect total number of hits should throw the 
		/// correct exception when trying to fetch "too many".
		/// </summary>
		[Test]
		public virtual void  TestSearchHitsDeleteEvery8thHitAndInAdvance()
		{
			DoTestSearchHitsDeleteEvery(8, true);
		}
		
		/// <summary> Verify that ok also with no deletions at all.</summary>
		[Test]
		public virtual void  TestSearchHitsNoDeletes()
		{
			DoTestSearchHitsDeleteEvery(N + 100, false);
		}
		
		/// <summary> Deletions that affect total number of hits should throw the 
		/// correct exception when trying to fetch "too many".
		/// </summary>
		[Test]
		public virtual void  TestSearchHitsDeleteInAdvance()
		{
			DoTestSearchHitsDeleteEvery(N + 100, true);
		}
		
		/// <summary> Intermittent deletions during search, should not alter previously retrieved hits.
		/// (Using a debugger to verify that the check in Hits is performed only  
		/// </summary>
		[Test]
		public virtual void  TestSearchHitsDeleteIntermittent()
		{
			DoTestSearchHitsDeleteEvery(- 1, false);
		}
		
		
		private void  DoTestSearchHitsDeleteEvery(int k, bool deleteInFront)
		{
			bool intermittent = k < 0;
			Log("Test search hits with " + (intermittent?"intermittent deletions.":"deletions of every " + k + " hit."));
			IndexSearcher searcher = new IndexSearcher(directory);
			IndexReader reader = searcher.GetIndexReader();
			Query q = new TermQuery(new Term(TEXT_FIELD, "text")); // matching all docs
			Hits hits = searcher.Search(q);
			Log("Got " + hits.Length() + " results");
			Assert.AreEqual(N, hits.Length(), "must match all " + N + " docs, not only " + hits.Length() + " docs!");
			if (deleteInFront)
			{
				Log("deleting hits that was not yet retrieved!");
				reader.DeleteDocument(reader.MaxDoc() - 1);
				reader.DeleteDocument(reader.MaxDoc() - 2);
				reader.DeleteDocument(reader.MaxDoc() - 3);
			}
			try
			{
				for (int i = 0; i < hits.Length(); i++)
				{
					int id = hits.Id(i);
					Assert.AreEqual(i, hits.Id(i), "Hit " + i + " has doc id " + hits.Id(i) + " instead of " + i);
					if ((intermittent && (i == 50 || i == 250 || i == 950)) || (!intermittent && (k < 2 || (i > 0 && i % k == 0))))
					{
						Document doc = hits.Doc(id);
						Log("Deleting hit " + i + " - doc " + doc + " with id " + id);
						reader.DeleteDocument(id);
					}
					if (intermittent)
					{
						// check internal behavior of Hits (go 50 ahead of getMoreDocs points because the deletions cause to use more of the available hits)
						if (i == 150 || i == 450 || i == 1650)
						{
							Assert.IsTrue(hits.debugCheckedForDeletions, "Hit " + i + ": hits should have checked for deletions in last call to getMoreDocs()");
						}
						else if (i == 50 || i == 250 || i == 850)
						{
							Assert.IsFalse(hits.debugCheckedForDeletions, "Hit " + i + ": hits should have NOT checked for deletions in last call to getMoreDocs()");
						}
					}
				}
			}
			catch (System.Exception e)
			{
				// this is the only valid exception, and only when deletng in front.
				Assert.IsTrue(deleteInFront, e.Message + " not expected unless deleting hits that were not yet seen!");
			}
			searcher.Close();
		}
		
		private static Document CreateDocument(int id)
		{
			Document doc = new Document();
			doc.Add(new Field(TEXT_FIELD, "text of document" + id, Field.Store.YES, Field.Index.ANALYZED));
			return doc;
		}
		
		private static void  Log(System.String s)
		{
			if (VERBOSE)
			{
				System.Console.Out.WriteLine(s);
			}
		}
	}
}