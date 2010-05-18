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

using KeywordAnalyzer = Lucene.Net.Analysis.KeywordAnalyzer;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using SetBasedFieldSelector = Lucene.Net.Documents.SetBasedFieldSelector;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> Tests {@link MultiSearcher} class.
	/// 
	/// </summary>
	/// <version>  $Id: TestMultiSearcher.java 781130 2009-06-02 19:16:20Z mikemccand $
	/// </version>
    [TestFixture]
	public class TestMultiSearcher:LuceneTestCase
	{
		[Serializable]
		private class AnonymousClassDefaultSimilarity:DefaultSimilarity
		{
			public AnonymousClassDefaultSimilarity(TestMultiSearcher enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestMultiSearcher enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestMultiSearcher enclosingInstance;
			public TestMultiSearcher Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			// overide all
			public override float Idf(int docFreq, int numDocs)
			{
				return 100.0f;
			}
			public override float Coord(int overlap, int maxOverlap)
			{
				return 1.0f;
			}
			public override float LengthNorm(System.String fieldName, int numTokens)
			{
				return 1.0f;
			}
			public override float QueryNorm(float sumOfSquaredWeights)
			{
				return 1.0f;
			}
			public override float SloppyFreq(int distance)
			{
				return 1.0f;
			}
			public override float Tf(float freq)
			{
				return 1.0f;
			}
		}
		
		/// <summary> ReturnS a new instance of the concrete MultiSearcher class
		/// used in this test.
		/// </summary>
		protected internal virtual MultiSearcher GetMultiSearcherInstance(Searcher[] searchers)
		{
			return new MultiSearcher(searchers);
		}
		
		[Test]
		public virtual void  TestEmptyIndex()
		{
			// creating two directories for indices
			Directory indexStoreA = new MockRAMDirectory();
			Directory indexStoreB = new MockRAMDirectory();
			
			// creating a document to store
			Document lDoc = new Document();
			lDoc.Add(new Field("fulltext", "Once upon a time.....", Field.Store.YES, Field.Index.ANALYZED));
			lDoc.Add(new Field("id", "doc1", Field.Store.YES, Field.Index.NOT_ANALYZED));
			lDoc.Add(new Field("handle", "1", Field.Store.YES, Field.Index.NOT_ANALYZED));
			
			// creating a document to store
			Document lDoc2 = new Document();
			lDoc2.Add(new Field("fulltext", "in a galaxy far far away.....", Field.Store.YES, Field.Index.ANALYZED));
			lDoc2.Add(new Field("id", "doc2", Field.Store.YES, Field.Index.NOT_ANALYZED));
			lDoc2.Add(new Field("handle", "1", Field.Store.YES, Field.Index.NOT_ANALYZED));
			
			// creating a document to store
			Document lDoc3 = new Document();
			lDoc3.Add(new Field("fulltext", "a bizarre bug manifested itself....", Field.Store.YES, Field.Index.ANALYZED));
			lDoc3.Add(new Field("id", "doc3", Field.Store.YES, Field.Index.NOT_ANALYZED));
			lDoc3.Add(new Field("handle", "1", Field.Store.YES, Field.Index.NOT_ANALYZED));
			
			// creating an index writer for the first index
			IndexWriter writerA = new IndexWriter(indexStoreA, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			// creating an index writer for the second index, but writing nothing
			IndexWriter writerB = new IndexWriter(indexStoreB, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			//--------------------------------------------------------------------
			// scenario 1
			//--------------------------------------------------------------------
			
			// writing the documents to the first index
			writerA.AddDocument(lDoc);
			writerA.AddDocument(lDoc2);
			writerA.AddDocument(lDoc3);
			writerA.Optimize();
			writerA.Close();
			
			// closing the second index
			writerB.Close();
			
			// creating the query
			QueryParser parser = new QueryParser("fulltext", new StandardAnalyzer());
			Query query = parser.Parse("handle:1");
			
			// building the searchables
			Searcher[] searchers = new Searcher[2];
			// VITAL STEP:adding the searcher for the empty index first, before the searcher for the populated index
			searchers[0] = new IndexSearcher(indexStoreB);
			searchers[1] = new IndexSearcher(indexStoreA);
			// creating the multiSearcher
			Searcher mSearcher = GetMultiSearcherInstance(searchers);
			// performing the search
			ScoreDoc[] hits = mSearcher.Search(query, null, 1000).scoreDocs;
			
			Assert.AreEqual(3, hits.Length);
			
			// iterating over the hit documents
			for (int i = 0; i < hits.Length; i++)
			{
				mSearcher.Doc(hits[i].doc);
			}
			mSearcher.Close();
			
			
			//--------------------------------------------------------------------
			// scenario 2
			//--------------------------------------------------------------------
			
			// adding one document to the empty index
			writerB = new IndexWriter(indexStoreB, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writerB.AddDocument(lDoc);
			writerB.Optimize();
			writerB.Close();
			
			// building the searchables
			Searcher[] searchers2 = new Searcher[2];
			// VITAL STEP:adding the searcher for the empty index first, before the searcher for the populated index
			searchers2[0] = new IndexSearcher(indexStoreB);
			searchers2[1] = new IndexSearcher(indexStoreA);
			// creating the mulitSearcher
			MultiSearcher mSearcher2 = GetMultiSearcherInstance(searchers2);
			// performing the same search
			ScoreDoc[] hits2 = mSearcher2.Search(query, null, 1000).scoreDocs;
			
			Assert.AreEqual(4, hits2.Length);
			
			// iterating over the hit documents
			for (int i = 0; i < hits2.Length; i++)
			{
				// no exception should happen at this point
				mSearcher2.Doc(hits2[i].doc);
			}
			
			// test the subSearcher() method:
			Query subSearcherQuery = parser.Parse("id:doc1");
			hits2 = mSearcher2.Search(subSearcherQuery, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits2.Length);
			Assert.AreEqual(0, mSearcher2.SubSearcher(hits2[0].doc)); // hit from searchers2[0]
			Assert.AreEqual(1, mSearcher2.SubSearcher(hits2[1].doc)); // hit from searchers2[1]
			subSearcherQuery = parser.Parse("id:doc2");
			hits2 = mSearcher2.Search(subSearcherQuery, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits2.Length);
			Assert.AreEqual(1, mSearcher2.SubSearcher(hits2[0].doc)); // hit from searchers2[1]
			mSearcher2.Close();
			
			//--------------------------------------------------------------------
			// scenario 3
			//--------------------------------------------------------------------
			
			// deleting the document just added, this will cause a different exception to take place
			Term term = new Term("id", "doc1");
			IndexReader readerB = IndexReader.Open(indexStoreB);
			readerB.DeleteDocuments(term);
			readerB.Close();
			
			// optimizing the index with the writer
			writerB = new IndexWriter(indexStoreB, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			writerB.Optimize();
			writerB.Close();
			
			// building the searchables
			Searcher[] searchers3 = new Searcher[2];
			
			searchers3[0] = new IndexSearcher(indexStoreB);
			searchers3[1] = new IndexSearcher(indexStoreA);
			// creating the mulitSearcher
			Searcher mSearcher3 = GetMultiSearcherInstance(searchers3);
			// performing the same search
			ScoreDoc[] hits3 = mSearcher3.Search(query, null, 1000).scoreDocs;
			
			Assert.AreEqual(3, hits3.Length);
			
			// iterating over the hit documents
			for (int i = 0; i < hits3.Length; i++)
			{
				mSearcher3.Doc(hits3[i].doc);
			}
			mSearcher3.Close();
			indexStoreA.Close();
			indexStoreB.Close();
		}
		
		private static Document CreateDocument(System.String contents1, System.String contents2)
		{
			Document document = new Document();
			
			document.Add(new Field("contents", contents1, Field.Store.YES, Field.Index.NOT_ANALYZED));
			document.Add(new Field("other", "other contents", Field.Store.YES, Field.Index.NOT_ANALYZED));
			if (contents2 != null)
			{
				document.Add(new Field("contents", contents2, Field.Store.YES, Field.Index.NOT_ANALYZED));
			}
			
			return document;
		}
		
		private static void  InitIndex(Directory directory, int nDocs, bool create, System.String contents2)
		{
			IndexWriter indexWriter = null;
			
			try
			{
				indexWriter = new IndexWriter(directory, new KeywordAnalyzer(), create, IndexWriter.MaxFieldLength.LIMITED);
				
				for (int i = 0; i < nDocs; i++)
				{
					indexWriter.AddDocument(CreateDocument("doc" + i, contents2));
				}
			}
			finally
			{
				if (indexWriter != null)
				{
					indexWriter.Close();
				}
			}
		}
		
		[Test]
		public virtual void  TestFieldSelector()
		{
			RAMDirectory ramDirectory1, ramDirectory2;
			IndexSearcher indexSearcher1, indexSearcher2;
			
			ramDirectory1 = new RAMDirectory();
			ramDirectory2 = new RAMDirectory();
			Query query = new TermQuery(new Term("contents", "doc0"));
			
			// Now put the documents in a different index
			InitIndex(ramDirectory1, 10, true, null); // documents with a single token "doc0", "doc1", etc...
			InitIndex(ramDirectory2, 10, true, "x"); // documents with two tokens "doc0" and "x", "doc1" and x, etc...
			
			indexSearcher1 = new IndexSearcher(ramDirectory1);
			indexSearcher2 = new IndexSearcher(ramDirectory2);
			
			MultiSearcher searcher = GetMultiSearcherInstance(new Searcher[]{indexSearcher1, indexSearcher2});
			Assert.IsTrue(searcher != null, "searcher is null and it shouldn't be");
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.IsTrue(hits != null, "hits is null and it shouldn't be");
			Assert.IsTrue(hits.Length == 2, hits.Length + " does not equal: " + 2);
			Document document = searcher.Doc(hits[0].doc);
			Assert.IsTrue(document != null, "document is null and it shouldn't be");
			Assert.IsTrue(document.GetFields().Count == 2, "document.getFields() Size: " + document.GetFields().Count + " is not: " + 2);
			//Should be one document from each directory
			//they both have two fields, contents and other
			System.Collections.Hashtable ftl = new System.Collections.Hashtable();
			SupportClass.CollectionsHelper.AddIfNotContains(ftl, "other");
			SetBasedFieldSelector fs = new SetBasedFieldSelector(ftl, (System.Collections.Hashtable) new System.Collections.Hashtable());
			document = searcher.Doc(hits[0].doc, fs);
			Assert.IsTrue(document != null, "document is null and it shouldn't be");
			Assert.IsTrue(document.GetFields().Count == 1, "document.getFields() Size: " + document.GetFields().Count + " is not: " + 1);
			System.String value_Renamed = document.Get("contents");
			Assert.IsTrue(value_Renamed == null, "value is not null and it should be");
			value_Renamed = document.Get("other");
			Assert.IsTrue(value_Renamed != null, "value is null and it shouldn't be");
			ftl.Clear();
			SupportClass.CollectionsHelper.AddIfNotContains(ftl, "contents");
			fs = new SetBasedFieldSelector(ftl, (System.Collections.Hashtable) new System.Collections.Hashtable());
			document = searcher.Doc(hits[1].doc, fs);
			value_Renamed = document.Get("contents");
			Assert.IsTrue(value_Renamed != null, "value is null and it shouldn't be");
			value_Renamed = document.Get("other");
			Assert.IsTrue(value_Renamed == null, "value is not null and it should be");
		}
		
		/* uncomment this when the highest score is always normalized to 1.0, even when it was < 1.0
		public void testNormalization1() throws IOException {
		testNormalization(1, "Using 1 document per index:");
		}
		*/
		
		[Test]
		public virtual void  TestNormalization10()
		{
			TestNormalization(10, "Using 10 documents per index:");
		}
		
		private void  TestNormalization(int nDocs, System.String message)
		{
			Query query = new TermQuery(new Term("contents", "doc0"));
			
			RAMDirectory ramDirectory1;
			IndexSearcher indexSearcher1;
			ScoreDoc[] hits;
			
			ramDirectory1 = new MockRAMDirectory();
			
			// First put the documents in the same index
			InitIndex(ramDirectory1, nDocs, true, null); // documents with a single token "doc0", "doc1", etc...
			InitIndex(ramDirectory1, nDocs, false, "x"); // documents with two tokens "doc0" and "x", "doc1" and x, etc...
			
			indexSearcher1 = new IndexSearcher(ramDirectory1);
			indexSearcher1.SetDefaultFieldSortScoring(true, true);
			
			hits = indexSearcher1.Search(query, null, 1000).scoreDocs;
			
			Assert.AreEqual(2, hits.Length, message);
			
			// Store the scores for use later
			float[] scores = new float[]{hits[0].score, hits[1].score};
			
			Assert.IsTrue(scores[0] > scores[1], message);
			
			indexSearcher1.Close();
			ramDirectory1.Close();
			hits = null;
			
			
			
			RAMDirectory ramDirectory2;
			IndexSearcher indexSearcher2;
			
			ramDirectory1 = new MockRAMDirectory();
			ramDirectory2 = new MockRAMDirectory();
			
			// Now put the documents in a different index
			InitIndex(ramDirectory1, nDocs, true, null); // documents with a single token "doc0", "doc1", etc...
			InitIndex(ramDirectory2, nDocs, true, "x"); // documents with two tokens "doc0" and "x", "doc1" and x, etc...
			
			indexSearcher1 = new IndexSearcher(ramDirectory1);
			indexSearcher1.SetDefaultFieldSortScoring(true, true);
			indexSearcher2 = new IndexSearcher(ramDirectory2);
			indexSearcher2.SetDefaultFieldSortScoring(true, true);
			
			Searcher searcher = GetMultiSearcherInstance(new Searcher[]{indexSearcher1, indexSearcher2});
			
			hits = searcher.Search(query, null, 1000).scoreDocs;
			
			Assert.AreEqual(2, hits.Length, message);
			
			// The scores should be the same (within reason)
			Assert.AreEqual(scores[0], hits[0].score, 1e-6, message); // This will a document from ramDirectory1
			Assert.AreEqual(scores[1], hits[1].score, 1e-6, message); // This will a document from ramDirectory2
			
			
			
			// Adding a Sort.RELEVANCE object should not change anything
			hits = searcher.Search(query, null, 1000, Sort.RELEVANCE).scoreDocs;
			
			Assert.AreEqual(2, hits.Length, message);
			
			Assert.AreEqual(scores[0], hits[0].score, 1e-6, message); // This will a document from ramDirectory1
			Assert.AreEqual(scores[1], hits[1].score, 1e-6, message); // This will a document from ramDirectory2
			
			searcher.Close();
			
			ramDirectory1.Close();
			ramDirectory2.Close();
		}
		
		/// <summary> test that custom similarity is in effect when using MultiSearcher (LUCENE-789).</summary>
		/// <throws>  IOException  </throws>
		[Test]
		public virtual void  TestCustomSimilarity()
		{
			RAMDirectory dir = new RAMDirectory();
			InitIndex(dir, 10, true, "x"); // documents with two tokens "doc0" and "x", "doc1" and x, etc...
			IndexSearcher srchr = new IndexSearcher(dir);
			MultiSearcher msrchr = GetMultiSearcherInstance(new Searcher[]{srchr});
			
			Similarity customSimilarity = new AnonymousClassDefaultSimilarity(this);
			
			srchr.SetSimilarity(customSimilarity);
			msrchr.SetSimilarity(customSimilarity);
			
			Query query = new TermQuery(new Term("contents", "doc0"));
			
			// Get a score from IndexSearcher
			TopDocs topDocs = srchr.Search(query, null, 1);
			float score1 = topDocs.GetMaxScore();
			
			// Get the score from MultiSearcher
			topDocs = msrchr.Search(query, null, 1);
			float scoreN = topDocs.GetMaxScore();
			
			// The scores from the IndexSearcher and Multisearcher should be the same
			// if the same similarity is used.
			Assert.AreEqual(score1, scoreN, 1e-6, "MultiSearcher score must be equal to single esrcher score!");
		}
	}
}