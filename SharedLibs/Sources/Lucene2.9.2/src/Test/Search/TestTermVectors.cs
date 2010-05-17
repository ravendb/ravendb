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
using Lucene.Net.Index;
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using English = Lucene.Net.Util.English;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestTermVectors:LuceneTestCase
	{
		private IndexSearcher searcher;
		private Directory directory = new MockRAMDirectory();
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			IndexWriter writer = new IndexWriter(directory, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			//writer.setUseCompoundFile(true);
			//writer.infoStream = System.out;
			for (int i = 0; i < 1000; i++)
			{
				Document doc = new Document();
				Field.TermVector termVector;
				int mod3 = i % 3;
				int mod2 = i % 2;
				if (mod2 == 0 && mod3 == 0)
				{
					termVector = Field.TermVector.WITH_POSITIONS_OFFSETS;
				}
				else if (mod2 == 0)
				{
					termVector = Field.TermVector.WITH_POSITIONS;
				}
				else if (mod3 == 0)
				{
					termVector = Field.TermVector.WITH_OFFSETS;
				}
				else
				{
					termVector = Field.TermVector.YES;
				}
				doc.Add(new Field("field", English.IntToEnglish(i), Field.Store.YES, Field.Index.ANALYZED, termVector));
				writer.AddDocument(doc);
			}
			writer.Close();
			searcher = new IndexSearcher(directory);
		}
		
		[Test]
		public virtual void  Test()
		{
			Assert.IsTrue(searcher != null);
		}
		
		[Test]
		public virtual void  TestTermVectors_Renamed()
		{
			Query query = new TermQuery(new Term("field", "seventy"));
			try
			{
				ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
				Assert.AreEqual(100, hits.Length);
				
				for (int i = 0; i < hits.Length; i++)
				{
					TermFreqVector[] vector = searcher.reader_ForNUnit.GetTermFreqVectors(hits[i].doc);
					Assert.IsTrue(vector != null);
					Assert.IsTrue(vector.Length == 1);
				}
			}
			catch (System.IO.IOException e)
			{
				Assert.IsTrue(false);
			}
		}
		
		[Test]
		public virtual void  TestTermVectorsFieldOrder()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("c", "some content here", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			doc.Add(new Field("a", "some content here", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			doc.Add(new Field("b", "some content here", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			doc.Add(new Field("x", "some content here", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			writer.AddDocument(doc);
			writer.Close();
			IndexReader reader = IndexReader.Open(dir);
			TermFreqVector[] v = reader.GetTermFreqVectors(0);
			Assert.AreEqual(4, v.Length);
			System.String[] expectedFields = new System.String[]{"a", "b", "c", "x"};
			int[] expectedPositions = new int[]{1, 2, 0};
			for (int i = 0; i < v.Length; i++)
			{
				TermPositionVector posVec = (TermPositionVector) v[i];
				Assert.AreEqual(expectedFields[i], posVec.GetField());
				System.String[] terms = posVec.GetTerms();
				Assert.AreEqual(3, terms.Length);
				Assert.AreEqual("content", terms[0]);
				Assert.AreEqual("here", terms[1]);
				Assert.AreEqual("some", terms[2]);
				for (int j = 0; j < 3; j++)
				{
					int[] positions = posVec.GetTermPositions(j);
					Assert.AreEqual(1, positions.Length);
					Assert.AreEqual(expectedPositions[j], positions[0]);
				}
			}
		}
		
		[Test]
		public virtual void  TestTermPositionVectors()
		{
			Query query = new TermQuery(new Term("field", "zero"));
			try
			{
				ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
				Assert.AreEqual(1, hits.Length);
				
				for (int i = 0; i < hits.Length; i++)
				{
					TermFreqVector[] vector = searcher.reader_ForNUnit.GetTermFreqVectors(hits[i].doc);
					Assert.IsTrue(vector != null);
					Assert.IsTrue(vector.Length == 1);
					
					bool shouldBePosVector = (hits[i].doc % 2 == 0)?true:false;
					Assert.IsTrue((shouldBePosVector == false) || (shouldBePosVector == true && (vector[0] is TermPositionVector == true)));
					
					bool shouldBeOffVector = (hits[i].doc % 3 == 0)?true:false;
					Assert.IsTrue((shouldBeOffVector == false) || (shouldBeOffVector == true && (vector[0] is TermPositionVector == true)));
					
					if (shouldBePosVector || shouldBeOffVector)
					{
						TermPositionVector posVec = (TermPositionVector) vector[0];
						System.String[] terms = posVec.GetTerms();
						Assert.IsTrue(terms != null && terms.Length > 0);
						
						for (int j = 0; j < terms.Length; j++)
						{
							int[] positions = posVec.GetTermPositions(j);
							TermVectorOffsetInfo[] offsets = posVec.GetOffsets(j);
							
							if (shouldBePosVector)
							{
								Assert.IsTrue(positions != null);
								Assert.IsTrue(positions.Length > 0);
							}
							else
								Assert.IsTrue(positions == null);
							
							if (shouldBeOffVector)
							{
								Assert.IsTrue(offsets != null);
								Assert.IsTrue(offsets.Length > 0);
							}
							else
								Assert.IsTrue(offsets == null);
						}
					}
					else
					{
						try
						{
							TermPositionVector posVec = (TermPositionVector) vector[0];
							Assert.IsTrue(false);
						}
						catch (System.InvalidCastException ignore)
						{
							TermFreqVector freqVec = vector[0];
							System.String[] terms = freqVec.GetTerms();
							Assert.IsTrue(terms != null && terms.Length > 0);
						}
					}
				}
			}
			catch (System.IO.IOException e)
			{
				Assert.IsTrue(false);
			}
		}
		
		[Test]
		public virtual void  TestTermOffsetVectors()
		{
			Query query = new TermQuery(new Term("field", "fifty"));
			try
			{
				ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
				Assert.AreEqual(100, hits.Length);
				
				for (int i = 0; i < hits.Length; i++)
				{
					TermFreqVector[] vector = searcher.reader_ForNUnit.GetTermFreqVectors(hits[i].doc);
					Assert.IsTrue(vector != null);
					Assert.IsTrue(vector.Length == 1);
					
					//Assert.IsTrue();
				}
			}
			catch (System.IO.IOException e)
			{
				Assert.IsTrue(false);
			}
		}
		
		[Test]
		public virtual void  TestKnownSetOfDocuments()
		{
			System.String test1 = "eating chocolate in a computer lab"; //6 terms
			System.String test2 = "computer in a computer lab"; //5 terms
			System.String test3 = "a chocolate lab grows old"; //5 terms
			System.String test4 = "eating chocolate with a chocolate lab in an old chocolate colored computer lab"; //13 terms
			System.Collections.IDictionary test4Map = new System.Collections.Hashtable();
			test4Map["chocolate"] = 3;
			test4Map["lab"] = 2;
			test4Map["eating"] = 1;
			test4Map["computer"] = 1;
			test4Map["with"] = 1;
			test4Map["a"] = 1;
			test4Map["colored"] = 1;
			test4Map["in"] = 1;
			test4Map["an"] = 1;
			test4Map["computer"] = 1;
			test4Map["old"] = 1;
			
			Document testDoc1 = new Document();
			SetupDoc(testDoc1, test1);
			Document testDoc2 = new Document();
			SetupDoc(testDoc2, test2);
			Document testDoc3 = new Document();
			SetupDoc(testDoc3, test3);
			Document testDoc4 = new Document();
			SetupDoc(testDoc4, test4);
			
			Directory dir = new MockRAMDirectory();
			
			try
			{
				IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
				Assert.IsTrue(writer != null);
				writer.AddDocument(testDoc1);
				writer.AddDocument(testDoc2);
				writer.AddDocument(testDoc3);
				writer.AddDocument(testDoc4);
				writer.Close();
				IndexSearcher knownSearcher = new IndexSearcher(dir);
				TermEnum termEnum = knownSearcher.reader_ForNUnit.Terms();
				TermDocs termDocs = knownSearcher.reader_ForNUnit.TermDocs();
				//System.out.println("Terms: " + termEnum.size() + " Orig Len: " + termArray.length);
				
				Similarity sim = knownSearcher.GetSimilarity();
				while (termEnum.Next() == true)
				{
					Term term = termEnum.Term();
					//System.out.println("Term: " + term);
					termDocs.Seek(term);
					while (termDocs.Next())
					{
						int docId = termDocs.Doc();
						int freq = termDocs.Freq();
						//System.out.println("Doc Id: " + docId + " freq " + freq);
						TermFreqVector vector = knownSearcher.reader_ForNUnit.GetTermFreqVector(docId, "field");
						float tf = sim.Tf(freq);
						float idf = sim.Idf(term, knownSearcher);
						//float qNorm = sim.queryNorm()
						//This is fine since we don't have stop words
						float lNorm = sim.LengthNorm("field", vector.GetTerms().Length);
						//float coord = sim.coord()
						//System.out.println("TF: " + tf + " IDF: " + idf + " LenNorm: " + lNorm);
						Assert.IsTrue(vector != null);
						System.String[] vTerms = vector.GetTerms();
						int[] freqs = vector.GetTermFrequencies();
						for (int i = 0; i < vTerms.Length; i++)
						{
							if (term.Text().Equals(vTerms[i]))
							{
								Assert.IsTrue(freqs[i] == freq);
							}
						}
					}
					//System.out.println("--------");
				}
				Query query = new TermQuery(new Term("field", "chocolate"));
				ScoreDoc[] hits = knownSearcher.Search(query, null, 1000).scoreDocs;
				//doc 3 should be the first hit b/c it is the shortest match
				Assert.IsTrue(hits.Length == 3);
				float score = hits[0].score;
				/*System.out.println("Hit 0: " + hits.id(0) + " Score: " + hits.score(0) + " String: " + hits.doc(0).toString());
				System.out.println("Explain: " + knownSearcher.explain(query, hits.id(0)));
				System.out.println("Hit 1: " + hits.id(1) + " Score: " + hits.score(1) + " String: " + hits.doc(1).toString());
				System.out.println("Explain: " + knownSearcher.explain(query, hits.id(1)));
				System.out.println("Hit 2: " + hits.id(2) + " Score: " + hits.score(2) + " String: " +  hits.doc(2).toString());
				System.out.println("Explain: " + knownSearcher.explain(query, hits.id(2)));*/
				Assert.IsTrue(hits[0].doc == 2);
				Assert.IsTrue(hits[1].doc == 3);
				Assert.IsTrue(hits[2].doc == 0);
				TermFreqVector vector2 = knownSearcher.reader_ForNUnit.GetTermFreqVector(hits[1].doc, "field");
				Assert.IsTrue(vector2 != null);
				//System.out.println("Vector: " + vector);
				System.String[] terms = vector2.GetTerms();
				int[] freqs2 = vector2.GetTermFrequencies();
				Assert.IsTrue(terms != null && terms.Length == 10);
				for (int i = 0; i < terms.Length; i++)
				{
					System.String term = terms[i];
					//System.out.println("Term: " + term);
					int freq = freqs2[i];
					Assert.IsTrue(test4.IndexOf(term) != - 1);
					System.Int32 freqInt = -1;
					try
					{
						freqInt = (System.Int32) test4Map[term];
					}
					catch (Exception)
					{
						Assert.IsTrue(false);
					}
					Assert.IsTrue(freqInt == freq);
				}
				SortedTermVectorMapper mapper = new SortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
				knownSearcher.reader_ForNUnit.GetTermFreqVector(hits[1].doc, mapper);
				System.Collections.Generic.SortedDictionary<object, object> vectorEntrySet = mapper.GetTermVectorEntrySet();
				Assert.IsTrue(vectorEntrySet.Count == 10, "mapper.getTermVectorEntrySet() Size: " + vectorEntrySet.Count + " is not: " + 10);
				TermVectorEntry last = null;
                foreach(TermVectorEntry tve in vectorEntrySet.Keys)
				{
					if (tve != null && last != null)
					{
						Assert.IsTrue(last.GetFrequency() >= tve.GetFrequency(), "terms are not properly sorted");
						System.Int32 expectedFreq = (System.Int32) test4Map[tve.GetTerm()];
						//we expect double the expectedFreq, since there are two fields with the exact same text and we are collapsing all fields
						Assert.IsTrue(tve.GetFrequency() == 2 * expectedFreq, "Frequency is not correct:");
					}
					last = tve;
				}
				
				FieldSortedTermVectorMapper fieldMapper = new FieldSortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
				knownSearcher.reader_ForNUnit.GetTermFreqVector(hits[1].doc, fieldMapper);
				System.Collections.IDictionary map = fieldMapper.GetFieldToTerms();
				Assert.IsTrue(map.Count == 2, "map Size: " + map.Count + " is not: " + 2);
				vectorEntrySet = (System.Collections.Generic.SortedDictionary<Object,Object>) map["field"];
				Assert.IsTrue(vectorEntrySet != null, "vectorEntrySet is null and it shouldn't be");
				Assert.IsTrue(vectorEntrySet.Count == 10, "vectorEntrySet Size: " + vectorEntrySet.Count + " is not: " + 10);
				knownSearcher.Close();
			}
			catch (System.IO.IOException e)
			{
				System.Console.Error.WriteLine(e.StackTrace);
				Assert.IsTrue(false);
			}
		}
		
		private void  SetupDoc(Document doc, System.String text)
		{
			doc.Add(new Field("field2", text, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			doc.Add(new Field("field", text, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
			//System.out.println("Document: " + doc);
		}
		
		// Test only a few docs having vectors
		[Test]
		public virtual void  TestRareVectors()
		{
			IndexWriter writer = new IndexWriter(directory, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < 100; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("field", English.IntToEnglish(i), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
				writer.AddDocument(doc);
			}
			for (int i = 0; i < 10; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("field", English.IntToEnglish(100 + i), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
				writer.AddDocument(doc);
			}
			
			writer.Close();
			searcher = new IndexSearcher(directory);
			
			Query query = new TermQuery(new Term("field", "hundred"));
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(10, hits.Length);
			for (int i = 0; i < hits.Length; i++)
			{
				TermFreqVector[] vector = searcher.reader_ForNUnit.GetTermFreqVectors(hits[i].doc);
				Assert.IsTrue(vector != null);
				Assert.IsTrue(vector.Length == 1);
			}
		}
		
		
		// In a single doc, for the same field, mix the term
		// vectors up
		[Test]
		public virtual void  TestMixedVectrosVectors()
		{
			IndexWriter writer = new IndexWriter(directory, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("field", "one", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
			doc.Add(new Field("field", "one", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
			doc.Add(new Field("field", "one", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS));
			doc.Add(new Field("field", "one", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_OFFSETS));
			doc.Add(new Field("field", "one", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			writer.AddDocument(doc);
			writer.Close();
			
			searcher = new IndexSearcher(directory);
			
			Query query = new TermQuery(new Term("field", "one"));
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			
			TermFreqVector[] vector = searcher.reader_ForNUnit.GetTermFreqVectors(hits[0].doc);
			Assert.IsTrue(vector != null);
			Assert.IsTrue(vector.Length == 1);
			TermPositionVector tfv = (TermPositionVector) vector[0];
			Assert.IsTrue(tfv.GetField().Equals("field"));
			System.String[] terms = tfv.GetTerms();
			Assert.AreEqual(1, terms.Length);
			Assert.AreEqual(terms[0], "one");
			Assert.AreEqual(5, tfv.GetTermFrequencies()[0]);
			
			int[] positions = tfv.GetTermPositions(0);
			Assert.AreEqual(5, positions.Length);
			for (int i = 0; i < 5; i++)
				Assert.AreEqual(i, positions[i]);
			TermVectorOffsetInfo[] offsets = tfv.GetOffsets(0);
			Assert.AreEqual(5, offsets.Length);
			for (int i = 0; i < 5; i++)
			{
				Assert.AreEqual(4 * i, offsets[i].GetStartOffset());
				Assert.AreEqual(4 * i + 3, offsets[i].GetEndOffset());
			}
		}
	}
}