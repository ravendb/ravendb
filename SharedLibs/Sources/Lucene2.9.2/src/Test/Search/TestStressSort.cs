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
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using FieldCacheSanityChecker = Lucene.Net.Util.FieldCacheSanityChecker;
using Insanity = Lucene.Net.Util.FieldCacheSanityChecker.Insanity;
using InsanityType = Lucene.Net.Util.FieldCacheSanityChecker.InsanityType;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestStressSort:LuceneTestCase
	{
		
		private const int NUM_DOCS = 5000;
		// NOTE: put seed in here to make failures
		// deterministic, but do not commit with a seed (to
		// better test):
		private System.Random r;
		private Directory dir, dir2, dir3;
		private IndexSearcher searcherMultiSegment;
		private IndexSearcher searcherFewSegment;
		private IndexSearcher searcherSingleSegment;
		
		private const bool VERBOSE = false;
		
		// min..max
		private int NextInt(int min, int max)
		{
			return min + r.Next(max - min + 1);
		}
		
		// 0..(lim-1)
		private int NextInt(int lim)
		{
			return r.Next(lim);
		}
		
		internal char[] buffer = new char[20];
		private System.String RandomString(int size)
		{
			System.Diagnostics.Debug.Assert(size < 20);
			for (int i = 0; i < size; i++)
			{
				buffer[i] = (char) NextInt(48, 122);
			}
			return new System.String(buffer, 0, size);
		}
		
		private void  Create()
		{
			
			// NOTE: put seed in here to make failures
			// deterministic, but do not commit with a seed (to
			// better test):
			dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(17);
			
			Document doc = new Document();
			Document doc2 = new Document();
			
			Field id = new Field("id", "", Field.Store.YES, Field.Index.NO);
			doc.Add(id);
			doc2.Add(id);
			
			Field contents = new Field("contents", "", Field.Store.NO, Field.Index.ANALYZED);
			doc.Add(contents);
			doc2.Add(contents);
			
			Field byteField = new Field("byte", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
			doc.Add(byteField);
			doc2.Add(byteField);
			
			Field shortField = new Field("short", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
			doc.Add(shortField);
			doc2.Add(shortField);
			
			Field intField = new Field("int", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
			doc.Add(intField);
			doc2.Add(intField);
			
			Field longField = new Field("long", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
			doc.Add(longField);
			doc2.Add(longField);
			
			Field floatField = new Field("float", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
			doc.Add(floatField);
			doc2.Add(floatField);
			
			Field doubleField = new Field("double", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
			doc.Add(doubleField);
			doc2.Add(doubleField);
			
			// we use two diff string fields so our FieldCache usage
			// is less suspicious to cache inspection
			Field stringField = new Field("string", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
			doc.Add(stringField);
			Field stringFieldIdx = new Field("stringIdx", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
			doc.Add(stringFieldIdx);
			// doc2 doesn't have stringField or stringFieldIdx, so we get nulls
			
			for (int i = 0; i < NUM_DOCS; i++)
			{
				id.SetValue("" + i);
				if (i % 1000 == 0)
				{
					contents.SetValue("a b c z");
				}
				else if (i % 100 == 0)
				{
					contents.SetValue("a b c y");
				}
				else if (i % 10 == 0)
				{
					contents.SetValue("a b c x");
				}
				else
				{
					contents.SetValue("a b c");
				}
				byteField.SetValue("" + NextInt((sbyte) System.SByte.MinValue, (sbyte) System.SByte.MaxValue));
				if (NextInt(10) == 3)
				{
					shortField.SetValue("" + System.Int16.MinValue);
				}
				else if (NextInt(10) == 7)
				{
					shortField.SetValue("" + System.Int16.MaxValue);
				}
				else
				{
					shortField.SetValue("" + NextInt(System.Int16.MinValue, System.Int16.MaxValue));
				}
				
				if (NextInt(10) == 3)
				{
					intField.SetValue("" + System.Int32.MinValue);
				}
				else if (NextInt(10) == 7)
				{
					intField.SetValue("" + System.Int32.MaxValue);
				}
				else
				{
					intField.SetValue("" + this.r.Next());
				}
				
				if (NextInt(10) == 3)
				{
					longField.SetValue("" + System.Int64.MinValue);
				}
				else if (NextInt(10) == 7)
				{
					longField.SetValue("" + System.Int64.MaxValue);
				}
				else
				{
					longField.SetValue("" + this.r.Next(System.Int32.MaxValue));
				}
				floatField.SetValue("" + (float) this.r.NextDouble());
				doubleField.SetValue("" + this.r.NextDouble());
				if (i % 197 == 0)
				{
					writer.AddDocument(doc2);
				}
				else
				{
					System.String r = RandomString(NextInt(20));
					stringField.SetValue(r);
					stringFieldIdx.SetValue(r);
					writer.AddDocument(doc);
				}
			}
			writer.Close();
			searcherMultiSegment = new IndexSearcher(dir);
			searcherMultiSegment.SetDefaultFieldSortScoring(true, true);
			
			dir2 = new MockRAMDirectory(dir);
			writer = new IndexWriter(dir2, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize();
			writer.Close();
			searcherSingleSegment = new IndexSearcher(dir2);
			searcherSingleSegment.SetDefaultFieldSortScoring(true, true);
			dir3 = new MockRAMDirectory(dir);
			writer = new IndexWriter(dir3, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize(3);
			writer.Close();
			searcherFewSegment = new IndexSearcher(dir3);
			searcherFewSegment.SetDefaultFieldSortScoring(true, true);
		}
		
		private void  Close()
		{
			searcherMultiSegment.Close();
			searcherFewSegment.Close();
			searcherSingleSegment.Close();
			dir.Close();
			dir2.Close();
		}
		
        [Test]
		public virtual void  TestSort()
		{
			this.r = NewRandom();
			
			// reverse & not
			// all types
			// restrictive & non restrictive searches (on contents)
			
			Create();
			
			Sort[] sorts = new Sort[50];
			int sortCount = 0;
			
			for (int r = 0; r < 2; r++)
			{
				Sort sort;
				bool reverse = 1 == r;
				
				sorts[sortCount++] = sort = new Sort();
				sort.SetSort(new SortField[]{new SortField("byte", SortField.BYTE, reverse)});
				
				sorts[sortCount++] = sort = new Sort();
				sort.SetSort(new SortField[]{new SortField("short", SortField.SHORT, reverse)});
				
				sorts[sortCount++] = sort = new Sort();
				sort.SetSort(new SortField[]{new SortField("int", SortField.INT, reverse)});
				
				sorts[sortCount++] = sort = new Sort();
				sort.SetSort(new SortField[]{new SortField("long", SortField.LONG, reverse)});
				
				sorts[sortCount++] = sort = new Sort();
				sort.SetSort(new SortField[]{new SortField("float", SortField.FLOAT, reverse)});
				
				sorts[sortCount++] = sort = new Sort();
				sort.SetSort(new SortField[]{new SortField("double", SortField.DOUBLE, reverse)});
				
				sorts[sortCount++] = sort = new Sort();
				sort.SetSort(new SortField[]{new SortField("string", SortField.STRING_VAL, reverse)});
				
				sorts[sortCount++] = sort = new Sort();
				sort.SetSort(new SortField[]{new SortField("stringIdx", SortField.STRING, reverse)});
				
				//sorts[sortCount++] = sort = new Sort();
				//sort.setSort(new SortField[] {new SortField("string", SortField.STRING_ORD, reverse)});
				
				//sorts[sortCount++] = sort = new Sort();
				//sort.setSort(new SortField[] {new SortField("string", SortField.STRING_ORD_VAL, reverse)});
				
				//sorts[sortCount++] = sort = new Sort();
				//sort.setSort(new SortField[] {new SortField("string", SortField.STRING_ORD_VAL_DEM, reverse)});
				
				//sorts[sortCount++] = sort = new Sort();
				//sort.setSort(new SortField[] {new SortField("string", SortField.STRING_ORD_VAL_DEM2, reverse)});
				
				sorts[sortCount++] = sort = new Sort();
				sort.SetSort(new SortField[]{new SortField(null, SortField.SCORE, reverse)});
				
				sorts[sortCount++] = sort = new Sort();
				sort.SetSort(new SortField[]{new SortField(null, SortField.DOC, reverse)});
			}
			
			Query[] queries = new Query[4];
			queries[0] = new MatchAllDocsQuery();
			queries[1] = new TermQuery(new Term("contents", "x")); // matches every 10th doc
			queries[2] = new TermQuery(new Term("contents", "y")); // matches every 100th doc
			queries[3] = new TermQuery(new Term("contents", "z")); // matches every 1000th doc
			
			for (int sx = 0; sx < 3; sx++)
			{
				IndexSearcher searcher;
				if (sx == 0)
				{
					searcher = searcherSingleSegment;
				}
				else if (sx == 1)
				{
					searcher = searcherFewSegment;
				}
				else
				{
					searcher = searcherMultiSegment;
				}
				
				for (int qx = 0; qx < queries.Length; qx++)
				{
					Query query = queries[qx];
					
					for (int q = 0; q < 3; q++)
					{
						
						int queueSize;
						if (q == 0)
						{
							queueSize = 10;
						}
						else if (q == 1)
						{
							queueSize = 100;
						}
						else
						{
							queueSize = 1000;
						}
						
						for (int s = 0; s < sortCount; s++)
						{
							Sort sort1 = sorts[s];
							
							for (int s2 = - 1; s2 < sortCount; s2++)
							{
								Sort sort;
								if (s2 == - 1)
								{
									// Single field sort
									sort = sort1;
								}
								else
								{
									sort = new Sort(new SortField[]{sort1.GetSort()[0], sorts[s2].GetSort()[0]});
								}
								
								// Old
								Sort oldSort = GetOldSort(sort);
								
								if (VERBOSE)
								{
									System.Console.Out.WriteLine("query=" + query);
									if (sx == 0)
									{
										System.Console.Out.WriteLine("  single-segment index");
									}
									else if (sx == 1)
									{
										System.Console.Out.WriteLine("  few-segment index");
									}
									else
									{
										System.Console.Out.WriteLine("  many-segment index");
									}
									System.Console.Out.WriteLine("  numHit=" + queueSize);
									System.Console.Out.WriteLine("  old=" + oldSort);
									System.Console.Out.WriteLine("  new=" + sort);
								}
								
								TopDocs newHits = searcher.Search(query, null, queueSize, sort);
								TopDocs oldHits = searcher.Search(query, null, queueSize, oldSort);
								
								Compare(oldHits, newHits);
							}
						}
					}
				}
			}
			
			// we explicitly test the old sort method and
			// compare with the new, so we expect to see SUBREADER
			// sanity checks fail.
			Insanity[] insanity = FieldCacheSanityChecker.CheckSanity(Lucene.Net.Search.FieldCache_Fields.DEFAULT);
			try
			{
				int ignored = 0;
				for (int i = 0; i < insanity.Length; i++)
				{
					if (insanity[i].GetType() == InsanityType.SUBREADER)
					{
						insanity[i] = new Insanity(InsanityType.EXPECTED, insanity[i].GetMsg(), insanity[i].GetCacheEntries());
						ignored++;
					}
				}
				Assert.AreEqual(ignored, insanity.Length, "Not all insane field cache usage was expected");
				
				insanity = null;
			}
			finally
			{
				// report this in the event of any exception/failure
				// if no failure, then insanity will be null
				if (null != insanity)
				{
					System.IO.StreamWriter temp_writer;
					temp_writer = new System.IO.StreamWriter(System.Console.OpenStandardError(), System.Console.Error.Encoding);
					temp_writer.AutoFlush = true;
					DumpArray(GetTestLabel() + ": Insane FieldCache usage(s)", insanity, temp_writer);
				}
			}
			// we've already checked FieldCache, purge so tearDown doesn't complain
			PurgeFieldCache(Lucene.Net.Search.FieldCache_Fields.DEFAULT); // so
			
			Close();
		}
		
		private Sort GetOldSort(Sort sort)
		{
			SortField[] fields = sort.GetSort();
			SortField[] oldFields = new SortField[fields.Length];
			for (int i = 0; i < fields.Length; i++)
			{
				int sortType;
				if (fields[i].GetField() != null && fields[i].GetField().Equals("string"))
				{
					sortType = SortField.STRING;
				}
				else
				{
					sortType = fields[i].GetType();
				}
				oldFields[i] = new SortField(fields[i].GetField(), sortType, fields[i].GetReverse());
				oldFields[i].SetUseLegacySearch(true);
			}
			return new Sort(oldFields);
		}
		
		private void  Compare(TopDocs oldHits, TopDocs newHits)
		{
			Assert.AreEqual(oldHits.totalHits, newHits.totalHits);
			Assert.AreEqual(oldHits.scoreDocs.Length, newHits.scoreDocs.Length);
			ScoreDoc[] oldDocs = oldHits.scoreDocs;
			ScoreDoc[] newDocs = newHits.scoreDocs;
			
			for (int i = 0; i < oldDocs.Length; i++)
			{
				if (oldDocs[i] is FieldDoc)
				{
					System.Diagnostics.Debug.Assert(newDocs[i] is FieldDoc);
					FieldDoc oldHit = (FieldDoc) oldDocs[i];
					FieldDoc newHit = (FieldDoc) newDocs[i];
					Assert.AreEqual(oldHit.doc, newHit.doc, "hit " + i + " of " + oldDocs.Length + " differs: oldDoc=" + oldHit.doc + " vs newDoc=" + newHit.doc + " oldFields=" + _TestUtil.ArrayToString(oldHit.fields) + " newFields=" + _TestUtil.ArrayToString(newHit.fields));
					
					Assert.AreEqual(oldHit.score, newHit.score, 0.00001);
					Assert.IsTrue(SupportClass.CollectionsHelper.Equals(oldHit.fields, newHit.fields));
				}
				else
				{
					ScoreDoc oldHit = oldDocs[i];
					ScoreDoc newHit = newDocs[i];
					Assert.AreEqual(oldHit.doc, newHit.doc);
					Assert.AreEqual(oldHit.score, newHit.score, 0.00001);
				}
			}
		}
	}
}