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
using Lucene.Net.Index;
using Lucene.Net.Store;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestElevationComparator:LuceneTestCase
	{
		
		private System.Collections.IDictionary priority = new System.Collections.Hashtable();
		
		//@Test
        [Test]
		public virtual void  TestSorting()
		{
			Directory directory = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			writer.SetMergeFactor(1000);
			writer.AddDocument(Adoc(new System.String[]{"id", "a", "title", "ipod", "str_s", "a"}));
			writer.AddDocument(Adoc(new System.String[]{"id", "b", "title", "ipod ipod", "str_s", "b"}));
			writer.AddDocument(Adoc(new System.String[]{"id", "c", "title", "ipod ipod ipod", "str_s", "c"}));
			writer.AddDocument(Adoc(new System.String[]{"id", "x", "title", "boosted", "str_s", "x"}));
			writer.AddDocument(Adoc(new System.String[]{"id", "y", "title", "boosted boosted", "str_s", "y"}));
			writer.AddDocument(Adoc(new System.String[]{"id", "z", "title", "boosted boosted boosted", "str_s", "z"}));
			
			IndexReader r = writer.GetReader();
			writer.Close();
			
			IndexSearcher searcher = new IndexSearcher(r);
			
			RunTest(searcher, true);
			RunTest(searcher, false);
			
			searcher.Close();
			r.Close();
			directory.Close();
		}
		
		private void  RunTest(IndexSearcher searcher, bool reversed)
		{
			
			BooleanQuery newq = new BooleanQuery(false);
			TermQuery query = new TermQuery(new Term("title", "ipod"));
			
			newq.Add(query, BooleanClause.Occur.SHOULD);
			newq.Add(GetElevatedQuery(new System.String[]{"id", "a", "id", "x"}), BooleanClause.Occur.SHOULD);
			
			Sort sort = new Sort(new SortField[]{new SortField("id", new ElevationComparatorSource(priority), false), new SortField(null, SortField.SCORE, reversed)});
			
			TopDocsCollector topCollector = TopFieldCollector.create(sort, 50, false, true, true, true);
			searcher.Search(newq, null, topCollector);
			
			TopDocs topDocs = topCollector.TopDocs(0, 10);
			int nDocsReturned = topDocs.scoreDocs.Length;
			
			Assert.AreEqual(4, nDocsReturned);
			
			// 0 & 3 were elevated
			Assert.AreEqual(0, topDocs.scoreDocs[0].doc);
			Assert.AreEqual(3, topDocs.scoreDocs[1].doc);
			
			if (reversed)
			{
				Assert.AreEqual(2, topDocs.scoreDocs[2].doc);
				Assert.AreEqual(1, topDocs.scoreDocs[3].doc);
			}
			else
			{
				Assert.AreEqual(1, topDocs.scoreDocs[2].doc);
				Assert.AreEqual(2, topDocs.scoreDocs[3].doc);
			}
			
			/*
			for (int i = 0; i < nDocsReturned; i++) {
			ScoreDoc scoreDoc = topDocs.scoreDocs[i];
			ids[i] = scoreDoc.doc;
			scores[i] = scoreDoc.score;
			documents[i] = searcher.doc(ids[i]);
			System.out.println("ids[i] = " + ids[i]);
			System.out.println("documents[i] = " + documents[i]);
			System.out.println("scores[i] = " + scores[i]);
			}
			*/
		}
		
		private Query GetElevatedQuery(System.String[] vals)
		{
			BooleanQuery q = new BooleanQuery(false);
			q.SetBoost(0);
			int max = (vals.Length / 2) + 5;
			for (int i = 0; i < vals.Length - 1; i += 2)
			{
				q.Add(new TermQuery(new Term(vals[i], vals[i + 1])), BooleanClause.Occur.SHOULD);
				priority[vals[i + 1]] = (System.Int32) max--;
				// System.out.println(" pri doc=" + vals[i+1] + " pri=" + (1+max));
			}
			return q;
		}
		
		private Document Adoc(System.String[] vals)
		{
			Document doc = new Document();
			for (int i = 0; i < vals.Length - 2; i += 2)
			{
				doc.Add(new Field(vals[i], vals[i + 1], Field.Store.YES, Field.Index.ANALYZED));
			}
			return doc;
		}
	}
	
	[Serializable]
	class ElevationComparatorSource:FieldComparatorSource
	{
		private class AnonymousClassFieldComparator:FieldComparator
		{
			public AnonymousClassFieldComparator(int numHits, System.String fieldname, ElevationComparatorSource enclosingInstance)
			{
				InitBlock(numHits, fieldname, enclosingInstance);
			}
			private void  InitBlock(int numHits, System.String fieldname, ElevationComparatorSource enclosingInstance)
			{
				this.numHits = numHits;
				this.fieldname = fieldname;
				this.enclosingInstance = enclosingInstance;
				values = new int[numHits];
			}
			private int numHits;
			private System.String fieldname;
			private ElevationComparatorSource enclosingInstance;
			public ElevationComparatorSource Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			internal Lucene.Net.Search.StringIndex idIndex;
			private int[] values;
			internal int bottomVal;
			
			public override int Compare(int slot1, int slot2)
			{
				return values[slot2] - values[slot1]; // values will be small enough that there is no overflow concern
			}
			
			public override void  SetBottom(int slot)
			{
				bottomVal = values[slot];
			}
			
			private int DocVal(int doc)
			{
				System.String id = idIndex.lookup[idIndex.order[doc]];
				object prio = Enclosing_Instance.priority[id];
                return prio == null ? 0 : (int)prio;
			}
			
			public override int CompareBottom(int doc)
			{
				return DocVal(doc) - bottomVal;
			}
			
			public override void  Copy(int slot, int doc)
			{
				values[slot] = DocVal(doc);
			}
			
			public override void  SetNextReader(IndexReader reader, int docBase)
			{
				idIndex = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetStringIndex(reader, fieldname);
			}
			
			public override System.IComparable Value(int slot)
			{
				return (System.Int32) values[slot];
			}
		}
		private System.Collections.IDictionary priority;
		
		public ElevationComparatorSource(System.Collections.IDictionary boosts)
		{
			this.priority = boosts;
		}
		
		public override FieldComparator NewComparator(System.String fieldname, int numHits, int sortPos, bool reversed)
		{
			return new AnonymousClassFieldComparator(numHits, fieldname, this);
		}
	}
}