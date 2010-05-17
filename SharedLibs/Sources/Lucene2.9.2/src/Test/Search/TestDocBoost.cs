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
using Lucene.Net.Documents;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary>Document boost unit test.
	/// 
	/// 
	/// </summary>
	/// <version>  $Revision: 787772 $
	/// </version>
    [TestFixture]
	public class TestDocBoost:LuceneTestCase
	{
		private class AnonymousClassCollector:Collector
		{
			public AnonymousClassCollector(float[] scores, TestDocBoost enclosingInstance)
			{
				InitBlock(scores, enclosingInstance);
			}
			private void  InitBlock(float[] scores, TestDocBoost enclosingInstance)
			{
				this.scores = scores;
				this.enclosingInstance = enclosingInstance;
			}
			private float[] scores;
			private TestDocBoost enclosingInstance;
			public TestDocBoost Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private int base_Renamed = 0;
			private Scorer scorer;
			public override void  SetScorer(Scorer scorer)
			{
				this.scorer = scorer;
			}
			public override void  Collect(int doc)
			{
				scores[doc + base_Renamed] = scorer.Score();
			}
			public override void  SetNextReader(IndexReader reader, int docBase)
			{
				base_Renamed = docBase;
			}
			public override bool AcceptsDocsOutOfOrder()
			{
				return true;
			}
		}
		
		[Test]
		public virtual void  TestDocBoost_Renamed()
		{
			RAMDirectory store = new RAMDirectory();
			IndexWriter writer = new IndexWriter(store, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			Fieldable f1 = new Field("field", "word", Field.Store.YES, Field.Index.ANALYZED);
			Fieldable f2 = new Field("field", "word", Field.Store.YES, Field.Index.ANALYZED);
			f2.SetBoost(2.0f);
			
			Document d1 = new Document();
			Document d2 = new Document();
			Document d3 = new Document();
			Document d4 = new Document();
			d3.SetBoost(3.0f);
			d4.SetBoost(2.0f);
			
			d1.Add(f1); // boost = 1
			d2.Add(f2); // boost = 2
			d3.Add(f1); // boost = 3
			d4.Add(f2); // boost = 4
			
			writer.AddDocument(d1);
			writer.AddDocument(d2);
			writer.AddDocument(d3);
			writer.AddDocument(d4);
			writer.Optimize();
			writer.Close();
			
			float[] scores = new float[4];
			
			new IndexSearcher(store).Search(new TermQuery(new Term("field", "word")), new AnonymousClassCollector(scores, this));
			
			float lastScore = 0.0f;
			
			for (int i = 0; i < 4; i++)
			{
				Assert.IsTrue(scores[i] > lastScore);
				lastScore = scores[i];
			}
		}
	}
}