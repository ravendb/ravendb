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
using NumericField = Lucene.Net.Documents.NumericField;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using MaxFieldLength = Lucene.Net.Index.IndexWriter.MaxFieldLength;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestMultiValuedNumericRangeQuery:LuceneTestCase
	{
		
		/// <summary>Tests NumericRangeQuery on a multi-valued field (multiple numeric values per document).
		/// This test ensures, that a classical TermRangeQuery returns exactly the same document numbers as
		/// NumericRangeQuery (see SOLR-1322 for discussion) and the multiple precision terms per numeric value
		/// do not interfere with multiple numeric values.
		/// </summary>
		
        [Test]
		public virtual void  TestMultiValuedNRQ()
		{
			System.Random rnd = NewRandom();
			
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, MaxFieldLength.UNLIMITED);
			
			//DecimalFormat format = new DecimalFormat("00000000000", new System.Globalization.CultureInfo("en-US").NumberFormat);
			
			for (int l = 0; l < 5000; l++)
			{
				Document doc = new Document();
				for (int m = 0, c = rnd.Next(10); m <= c; m++)
				{
					int value_Renamed = rnd.Next(System.Int32.MaxValue);
                    doc.Add(new Field("asc", value_Renamed.ToString().PadLeft(11, '0'), Field.Store.NO, Field.Index.NOT_ANALYZED));
					doc.Add(new NumericField("trie", Field.Store.NO, true).SetIntValue(value_Renamed));
				}
				writer.AddDocument(doc);
			}
			writer.Close();
			
			Searcher searcher = new IndexSearcher(directory, true);
			for (int i = 0; i < 50; i++)
			{
				int lower = rnd.Next(System.Int32.MaxValue);
				int upper = rnd.Next(System.Int32.MaxValue);
				if (lower > upper)
				{
					int a = lower; lower = upper; upper = a;
				}
				TermRangeQuery cq = new TermRangeQuery("asc", lower.ToString().PadLeft(11, '0'),  upper.ToString().PadLeft(11, '0'), true, true);
				System.Int32 tempAux = (System.Int32) lower;
				System.Int32 tempAux2 = (System.Int32) upper;
				NumericRangeQuery tq = NumericRangeQuery.NewIntRange("trie", tempAux, tempAux2, true, true);
				TopDocs trTopDocs = searcher.Search(cq, 1);
				TopDocs nrTopDocs = searcher.Search(tq, 1);
				Assert.AreEqual(trTopDocs.totalHits, nrTopDocs.totalHits, "Returned count for NumericRangeQuery and TermRangeQuery must be equal");
			}
			searcher.Close();
			
			directory.Close();
		}
	}
}