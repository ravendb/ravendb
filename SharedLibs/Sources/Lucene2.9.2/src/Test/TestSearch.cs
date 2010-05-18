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

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Store;
using Lucene.Net.Search;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net
{
	
	/// <summary>JUnit adaptation of an older test case SearchTest.
	/// 
	/// </summary>
	/// <version>  $Id: TestSearch.java 694004 2008-09-10 21:38:52Z mikemccand $
	/// </version>
	[TestFixture]
	public class TestSearch:LuceneTestCase
	{
		
		/// <summary>Main for running test case by itself. </summary>
		[STAThread]
		public static void  Main(System.String[] args)
		{
			// TestRunner.run(new TestSuite(typeof(TestSearch))); // {{Aroush-2.9}} how is this done in NUnit?
		}
		
		/// <summary>This test performs a number of searches. It also compares output
		/// of searches using multi-file index segments with single-file
		/// index segments.
		/// 
		/// TODO: someone should check that the results of the searches are
		/// still correct by adding assert statements. Right now, the test
		/// passes if the results are the same between multi-file and
		/// single-file formats, even if the results are wrong.
		/// </summary>
        [Test]
        public virtual void TestSearch_Renamed()
		{
			System.IO.MemoryStream sw = new System.IO.MemoryStream();
			System.IO.StreamWriter pw = new System.IO.StreamWriter(sw);
			DoTestSearch(pw, false);
			pw.Close();
			sw.Close();
			System.String multiFileOutput = System.Text.ASCIIEncoding.ASCII.GetString(sw.ToArray());
			//System.out.println(multiFileOutput);
			
			sw = new System.IO.MemoryStream();
			pw = new System.IO.StreamWriter(sw);
			DoTestSearch(pw, true);
			pw.Close();
			sw.Close();
			System.String singleFileOutput = System.Text.ASCIIEncoding.ASCII.GetString(sw.ToArray());
			
			Assert.AreEqual(multiFileOutput, singleFileOutput);
		}
		
		
		private void  DoTestSearch(System.IO.StreamWriter out_Renamed, bool useCompoundFile)
		{
			Directory directory = new RAMDirectory();
			Analyzer analyzer = new SimpleAnalyzer();
			IndexWriter writer = new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			
			writer.SetUseCompoundFile(useCompoundFile);
			
			System.String[] docs = new System.String[]{"a b c d e", "a b c d e a b c d e", "a b c d e f g h i j", "a c e", "e c a", "a c e a c e", "a c e a b c"};
			for (int j = 0; j < docs.Length; j++)
			{
				Document d = new Document();
				d.Add(new Field("contents", docs[j], Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(d);
			}
			writer.Close();
			
			Searcher searcher = new IndexSearcher(directory);
			
			System.String[] queries = new System.String[]{"a b", "\"a b\"", "\"a b c\"", "a c", "\"a c\"", "\"a c e\""};
			ScoreDoc[] hits = null;
			
			QueryParser parser = new QueryParser("contents", analyzer);
			parser.SetPhraseSlop(4);
			for (int j = 0; j < queries.Length; j++)
			{
				Query query = parser.Parse(queries[j]);
				out_Renamed.WriteLine("Query: " + query.ToString("contents"));
				
				//DateFilter filter =
				//  new DateFilter("modified", Time(1997,0,1), Time(1998,0,1));
				//DateFilter filter = DateFilter.Before("modified", Time(1997,00,01));
				//System.out.println(filter);
				
				hits = searcher.Search(query, null, 1000).scoreDocs;
				
				out_Renamed.WriteLine(hits.Length + " total results");
				for (int i = 0; i < hits.Length && i < 10; i++)
				{
					Document d = searcher.Doc(hits[i].doc);
					out_Renamed.WriteLine(i + " " + hits[i].score + " " + d.Get("contents"));
				}
			}
			searcher.Close();
		}
		
		internal static long Time(int year, int month, int day)
		{
			System.DateTime calendar = new System.DateTime(year, month, day, 0, 0, 0, 0, new System.Globalization.GregorianCalendar());
			return calendar.Ticks;
		}
	}
}