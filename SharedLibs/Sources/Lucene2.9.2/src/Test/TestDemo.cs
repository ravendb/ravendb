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

using Analyzer = Lucene.Net.Analysis.Analyzer;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using ParseException = Lucene.Net.QueryParsers.ParseException;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using Version = Lucene.Net.Util.Version;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net
{
	
	/// <summary> A very simple demo used in the API documentation (src/java/overview.html).
	/// 
	/// Please try to keep src/java/overview.html up-to-date when making changes
	/// to this class.
	/// </summary>
	[TestFixture]
	public class TestDemo:LuceneTestCase
	{
		
		[Test]
		public virtual void  TestDemo_Renamed()
		{
			
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
			
			// Store the index in memory:
			Directory directory = new RAMDirectory();
			// To store an index on disk, use this instead:
			//Directory directory = FSDirectory.open("/tmp/testindex");
			IndexWriter iwriter = new IndexWriter(directory, analyzer, true, new IndexWriter.MaxFieldLength(25000));
			Document doc = new Document();
			System.String text = "This is the text to be indexed.";
			doc.Add(new Field("fieldname", text, Field.Store.YES, Field.Index.ANALYZED));
			iwriter.AddDocument(doc);
			iwriter.Close();
			
			// Now search the index:
			IndexSearcher isearcher = new IndexSearcher(directory, true); // read-only=true
			// Parse a simple query that searches for "text":
			QueryParser parser = new QueryParser("fieldname", analyzer);
			Query query = parser.Parse("text");
			ScoreDoc[] hits = isearcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length);
			// Iterate through the results:
			for (int i = 0; i < hits.Length; i++)
			{
				Document hitDoc = isearcher.Doc(hits[i].doc);
				Assert.AreEqual(hitDoc.Get("fieldname"), "This is the text to be indexed.");
			}
			isearcher.Close();
			directory.Close();
		}
	}
}