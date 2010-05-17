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

using OffsetAttribute = Lucene.Net.Analysis.Tokenattributes.OffsetAttribute;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using TermDocs = Lucene.Net.Index.TermDocs;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;

namespace Lucene.Net.Analysis
{
	
    [TestFixture]
	public class TestKeywordAnalyzer:BaseTokenStreamTestCase
	{
		
		private RAMDirectory directory;
		private IndexSearcher searcher;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
			Document doc = new Document();
			doc.Add(new Field("partnum", "Q36", Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.Add(new Field("description", "Illidium Space Modulator", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			
			writer.Close();
			
			searcher = new IndexSearcher(directory);
		}
		
        [Test]
		public virtual void  TestPerFieldAnalyzer()
		{
			PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new SimpleAnalyzer());
			analyzer.AddAnalyzer("partnum", new KeywordAnalyzer());
			
			QueryParser queryParser = new QueryParser("description", analyzer);
			Query query = queryParser.Parse("partnum:Q36 AND SPACE");
			
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual("+partnum:Q36 +space", query.ToString("description"), "Q36 kept as-is");
			Assert.AreEqual(1, hits.Length, "doc found!");
		}
		
        [Test]
		public virtual void  TestMutipleDocument()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new KeywordAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			doc.Add(new Field("partnum", "Q36", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			doc = new Document();
			doc.Add(new Field("partnum", "Q37", Field.Store.YES, Field.Index.ANALYZED));
			writer.AddDocument(doc);
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
			TermDocs td = reader.TermDocs(new Term("partnum", "Q36"));
			Assert.IsTrue(td.Next());
			td = reader.TermDocs(new Term("partnum", "Q37"));
			Assert.IsTrue(td.Next());
		}
		
		// LUCENE-1441
        [Test]
		public virtual void  TestOffsets()
		{
			TokenStream stream = new KeywordAnalyzer().TokenStream("field", new System.IO.StringReader("abcd"));
			OffsetAttribute offsetAtt = (OffsetAttribute) stream.AddAttribute(typeof(OffsetAttribute));
			Assert.IsTrue(stream.IncrementToken());
			Assert.AreEqual(0, offsetAtt.StartOffset());
			Assert.AreEqual(4, offsetAtt.EndOffset());
		}
	}
}