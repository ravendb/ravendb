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
using TokenStream = Lucene.Net.Analysis.TokenStream;
using Tokenizer = Lucene.Net.Analysis.Tokenizer;
using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	
    [TestFixture]
	public class TestTermRangeQuery:LuceneTestCase
	{
		
		private int docCount = 0;
		private RAMDirectory dir;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			dir = new RAMDirectory();
		}
		
        [Test]
		public virtual void  TestExclusive()
		{
			Query query = new TermRangeQuery("content", "A", "C", false, false);
			InitializeIndex(new System.String[]{"A", "B", "C", "D"});
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "A,B,C,D, only B in range");
			searcher.Close();
			
			InitializeIndex(new System.String[]{"A", "B", "D"});
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "A,B,D, only B in range");
			searcher.Close();
			
			AddDoc("C");
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "C added, still only B in range");
			searcher.Close();
		}
		
		//TODO: remove in Lucene 3.0
        [Test]
		public virtual void  TestDeprecatedCstrctors()
		{
			Query query = new RangeQuery(null, new Term("content", "C"), false);
			InitializeIndex(new System.String[]{"A", "B", "C", "D"});
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length, "A,B,C,D, only B in range");
			searcher.Close();
			
			query = new RangeQuery(new Term("content", "C"), null, false);
			InitializeIndex(new System.String[]{"A", "B", "C", "D"});
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "A,B,C,D, only B in range");
			searcher.Close();
		}
		
        [Test]
		public virtual void  TestInclusive()
		{
			Query query = new TermRangeQuery("content", "A", "C", true, true);
			
			InitializeIndex(new System.String[]{"A", "B", "C", "D"});
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length, "A,B,C,D - A,B,C in range");
			searcher.Close();
			
			InitializeIndex(new System.String[]{"A", "B", "D"});
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length, "A,B,D - A and B in range");
			searcher.Close();
			
			AddDoc("C");
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length, "C added - A, B, C in range");
			searcher.Close();
		}
		
        [Test]
		public virtual void  TestEqualsHashcode()
		{
			Query query = new TermRangeQuery("content", "A", "C", true, true);
			
			query.SetBoost(1.0f);
			Query other = new TermRangeQuery("content", "A", "C", true, true);
			other.SetBoost(1.0f);
			
			Assert.AreEqual(query, query, "query equals itself is true");
			Assert.AreEqual(query, other, "equivalent queries are equal");
			Assert.AreEqual(query.GetHashCode(), other.GetHashCode(), "hashcode must return same value when equals is true");
			
			other.SetBoost(2.0f);
			Assert.IsFalse(query.Equals(other), "Different boost queries are not equal");
			
			other = new TermRangeQuery("notcontent", "A", "C", true, true);
			Assert.IsFalse(query.Equals(other), "Different fields are not equal");
			
			other = new TermRangeQuery("content", "X", "C", true, true);
			Assert.IsFalse(query.Equals(other), "Different lower terms are not equal");
			
			other = new TermRangeQuery("content", "A", "Z", true, true);
			Assert.IsFalse(query.Equals(other), "Different upper terms are not equal");
			
			query = new TermRangeQuery("content", null, "C", true, true);
			other = new TermRangeQuery("content", null, "C", true, true);
			Assert.AreEqual(query, other, "equivalent queries with null lowerterms are equal()");
			Assert.AreEqual(query.GetHashCode(), other.GetHashCode(), "hashcode must return same value when equals is true");
			
			query = new TermRangeQuery("content", "C", null, true, true);
			other = new TermRangeQuery("content", "C", null, true, true);
			Assert.AreEqual(query, other, "equivalent queries with null upperterms are equal()");
			Assert.AreEqual(query.GetHashCode(), other.GetHashCode(), "hashcode returns same value");
			
			query = new TermRangeQuery("content", null, "C", true, true);
			other = new TermRangeQuery("content", "C", null, true, true);
			Assert.IsFalse(query.Equals(other), "queries with different upper and lower terms are not equal");
			
			query = new TermRangeQuery("content", "A", "C", false, false);
			other = new TermRangeQuery("content", "A", "C", true, true);
			Assert.IsFalse(query.Equals(other), "queries with different inclusive are not equal");
			
			query = new TermRangeQuery("content", "A", "C", false, false);
			other = new TermRangeQuery("content", "A", "C", false, false, System.Globalization.CultureInfo.CurrentCulture.CompareInfo);
			Assert.IsFalse(query.Equals(other), "a query with a collator is not equal to one without");
		}
		
        [Test]
		public virtual void  TestExclusiveCollating()
		{
			Query query = new TermRangeQuery("content", "A", "C", false, false, new System.Globalization.CultureInfo("en").CompareInfo);
			InitializeIndex(new System.String[]{"A", "B", "C", "D"});
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "A,B,C,D, only B in range");
			searcher.Close();
			
			InitializeIndex(new System.String[]{"A", "B", "D"});
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "A,B,D, only B in range");
			searcher.Close();
			
			AddDoc("C");
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "C added, still only B in range");
			searcher.Close();
		}
		
        [Test]
		public virtual void  TestInclusiveCollating()
		{
			Query query = new TermRangeQuery("content", "A", "C", true, true, new System.Globalization.CultureInfo("en").CompareInfo);
			
			InitializeIndex(new System.String[]{"A", "B", "C", "D"});
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length, "A,B,C,D - A,B,C in range");
			searcher.Close();
			
			InitializeIndex(new System.String[]{"A", "B", "D"});
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(2, hits.Length, "A,B,D - A and B in range");
			searcher.Close();
			
			AddDoc("C");
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(3, hits.Length, "C added - A, B, C in range");
			searcher.Close();
		}
		
        [Test]
		public virtual void  TestFarsi()
		{
			// Neither Java 1.4.2 nor 1.5.0 has Farsi Locale collation available in
			// RuleBasedCollator.  However, the Arabic Locale seems to order the Farsi
			// characters properly.
			System.Globalization.CompareInfo collator = new System.Globalization.CultureInfo("ar").CompareInfo;
			Query query = new TermRangeQuery("content", "\u062F", "\u0698", true, true, collator);
			// Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
			// orders the U+0698 character before the U+0633 character, so the single
			// index Term below should NOT be returned by a TermRangeQuery with a Farsi
			// Collator (or an Arabic one for the case when Farsi is not supported).
			InitializeIndex(new System.String[]{"\u0633\u0627\u0628"});
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length, "The index Term should not be included.");
			
			query = new TermRangeQuery("content", "\u0633", "\u0638", true, true, collator);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "The index Term should be included.");
			searcher.Close();
		}
		
        [Test]
		public virtual void  TestDanish()
		{
			System.Globalization.CompareInfo collator = new System.Globalization.CultureInfo("da" + "-" + "dk").CompareInfo;
			// Danish collation orders the words below in the given order (example taken
			// from TestSort.testInternationalSort() ).
			System.String[] words = new System.String[]{"H\u00D8T", "H\u00C5T", "MAND"};
			Query query = new TermRangeQuery("content", "H\u00D8T", "MAND", false, false, collator);
			
			// Unicode order would not include "H\u00C5T" in [ "H\u00D8T", "MAND" ],
			// but Danish collation does.
			InitializeIndex(words);
			IndexSearcher searcher = new IndexSearcher(dir);
			ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(1, hits.Length, "The index Term should be included.");
			
			query = new TermRangeQuery("content", "H\u00C5T", "MAND", false, false, collator);
			hits = searcher.Search(query, null, 1000).scoreDocs;
			Assert.AreEqual(0, hits.Length, "The index Term should not be included.");
			searcher.Close();
		}
		
		private class SingleCharAnalyzer:Analyzer
		{
			
			private class SingleCharTokenizer:Tokenizer
			{
				internal char[] buffer = new char[1];
				internal bool done;
				internal TermAttribute termAtt;
				
				public SingleCharTokenizer(System.IO.TextReader r):base(r)
				{
					termAtt = (TermAttribute) AddAttribute(typeof(TermAttribute));
				}
				
				public override bool IncrementToken()
				{
					int count = input.Read((System.Char[]) buffer, 0, buffer.Length);
					if (done)
						return false;
					else
					{
                        ClearAttributes();
						done = true;
						if (count == 1)
						{
							termAtt.TermBuffer()[0] = buffer[0];
							termAtt.SetTermLength(1);
						}
						else
							termAtt.SetTermLength(0);
						return true;
					}
				}
				
				public override void  Reset(System.IO.TextReader reader)
				{
					base.Reset(reader);
					done = false;
				}
			}
			
			public override TokenStream ReusableTokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				Tokenizer tokenizer = (Tokenizer) GetPreviousTokenStream();
				if (tokenizer == null)
				{
					tokenizer = new SingleCharTokenizer(reader);
					SetPreviousTokenStream(tokenizer);
				}
				else
					tokenizer.Reset(reader);
				return tokenizer;
			}
			
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				return new SingleCharTokenizer(reader);
			}
		}
		
		private void  InitializeIndex(System.String[] values)
		{
			InitializeIndex(values, new WhitespaceAnalyzer());
		}
		
		private void  InitializeIndex(System.String[] values, Analyzer analyzer)
		{
			IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			for (int i = 0; i < values.Length; i++)
			{
				InsertDoc(writer, values[i]);
			}
			writer.Close();
		}
		
		private void  AddDoc(System.String content)
		{
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
			InsertDoc(writer, content);
			writer.Close();
		}
		
		private void  InsertDoc(IndexWriter writer, System.String content)
		{
			Document doc = new Document();
			
			doc.Add(new Field("id", "id" + docCount, Field.Store.YES, Field.Index.NOT_ANALYZED));
			doc.Add(new Field("content", content, Field.Store.NO, Field.Index.ANALYZED));
			
			writer.AddDocument(doc);
			docCount++;
		}
		
		// LUCENE-38
        [Test]
		public virtual void  TestExclusiveLowerNull()
		{
			Analyzer analyzer = new SingleCharAnalyzer();
			//http://issues.apache.org/jira/browse/LUCENE-38
			Query query = new TermRangeQuery("content", null, "C", false, false);
			InitializeIndex(new System.String[]{"A", "B", "", "C", "D"}, analyzer);
			IndexSearcher searcher = new IndexSearcher(dir);
			Hits hits = searcher.Search(query);
			// When Lucene-38 is fixed, use the assert on the next line:
			Assert.AreEqual(3, hits.Length(), "A,B,<empty string>,C,D => A, B & <empty string> are in range");
			// until Lucene-38 is fixed, use this assert:
            //Assert.AreEqual(2, hits.length(),"A,B,<empty string>,C,D => A, B & <empty string> are in range");
			
			searcher.Close();
			InitializeIndex(new System.String[]{"A", "B", "", "D"}, analyzer);
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query);
			// When Lucene-38 is fixed, use the assert on the next line:
			Assert.AreEqual(3, hits.Length(), "A,B,<empty string>,D => A, B & <empty string> are in range");
			// until Lucene-38 is fixed, use this assert:
            //Assert.AreEqual(2, hits.length(), "A,B,<empty string>,D => A, B & <empty string> are in range");
			searcher.Close();
			AddDoc("C");
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query);
			// When Lucene-38 is fixed, use the assert on the next line:
			Assert.AreEqual(3, hits.Length(), "C added, still A, B & <empty string> are in range");
			// until Lucene-38 is fixed, use this assert
            //Assert.AreEqual(2, hits.length(), "C added, still A, B & <empty string> are in range");
			searcher.Close();
		}
		
		// LUCENE-38
        [Test]
		public virtual void  TestInclusiveLowerNull()
		{
			//http://issues.apache.org/jira/browse/LUCENE-38
			Analyzer analyzer = new SingleCharAnalyzer();
			Query query = new TermRangeQuery("content", null, "C", true, true);
			InitializeIndex(new System.String[]{"A", "B", "", "C", "D"}, analyzer);
			IndexSearcher searcher = new IndexSearcher(dir);
			Hits hits = searcher.Search(query);
			// When Lucene-38 is fixed, use the assert on the next line:
			Assert.AreEqual(4, hits.Length(), "A,B,<empty string>,C,D => A,B,<empty string>,C in range");
			// until Lucene-38 is fixed, use this assert
            //Assert.AreEqual(3, hits.length(), "A,B,<empty string>,C,D => A,B,<empty string>,C in range");
			searcher.Close();
			InitializeIndex(new System.String[]{"A", "B", "", "D"}, analyzer);
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query);
			// When Lucene-38 is fixed, use the assert on the next line:
			Assert.AreEqual(3, hits.Length(), "A,B,<empty string>,D - A, B and <empty string> in range");
			// until Lucene-38 is fixed, use this assert
            //Assert.AreEqual(2, hits.length(), "A,B,<empty string>,D => A, B and <empty string> in range");
			searcher.Close();
			AddDoc("C");
			searcher = new IndexSearcher(dir);
			hits = searcher.Search(query);
			// When Lucene-38 is fixed, use the assert on the next line:
			Assert.AreEqual(4, hits.Length(), "C added => A,B,<empty string>,C in range");
			// until Lucene-38 is fixed, use this assert
            //Assert.AreEqual(3, hits.length(), "C added => A,B,<empty string>,C in range");
			searcher.Close();
		}
	}
}