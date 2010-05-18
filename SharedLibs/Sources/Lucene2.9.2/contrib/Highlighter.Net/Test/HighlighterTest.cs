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
using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using LowerCaseTokenizer = Lucene.Net.Analysis.LowerCaseTokenizer;
using Token = Lucene.Net.Analysis.Token;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using ParseException = Lucene.Net.QueryParsers.ParseException;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using FilteredQuery = Lucene.Net.Search.FilteredQuery;
using Hits = Lucene.Net.Search.Hits;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using MultiSearcher = Lucene.Net.Search.MultiSearcher;
using PhraseQuery = Lucene.Net.Search.PhraseQuery;
using Query = Lucene.Net.Search.Query;
using RangeFilter = Lucene.Net.Search.RangeFilter;
using Searcher = Lucene.Net.Search.Searcher;
using TermQuery = Lucene.Net.Search.TermQuery;
using SpanNearQuery = Lucene.Net.Search.Spans.SpanNearQuery;
using SpanQuery = Lucene.Net.Search.Spans.SpanQuery;
using SpanTermQuery = Lucene.Net.Search.Spans.SpanTermQuery;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using Formatter = Lucene.Net.Highlight.Formatter;
using Highlighter = Lucene.Net.Highlight.Highlighter;
using NullFragmenter = Lucene.Net.Highlight.NullFragmenter;
using QueryScorer = Lucene.Net.Highlight.QueryScorer;
using Scorer = Lucene.Net.Highlight.Scorer;
using SimpleFragmenter = Lucene.Net.Highlight.SimpleFragmenter;
using SimpleHTMLEncoder = Lucene.Net.Highlight.SimpleHTMLEncoder;
using SimpleHTMLFormatter = Lucene.Net.Highlight.SimpleHTMLFormatter;
using TextFragment = Lucene.Net.Highlight.TextFragment;
using TokenGroup = Lucene.Net.Highlight.TokenGroup;
using WeightedTerm = Lucene.Net.Highlight.WeightedTerm;

namespace Lucene.Net.Search.Highlight
{
	
	/// <summary> JUnit Test for Highlighter class.</summary>
	/// <author>  mark@searcharea.co.uk
	/// </author>
	[TestFixture]
    public class HighlighterTest : Formatter
	{
        // {{Aroush-2.0.0}} Fix me
        /*
		private class AnonymousClassScorer : Scorer
		{
			public AnonymousClassScorer(HighlighterTest enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(HighlighterTest enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private HighlighterTest enclosingInstance;
			public HighlighterTest Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public virtual void  StartFragment(TextFragment newFragment)
			{
			}
			public virtual float GetTokenScore(Token token)
			{
				return 0;
			}
			public virtual float GetFragmentScore()
			{
				return 1;
			}

            public override bool SkipTo(int target)
            {
                return false;
            }
            public override int Doc()
            {
                return -1;
            }
            public override Explanation Explain(int doc)
            {
                return null;
            }
            public override bool Next()
            {
                return false;
            }
            public override float Score()
            {
                return 0;
            }
		}
        */

		private class AnonymousClassTokenStream : TokenStream
		{
			public AnonymousClassTokenStream(HighlighterTest enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(HighlighterTest enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
				lst = new System.Collections.ArrayList();
				Token t;
				t = new Token("hi", 0, 2);
				lst.Add(t);
				t = new Token("hispeed", 0, 8);
				lst.Add(t);
				t = new Token("speed", 3, 8);
				t.SetPositionIncrement(0);
				lst.Add(t);
				t = new Token("10", 8, 10);
				lst.Add(t);
				t = new Token("foo", 11, 14);
				lst.Add(t);
				iter = lst.GetEnumerator();
			}
			private HighlighterTest enclosingInstance;
			public HighlighterTest Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal System.Collections.IEnumerator iter;
			internal System.Collections.ArrayList lst;
			public override Token Next()
			{
				return iter.MoveNext() ? (Token) iter.Current : null;
			}
		}

		private class AnonymousClassTokenStream1 : TokenStream
		{
			public AnonymousClassTokenStream1(HighlighterTest enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(HighlighterTest enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
				lst = new System.Collections.ArrayList();
				Token t;
				t = new Token("hispeed", 0, 8);
				lst.Add(t);
				t = new Token("hi", 0, 2);
				t.SetPositionIncrement(0);
				lst.Add(t);
				t = new Token("speed", 3, 8);
				lst.Add(t);
				t = new Token("10", 8, 10);
				lst.Add(t);
				t = new Token("foo", 11, 14);
				lst.Add(t);
				iter = lst.GetEnumerator();
			}
			private HighlighterTest enclosingInstance;
			public HighlighterTest Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal System.Collections.IEnumerator iter;
			internal System.Collections.ArrayList lst;
			public override Token Next()
			{
				return iter.MoveNext() ? (Token) iter.Current : null;
			}
		}

		private IndexReader reader;
		private const System.String FIELD_NAME = "contents";
		private Query query;
		internal RAMDirectory ramDir;
		public Searcher searcher = null;
		public Hits hits = null;
		internal int numHighlights = 0;
		internal Analyzer analyzer = new StandardAnalyzer();
		
		internal System.String[] texts = new System.String[]{"Hello this is a piece of text that is very long and contains too much preamble and the meat is really here which says kennedy has been shot", "This piece of text refers to Kennedy at the beginning then has a longer piece of text that is very long in the middle and finally ends with another reference to Kennedy", "JFK has been shot", "John Kennedy has been shot", "This text has a typo in referring to Keneddy"};
		
		/// <summary> Constructor for HighlightExtractorTest.</summary>
		/// <param name="">arg0
		/// </param>
		//public HighlighterTest(System.String arg0)
		//{
		//}
		
		[Test]
        public virtual void  TestSimpleHighlighter()
		{
			DoSearching("Kennedy");
			Highlighter highlighter = new Highlighter(new QueryScorer(query));
			highlighter.SetTextFragmenter(new SimpleFragmenter(40));
			int maxNumFragmentsRequired = 2;
			for (int i = 0; i < hits.Length(); i++)
			{
				System.String text = hits.Doc(i).Get(FIELD_NAME);
				TokenStream tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(text));
				
				System.String result = highlighter.GetBestFragments(tokenStream, text, maxNumFragmentsRequired, "...");
				System.Console.Out.WriteLine("\t" + result);
			}
			//Not sure we can assert anything here - just running to check we dont throw any exceptions
		}
		
		
		[Test]
		public virtual void  TestGetBestFragmentsSimpleQuery()
		{
			DoSearching("Kennedy");
			DoStandardHighlights();
			Assert.IsTrue(numHighlights == 4, "Failed to find correct number of highlights " + numHighlights + " found");
		}

        [Test]
        public virtual void  TestGetFuzzyFragments()
		{
			DoSearching("Kinnedy~");
			DoStandardHighlights();
			Assert.IsTrue(numHighlights == 5, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
		[Test]
        public virtual void  TestGetWildCardFragments()
		{
			DoSearching("K?nnedy");
			DoStandardHighlights();
			Assert.IsTrue(numHighlights == 4, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
        [Test]
        public virtual void  TestGetMidWildCardFragments()
		{
			DoSearching("K*dy");
			DoStandardHighlights();
			Assert.IsTrue(numHighlights == 5, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
        [Test]
        public virtual void  TestGetRangeFragments()
		{
			System.String queryString = FIELD_NAME + ":[kannedy TO kznnedy]";
			
			//Need to explicitly set the QueryParser property to use RangeQuery rather than RangeFilters
			QueryParser parser = new QueryParser(FIELD_NAME, new StandardAnalyzer());
			parser.SetUseOldRangeQuery(true);
			query = parser.Parse(queryString);
			DoSearching(query);
			
			DoStandardHighlights();
			Assert.IsTrue(numHighlights == 5, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
		[Test]
        public virtual void  TestGetBestFragmentsPhrase()
		{
			DoSearching("\"John Kennedy\"");
			DoStandardHighlights();
			//Currently highlights "John" and "Kennedy" separately
			Assert.IsTrue(numHighlights == 2, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
        [Test]
        public virtual void  TestGetBestFragmentsSpan()
		{
			SpanQuery[] clauses = new SpanQuery[]{new SpanTermQuery(new Term("contents", "john")), new SpanTermQuery(new Term("contents", "kennedy"))};
			
			SpanNearQuery snq = new SpanNearQuery(clauses, 1, true);
			DoSearching(snq);
			DoStandardHighlights();
			//Currently highlights "John" and "Kennedy" separately
			Assert.IsTrue(numHighlights == 2, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
        [Test]
		public virtual void  TestOffByOne()
		{
			TermQuery query = new TermQuery(new Term("data", "help"));
			Highlighter hg = new Highlighter(new SimpleHTMLFormatter(), new QueryScorer(query));
			hg.SetTextFragmenter(new NullFragmenter());
			
			System.String match = null;
			match = hg.GetBestFragment(new StandardAnalyzer(), "data", "help me [54-65]");
			Assert.AreEqual("<B>help</B> me [54-65]", match);
		}
		
        [Test]
        public virtual void  TestGetBestFragmentsFilteredQuery()
		{
			RangeFilter rf = new RangeFilter("contents", "john", "john", true, true);
			SpanQuery[] clauses = new SpanQuery[]{new SpanTermQuery(new Term("contents", "john")), new SpanTermQuery(new Term("contents", "kennedy"))};
			SpanNearQuery snq = new SpanNearQuery(clauses, 1, true);
			FilteredQuery fq = new FilteredQuery(snq, rf);
			
			DoSearching(fq);
			DoStandardHighlights();
			//Currently highlights "John" and "Kennedy" separately
			Assert.IsTrue(numHighlights == 2, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
        [Test]
        public virtual void  TestGetBestFragmentsFilteredPhraseQuery()
		{
			RangeFilter rf = new RangeFilter("contents", "john", "john", true, true);
			PhraseQuery pq = new PhraseQuery();
			pq.Add(new Term("contents", "john"));
			pq.Add(new Term("contents", "kennedy"));
			FilteredQuery fq = new FilteredQuery(pq, rf);
			
			DoSearching(fq);
			DoStandardHighlights();
			//Currently highlights "John" and "Kennedy" separately
			Assert.IsTrue(numHighlights == 2, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
		[Test]
        public virtual void  TestGetBestFragmentsMultiTerm()
		{
			DoSearching("John Kenn*");
			DoStandardHighlights();
			Assert.IsTrue(numHighlights == 5, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
        [Test]
        public virtual void  TestGetBestFragmentsWithOr()
		{
			DoSearching("JFK OR Kennedy");
			DoStandardHighlights();
			Assert.IsTrue(numHighlights == 5, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
		[Test]
        public virtual void  TestGetBestSingleFragment()
		{
			DoSearching("Kennedy");
			Highlighter highlighter = new Highlighter(this, new QueryScorer(query));
			highlighter.SetTextFragmenter(new SimpleFragmenter(40));
			
			for (int i = 0; i < hits.Length(); i++)
			{
				System.String text = hits.Doc(i).Get(FIELD_NAME);
				TokenStream tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(text));
				System.String result = highlighter.GetBestFragment(tokenStream, text);
				System.Console.Out.WriteLine("\t" + result);
			}
			Assert.IsTrue(numHighlights == 4, "Failed to find correct number of highlights " + numHighlights + " found");
			
			numHighlights = 0;
			for (int i = 0; i < hits.Length(); i++)
			{
				System.String text = hits.Doc(i).Get(FIELD_NAME);
				highlighter.GetBestFragment(analyzer, FIELD_NAME, text);
			}
			Assert.IsTrue(numHighlights == 4, "Failed to find correct number of highlights " + numHighlights + " found");
			
			numHighlights = 0;
			for (int i = 0; i < hits.Length(); i++)
			{
				System.String text = hits.Doc(i).Get(FIELD_NAME);
				highlighter.GetBestFragments(analyzer, FIELD_NAME, text, 10);
			}
			Assert.IsTrue(numHighlights == 4, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
		[Test]
        public virtual void  TestGetBestSingleFragmentWithWeights()
		{
			WeightedTerm[] wTerms = new WeightedTerm[2];
			wTerms[0] = new WeightedTerm(10f, "hello");
			wTerms[1] = new WeightedTerm(1f, "kennedy");
			Highlighter highlighter = new Highlighter(new QueryScorer(wTerms));
			TokenStream tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(texts[0]));
			highlighter.SetTextFragmenter(new SimpleFragmenter(2));
			
			System.String result = highlighter.GetBestFragment(tokenStream, texts[0]).Trim();
			Assert.IsTrue("<B>Hello</B>".Equals(result), "Failed to find best section using weighted terms. Found: [" + result + "]");
			
			//readjust weights
			wTerms[1].SetWeight(50f);
			tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(texts[0]));
			highlighter = new Highlighter(new QueryScorer(wTerms));
			highlighter.SetTextFragmenter(new SimpleFragmenter(2));
			
			result = highlighter.GetBestFragment(tokenStream, texts[0]).Trim();
			Assert.IsTrue("<B>kennedy</B>".Equals(result), "Failed to find best section using weighted terms. Found: " + result);
		}
		
		
		// tests a "complex" analyzer that produces multiple 
		// overlapping tokens 
		[Test]
        public virtual void  TestOverlapAnalyzer()
		{
			//UPGRADE_TODO: Class 'java.util.HashMap' was converted to 'System.Collections.Hashtable' which has a different behavior. 'ms-help://MS.VSCC.2003/commoner/redir/redirect.htm?keyword="jlca1073_javautilHashMap_3"'
			System.Collections.Hashtable synonyms = new System.Collections.Hashtable();
			synonyms["football"] = "soccer,footie";
			Analyzer analyzer = new SynonymAnalyzer(synonyms);
			System.String srchkey = "football";
			
			System.String s = "football-soccer in the euro 2004 footie competition";
			QueryParser parser = new QueryParser("bookid", analyzer);
			Query query = parser.Parse(srchkey);
			
			Highlighter highlighter = new Highlighter(new QueryScorer(query));
			TokenStream tokenStream = analyzer.TokenStream(null, new System.IO.StringReader(s));
			// Get 3 best fragments and seperate with a "..."
			System.String result = highlighter.GetBestFragments(tokenStream, s, 3, "...");
			System.String expectedResult = "<B>football</B>-<B>soccer</B> in the euro 2004 <B>footie</B> competition";
			Assert.IsTrue(expectedResult.Equals(result), "overlapping analyzer should handle highlights OK");
		}
		
		[Test]
		public virtual void  TestGetSimpleHighlight()
		{
			DoSearching("Kennedy");
			Highlighter highlighter = new Highlighter(this, new QueryScorer(query));
			
			for (int i = 0; i < hits.Length(); i++)
			{
				System.String text = hits.Doc(i).Get(FIELD_NAME);
				TokenStream tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(text));
				
				System.String result = highlighter.GetBestFragment(tokenStream, text);
				System.Console.Out.WriteLine("\t" + result);
			}
			Assert.IsTrue(numHighlights == 4, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
		[Test]
		public virtual void  TestGetTextFragments()
		{
			DoSearching("Kennedy");
			Highlighter highlighter = new Highlighter(this, new QueryScorer(query));
			highlighter.SetTextFragmenter(new SimpleFragmenter(20));
			
			for (int i = 0; i < hits.Length(); i++)
			{
				System.String text = hits.Doc(i).Get(FIELD_NAME);
				TokenStream tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(text));
				
				System.String[] stringResults = highlighter.GetBestFragments(tokenStream, text, 10);
				
				tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(text));
				TextFragment[] fragmentResults = highlighter.GetBestTextFragments(tokenStream, text, true, 10);
				
				Assert.IsTrue(fragmentResults.Length == stringResults.Length, "Failed to find correct number of text Fragments: " + fragmentResults.Length + " vs " + stringResults.Length);
				for (int j = 0; j < stringResults.Length; j++)
				{
					//UPGRADE_TODO: Method 'java.io.PrintStream.println' was converted to 'System.Console.Out.WriteLine' which has a different behavior. 'ms-help://MS.VSCC.2003/commoner/redir/redirect.htm?keyword="jlca1073_javaioPrintStreamprintln_javalangObject_3"'
					System.Console.Out.WriteLine(fragmentResults[j]);
					Assert.IsTrue(fragmentResults[j].ToString().Equals(stringResults[j]), "Failed to find same text Fragments: " + fragmentResults[j] + " found");
				}
			}
		}
		
        [Test]
		public virtual void  TestMaxSizeHighlight()
		{
			DoSearching("meat");
			Highlighter highlighter = new Highlighter(this, new QueryScorer(query));
			highlighter.SetMaxDocBytesToAnalyze(30);
			TokenStream tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(texts[0]));
			highlighter.GetBestFragment(tokenStream, texts[0]);
			Assert.IsTrue(numHighlights == 0, "Setting MaxDocBytesToAnalyze should have prevented " + "us from finding matches for this record: " + numHighlights + " found");
		}
		
        [Test]
		public virtual void  TestMaxSizeHighlightTruncates()
		{
			System.String goodWord = "goodtoken";
			System.String[] stopWords = new System.String[]{"stoppedtoken"};
			
			TermQuery query = new TermQuery(new Term("data", goodWord));
			SimpleHTMLFormatter fm = new SimpleHTMLFormatter();
			Highlighter hg = new Highlighter(fm, new QueryScorer(query));
			hg.SetTextFragmenter(new NullFragmenter());
			
			System.String match = null;
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			sb.Append(goodWord);
			for (int i = 0; i < 10000; i++)
			{
				sb.Append(" ");
				sb.Append(stopWords[0]);
			}
			
			hg.SetMaxDocBytesToAnalyze(100);
			match = hg.GetBestFragment(new StandardAnalyzer(stopWords), "data", sb.ToString());
			Assert.IsTrue(match.Length < hg.GetMaxDocBytesToAnalyze(), "Matched text should be no more than 100 chars in length ");
			
			//add another tokenized word to the overrall length - but set way beyond 
			//the length of text under consideration (after a large slug of stop words + whitespace)
			sb.Append(" ");
			sb.Append(goodWord);
			match = hg.GetBestFragment(new StandardAnalyzer(stopWords), "data", sb.ToString());
			Assert.IsTrue(match.Length < hg.GetMaxDocBytesToAnalyze(), "Matched text should be no more than 100 chars in length ");
		}
		
		
		[Test]
		public virtual void  TestUnRewrittenQuery()
		{
			//test to show how rewritten query can still be used
			searcher = new IndexSearcher(ramDir);
			Analyzer analyzer = new StandardAnalyzer();
			
			QueryParser parser = new QueryParser(FIELD_NAME, analyzer);
			Query query = parser.Parse("JF? or Kenned*");
			System.Console.Out.WriteLine("Searching with primitive query");
			//forget to set this and...
			//query=query.rewrite(reader);
			Hits hits = searcher.Search(query);
			
			//create an instance of the highlighter with the tags used to surround highlighted text
			//		QueryHighlightExtractor highlighter = new QueryHighlightExtractor(this, query, new StandardAnalyzer());
			Highlighter highlighter = new Highlighter(this, new QueryScorer(query));
			
			highlighter.SetTextFragmenter(new SimpleFragmenter(40));
			
			int maxNumFragmentsRequired = 3;
			
			for (int i = 0; i < hits.Length(); i++)
			{
				System.String text = hits.Doc(i).Get(FIELD_NAME);
				TokenStream tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(text));
				
				System.String highlightedText = highlighter.GetBestFragments(tokenStream, text, maxNumFragmentsRequired, "...");
				System.Console.Out.WriteLine(highlightedText);
			}
			//We expect to have zero highlights if the query is multi-terms and is not rewritten!
			Assert.IsTrue(numHighlights == 0, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
        [Test]
		public virtual void  TestNoFragments()
		{
			DoSearching("AnInvalidQueryWhichShouldYieldNoResults");
			Highlighter highlighter = new Highlighter(this, new QueryScorer(query));
			
			for (int i = 0; i < texts.Length; i++)
			{
				System.String text = texts[i];
				TokenStream tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(text));
				
				System.String result = highlighter.GetBestFragment(tokenStream, text);
				Assert.IsNull(result, "The highlight result should be null for text with no query terms");
			}
		}
		
		/// <summary> Demonstrates creation of an XHTML compliant doc using new encoding facilities.</summary>
		/// <throws>  Exception </throws>
		[Test]
		public virtual void  TestEncoding()
		{
            Assert.Fail("This test is failing because it has porting issues.");

            // {{Aroush-2.0.0}} Fix me
            /*
			System.String rawDocContent = "\"Smith & sons' prices < 3 and >4\" claims article";
			//run the highlighter on the raw content (scorer does not score any tokens for 
			// highlighting but scores a single fragment for selection
			Highlighter highlighter = new Highlighter(this, new SimpleHTMLEncoder(), new AnonymousClassScorer(this));
			highlighter.SetTextFragmenter(new SimpleFragmenter(2000));
			TokenStream tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(rawDocContent));
			
			System.String encodedSnippet = highlighter.GetBestFragments(tokenStream, rawDocContent, 1, "");
			//An ugly bit of XML creation:
			System.String xhtml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + "<!DOCTYPE html\n" + "PUBLIC \"//W3C//DTD XHTML 1.0 Transitional//EN\"\n" + "\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">\n" + "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\" lang=\"en\">\n" + "<head>\n" + "<title>My Test HTML Document</title>\n" + "</head>\n" + "<body>\n" + "<h2>" + encodedSnippet + "</h2>\n" + "</body>\n" + "</html>";
			//now an ugly built of XML parsing to test the snippet is encoded OK 
			//UPGRADE_ISSUE: Class 'javax.xml.parsers.DocumentBuilderFactory' was not converted. 'ms-help://MS.VSCC.2003/commoner/redir/redirect.htm?keyword="jlca1000_javaxxmlparsersDocumentBuilderFactory_3"'
			//UPGRADE_ISSUE: Method 'javax.xml.parsers.DocumentBuilderFactory.newInstance' was not converted. 'ms-help://MS.VSCC.2003/commoner/redir/redirect.htm?keyword="jlca1000_javaxxmlparsersDocumentBuilderFactory_3"'
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			System.Xml.XmlDocument db = new System.Xml.XmlDocument();
			System.Xml.XmlDocument tempDocument;
			tempDocument = (System.Xml.XmlDocument) db.Clone();
			tempDocument.Load(new System.IO.MemoryStream(System.Text.UTF8Encoding.UTF8.GetBytes(xhtml)));
			System.Xml.XmlDocument doc = tempDocument;
			System.Xml.XmlElement root = (System.Xml.XmlElement) doc.DocumentElement;
			System.Xml.XmlNodeList nodes = root.GetElementsByTagName("body");
			System.Xml.XmlElement body = (System.Xml.XmlElement) nodes.Item(0);
			nodes = body.GetElementsByTagName("h2");
			System.Xml.XmlElement h2 = (System.Xml.XmlElement) nodes.Item(0);
			System.String decodedSnippet = h2.FirstChild.Value;
			Assert.AreEqual(rawDocContent, decodedSnippet, "XHTML Encoding should have worked:");
            */
		}
		
        [Test]
		public virtual void  TestMultiSearcher()
		{
			//setup index 1
			RAMDirectory ramDir1 = new RAMDirectory();
			IndexWriter writer1 = new IndexWriter(ramDir1, new StandardAnalyzer(), true);
			Document d = new Document();
			Field f = new Field(FIELD_NAME, "multiOne", Field.Store.YES, Field.Index.TOKENIZED);
			d.Add(f);
			writer1.AddDocument(d);
			writer1.Optimize();
			writer1.Close();
			IndexReader reader1 = IndexReader.Open(ramDir1);
			
			//setup index 2
			RAMDirectory ramDir2 = new RAMDirectory();
			IndexWriter writer2 = new IndexWriter(ramDir2, new StandardAnalyzer(), true);
			d = new Document();
			f = new Field(FIELD_NAME, "multiTwo", Field.Store.YES, Field.Index.TOKENIZED);
			d.Add(f);
			writer2.AddDocument(d);
			writer2.Optimize();
			writer2.Close();
			IndexReader reader2 = IndexReader.Open(ramDir2);
			
			
			
			IndexSearcher[] searchers = new IndexSearcher[2];
			searchers[0] = new IndexSearcher(ramDir1);
			searchers[1] = new IndexSearcher(ramDir2);
			MultiSearcher multiSearcher = new MultiSearcher(searchers);
			QueryParser parser = new QueryParser(FIELD_NAME, new StandardAnalyzer());
			query = parser.Parse("multi*");
			System.Console.Out.WriteLine("Searching for: " + query.ToString(FIELD_NAME));
			//at this point the multisearcher calls combine(query[])
			hits = multiSearcher.Search(query);
			
			//query = QueryParser.parse("multi*", FIELD_NAME, new StandardAnalyzer());
			Query[] expandedQueries = new Query[2];
			expandedQueries[0] = query.Rewrite(reader1);
			expandedQueries[1] = query.Rewrite(reader2);
			query = query.Combine(expandedQueries);
			
			
			//create an instance of the highlighter with the tags used to surround highlighted text
			Highlighter highlighter = new Highlighter(this, new QueryScorer(query));
			
			for (int i = 0; i < hits.Length(); i++)
			{
				System.String text = hits.Doc(i).Get(FIELD_NAME);
				TokenStream tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(text));
				System.String highlightedText = highlighter.GetBestFragment(tokenStream, text);
				System.Console.Out.WriteLine(highlightedText);
			}
			Assert.IsTrue(numHighlights == 2, "Failed to find correct number of highlights " + numHighlights + " found");
		}
		
        [Test]
		public virtual void  TestFieldSpecificHighlighting()
		{
			System.String docMainText = "fred is one of the people";
			QueryParser parser = new QueryParser(FIELD_NAME, analyzer);
			Query query = parser.Parse("fred category:people");
			
			//highlighting respects fieldnames used in query
			QueryScorer fieldSpecificScorer = new QueryScorer(query, "contents");
			Highlighter fieldSpecificHighlighter = new Highlighter(new SimpleHTMLFormatter(), fieldSpecificScorer);
			fieldSpecificHighlighter.SetTextFragmenter(new NullFragmenter());
			System.String result = fieldSpecificHighlighter.GetBestFragment(analyzer, FIELD_NAME, docMainText);
			Assert.AreEqual(result, "<B>fred</B> is one of the people", "Should match");
			
			//highlighting does not respect fieldnames used in query
			QueryScorer fieldInSpecificScorer = new QueryScorer(query);
			Highlighter fieldInSpecificHighlighter = new Highlighter(new SimpleHTMLFormatter(), fieldInSpecificScorer);
			fieldInSpecificHighlighter.SetTextFragmenter(new NullFragmenter());
			result = fieldInSpecificHighlighter.GetBestFragment(analyzer, FIELD_NAME, docMainText);
			Assert.AreEqual(result, "<B>fred</B> is one of the <B>people</B>", "Should match");
			
			
			reader.Close();
		}
		
		protected internal virtual TokenStream GetTS2()
		{
			//String s = "Hi-Speed10 foo";
			return new AnonymousClassTokenStream(this);
		}
		
		// same token-stream as above, but the bigger token comes first this time
		protected internal virtual TokenStream GetTS2a()
		{
			//String s = "Hi-Speed10 foo";
			return new AnonymousClassTokenStream1(this);
		}
		
        [Test]
		public virtual void  TestOverlapAnalyzer2()
		{
			
			System.String s = "Hi-Speed10 foo";
			
			Query query; Highlighter highlighter; System.String result;
			
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("foo");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2(), s, 3, "...");
			Assert.AreEqual(result, "Hi-Speed10 <B>foo</B>");
			
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("10");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2(), s, 3, "...");
			Assert.AreEqual(result, "Hi-Speed<B>10</B> foo");
			
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("hi");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2(), s, 3, "...");
			Assert.AreEqual(result, "<B>Hi</B>-Speed10 foo");
			
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("speed");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2(), s, 3, "...");
			Assert.AreEqual(result, "Hi-<B>Speed</B>10 foo");
			
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("hispeed");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2(), s, 3, "...");
			Assert.AreEqual(result, "<B>Hi-Speed</B>10 foo");
			
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("hi speed");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2(), s, 3, "...");
			Assert.AreEqual(result, "<B>Hi-Speed</B>10 foo");
			
			/////////////////// same tests, just put the bigger overlapping token first
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("foo");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2a(), s, 3, "...");
			Assert.AreEqual(result, "Hi-Speed10 <B>foo</B>");
			
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("10");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2a(), s, 3, "...");
			Assert.AreEqual(result, "Hi-Speed<B>10</B> foo");
			
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("hi");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2a(), s, 3, "...");
			Assert.AreEqual(result, "<B>Hi</B>-Speed10 foo");
			
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("speed");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2a(), s, 3, "...");
			Assert.AreEqual(result, "Hi-<B>Speed</B>10 foo");
			
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("hispeed");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2a(), s, 3, "...");
			Assert.AreEqual(result, "<B>Hi-Speed</B>10 foo");
			
			query = new QueryParser("text", new WhitespaceAnalyzer()).Parse("hi speed");
			highlighter = new Highlighter(new QueryScorer(query));
			result = highlighter.GetBestFragments(GetTS2a(), s, 3, "...");
			Assert.AreEqual(result, "<B>Hi-Speed</B>10 foo");
		}
		
		
		/*
		
		public void testBigramAnalyzer() throws IOException, ParseException
		{
		//test to ensure analyzers with none-consecutive start/end offsets
		//dont double-highlight text
		//setup index 1
		RAMDirectory ramDir = new RAMDirectory();
		Analyzer bigramAnalyzer=new CJKAnalyzer();
		IndexWriter writer = new IndexWriter(ramDir,bigramAnalyzer , true);
		Document d = new Document();
		Field f = new Field(FIELD_NAME, "java abc def", true, true, true);
		d.add(f);
		writer.addDocument(d);
		writer.close();
		IndexReader reader = IndexReader.open(ramDir);
		
		IndexSearcher searcher=new IndexSearcher(reader);
		query = QueryParser.parse("abc", FIELD_NAME, bigramAnalyzer);
		System.out.println("Searching for: " + query.toString(FIELD_NAME));
		hits = searcher.search(query);
		
		Highlighter highlighter =
		new Highlighter(this,new QueryFragmentScorer(query));
		
		for (int i = 0; i < hits.length(); i++)
		{
		String text = hits.doc(i).get(FIELD_NAME);
		TokenStream tokenStream=bigramAnalyzer.tokenStream(FIELD_NAME,new StringReader(text));
		String highlightedText = highlighter.getBestFragment(tokenStream,text);
		System.out.println(highlightedText);
		}
		
		}*/
		
		
		public virtual System.String HighlightTerm(System.String originalText, TokenGroup group)
		{
			if (group.GetTotalScore() <= 0)
			{
				return originalText;
			}
			numHighlights++; //update stats used in assertions
			return "<b>" + originalText + "</b>";
		}
		
		public virtual void  DoSearching(System.String queryString)
		{
			QueryParser parser = new QueryParser(FIELD_NAME, new StandardAnalyzer());
			query = parser.Parse(queryString);
			DoSearching(query);
		}
		public virtual void  DoSearching(Query unReWrittenQuery)
		{
			searcher = new IndexSearcher(ramDir);
			//for any multi-term queries to work (prefix, wildcard, range,fuzzy etc) you must use a rewritten query!
			query = unReWrittenQuery.Rewrite(reader);
			System.Console.Out.WriteLine("Searching for: " + query.ToString(FIELD_NAME));
			hits = searcher.Search(query);
		}
		
		internal virtual void  DoStandardHighlights()
		{
			Highlighter highlighter = new Highlighter(this, new QueryScorer(query));
			highlighter.SetTextFragmenter(new SimpleFragmenter(20));
			for (int i = 0; i < hits.Length(); i++)
			{
				System.String text = hits.Doc(i).Get(FIELD_NAME);
				int maxNumFragmentsRequired = 2;
				System.String fragmentSeparator = "...";
				TokenStream tokenStream = analyzer.TokenStream(FIELD_NAME, new System.IO.StringReader(text));
				
				System.String result = highlighter.GetBestFragments(tokenStream, text, maxNumFragmentsRequired, fragmentSeparator);
				System.Console.Out.WriteLine("\t" + result);
			}
		}
		
		/*
		* @see TestCase#setUp()
		*/
		[SetUp]
        protected virtual void  SetUp()
		{
			ramDir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(ramDir, new StandardAnalyzer(), true);
			for (int i = 0; i < texts.Length; i++)
			{
				AddDoc(writer, texts[i]);
			}
			
			writer.Optimize();
			writer.Close();
			reader = IndexReader.Open(ramDir);
			numHighlights = 0;
		}
		
		private void  AddDoc(IndexWriter writer, System.String text)
		{
			Document d = new Document();
			Field f = new Field(FIELD_NAME, text, Field.Store.YES, Field.Index.TOKENIZED);
			d.Add(f);
			writer.AddDocument(d);
		}
		
		/*
		* @see TestCase#tearDown()
		*/
        [TearDown]
		protected virtual void  TearDown()
		{
			//base.TearDown();
		}
	}
	
	
	//===================================================================
	//========== BEGIN TEST SUPPORTING CLASSES
	//========== THESE LOOK LIKE, WITH SOME MORE EFFORT THESE COULD BE
	//========== MADE MORE GENERALLY USEFUL.
	// TODO - make synonyms all interchangeable with each other and produce
	// a version that does hyponyms - the "is a specialised type of ...."
	// so that car = audi, bmw and volkswagen but bmw != audi so different
	// behaviour to synonyms
	//===================================================================
	
	class SynonymAnalyzer : Analyzer
	{
		private System.Collections.IDictionary synonyms;
		
		public SynonymAnalyzer(System.Collections.IDictionary synonyms)
		{
			this.synonyms = synonyms;
		}
		
		/* (non-Javadoc)
		* @see org.apache.lucene.analysis.Analyzer#tokenStream(java.lang.String, java.io.Reader)
		*/
		public override TokenStream TokenStream(System.String arg0, System.IO.TextReader arg1)
		{
			return new SynonymTokenizer(new LowerCaseTokenizer(arg1), synonyms);
		}
	}
	
	/// <summary> Expands a token stream with synonyms (TODO - make the synonyms analyzed by choice of analyzer)</summary>
	/// <author>  MAHarwood
	/// </author>
	class SynonymTokenizer : TokenStream
	{
		private TokenStream realStream;
		private Token currentRealToken = null;
		private System.Collections.IDictionary synonyms;
		internal Tokenizer st = null;
		public SynonymTokenizer(TokenStream realStream, System.Collections.IDictionary synonyms)
		{
			this.realStream = realStream;
			this.synonyms = synonyms;
		}
		public override Token Next()
		{
			if (currentRealToken == null)
			{
				Token nextRealToken = realStream.Next();
				if (nextRealToken == null)
				{
					return null;
				}
				System.String expansions = (System.String) synonyms[nextRealToken.TermText()];
				if (expansions == null)
				{
					return nextRealToken;
				}
				st = new Tokenizer(expansions, ",");
				if (st.HasMoreTokens())
				{
					currentRealToken = nextRealToken;
				}
				return currentRealToken;
			}
			else
			{
				System.String nextExpandedValue = st.NextToken();
				Token expandedToken = new Token(nextExpandedValue, currentRealToken.StartOffset(), currentRealToken.EndOffset());
				expandedToken.SetPositionIncrement(0);
				if (!st.HasMoreTokens())
				{
					currentRealToken = null;
					st = null;
				}
				return expandedToken;
			}
		}
	}

    /// <summary>
    /// The class performs token processing in strings
    /// </summary>
    public class Tokenizer : System.Collections.IEnumerator
    {
        /// Position over the string
        private long currentPos = 0;

        /// Include demiliters in the results.
        private bool includeDelims = false;

        /// Char representation of the String to tokenize.
        private char[] chars = null;
			
        //The tokenizer uses the default delimiter set: the space character, the tab character, the newline character, and the carriage-return character and the form-feed character
        private string delimiters = " \t\n\r\f";		

        /// <summary>
        /// Initializes a new class instance with a specified string to process
        /// </summary>
        /// <param name="source">String to tokenize</param>
        public Tokenizer(System.String source)
        {			
            this.chars = source.ToCharArray();
        }

        /// <summary>
        /// Initializes a new class instance with a specified string to process
        /// and the specified token delimiters to use
        /// </summary>
        /// <param name="source">String to tokenize</param>
        /// <param name="delimiters">String containing the delimiters</param>
        public Tokenizer(System.String source, System.String delimiters):this(source)
        {			
            this.delimiters = delimiters;
        }


        /// <summary>
        /// Initializes a new class instance with a specified string to process, the specified token 
        /// delimiters to use, and whether the delimiters must be included in the results.
        /// </summary>
        /// <param name="source">String to tokenize</param>
        /// <param name="delimiters">String containing the delimiters</param>
        /// <param name="includeDelims">Determines if delimiters are included in the results.</param>
        public Tokenizer(System.String source, System.String delimiters, bool includeDelims):this(source,delimiters)
        {
            this.includeDelims = includeDelims;
        }	


        /// <summary>
        /// Returns the next token from the token list
        /// </summary>
        /// <returns>The string value of the token</returns>
        public System.String NextToken()
        {				
            return NextToken(this.delimiters);
        }

        /// <summary>
        /// Returns the next token from the source string, using the provided
        /// token delimiters
        /// </summary>
        /// <param name="delimiters">String containing the delimiters to use</param>
        /// <returns>The string value of the token</returns>
        public System.String NextToken(System.String delimiters)
        {
            //According to documentation, the usage of the received delimiters should be temporary (only for this call).
            //However, it seems it is not true, so the following line is necessary.
            this.delimiters = delimiters;

            //at the end 
            if (this.currentPos == this.chars.Length)
                throw new System.ArgumentOutOfRangeException();
                //if over a delimiter and delimiters must be returned
            else if (   (System.Array.IndexOf(delimiters.ToCharArray(),chars[this.currentPos]) != -1)
                && this.includeDelims )                	
                return "" + this.chars[this.currentPos++];
                //need to get the token wo delimiters.
            else
                return nextToken(delimiters.ToCharArray());
        }

        //Returns the nextToken wo delimiters
        private System.String nextToken(char[] delimiters)
        {
            string token="";
            long pos = this.currentPos;

            //skip possible delimiters
            while (System.Array.IndexOf(delimiters,this.chars[currentPos]) != -1)
                //The last one is a delimiter (i.e there is no more tokens)
                if (++this.currentPos == this.chars.Length)
                {
                    this.currentPos = pos;
                    throw new System.ArgumentOutOfRangeException();
                }
			
            //getting the token
            while (System.Array.IndexOf(delimiters,this.chars[this.currentPos]) == -1)
            {
                token+=this.chars[this.currentPos];
                //the last one is not a delimiter
                if (++this.currentPos == this.chars.Length)
                    break;
            }
            return token;
        }

				
        /// <summary>
        /// Determines if there are more tokens to return from the source string
        /// </summary>
        /// <returns>True or false, depending if there are more tokens</returns>
        public bool HasMoreTokens()
        {
            //keeping the current pos
            long pos = this.currentPos;
			
            try
            {
                this.NextToken();
            }
            catch (System.ArgumentOutOfRangeException)
            {				
                return false;
            }
            finally
            {
                this.currentPos = pos;
            }
            return true;
        }

        /// <summary>
        /// Remaining tokens count
        /// </summary>
        public int Count
        {
            get
            {
                //keeping the current pos
                long pos = this.currentPos;
                int i = 0;
			
                try
                {
                    while (true)
                    {
                        this.NextToken();
                        i++;
                    }
                }
                catch (System.ArgumentOutOfRangeException)
                {				
                    this.currentPos = pos;
                    return i;
                }
            }
        }

        /// <summary>
        ///  Performs the same action as NextToken.
        /// </summary>
        public System.Object Current
        {
            get
            {
                return (Object) this.NextToken();
            }		
        }		
		
        /// <summary>
        //  Performs the same action as HasMoreTokens.
        /// </summary>
        /// <returns>True or false, depending if there are more tokens</returns>
        public bool MoveNext()
        {
            return this.HasMoreTokens();
        }
		
        /// <summary>
        /// Does nothing.
        /// </summary>
        public void  Reset()
        {
            ;
        }			
    }
}
