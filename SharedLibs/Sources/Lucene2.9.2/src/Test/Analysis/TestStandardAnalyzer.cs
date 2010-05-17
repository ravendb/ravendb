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
using OffsetAttribute = Lucene.Net.Analysis.Tokenattributes.OffsetAttribute;
using PositionIncrementAttribute = Lucene.Net.Analysis.Tokenattributes.PositionIncrementAttribute;
using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using TypeAttribute = Lucene.Net.Analysis.Tokenattributes.TypeAttribute;
using Version = Lucene.Net.Util.Version;

namespace Lucene.Net.Analysis
{
	
    [TestFixture]
	public class TestStandardAnalyzer:BaseTokenStreamTestCase
	{
		
		private Analyzer a = new StandardAnalyzer();
		
        [Test]
		public virtual void  TestMaxTermLength()
		{
			StandardAnalyzer sa = new StandardAnalyzer();
			sa.SetMaxTokenLength(5);
			AssertAnalyzesTo(sa, "ab cd toolong xy z", new System.String[]{"ab", "cd", "xy", "z"});
		}
		
        [Test]
		public virtual void  TestMaxTermLength2()
		{
			StandardAnalyzer sa = new StandardAnalyzer();
			AssertAnalyzesTo(sa, "ab cd toolong xy z", new System.String[]{"ab", "cd", "toolong", "xy", "z"});
			sa.SetMaxTokenLength(5);
			
			AssertAnalyzesTo(sa, "ab cd toolong xy z", new System.String[]{"ab", "cd", "xy", "z"}, new int[]{1, 1, 2, 1});
		}
		
        [Test]
		public virtual void  TestMaxTermLength3()
		{
			char[] chars = new char[255];
			for (int i = 0; i < 255; i++)
				chars[i] = 'a';
			System.String longTerm = new System.String(chars, 0, 255);
			
			AssertAnalyzesTo(a, "ab cd " + longTerm + " xy z", new System.String[]{"ab", "cd", longTerm, "xy", "z"});
			AssertAnalyzesTo(a, "ab cd " + longTerm + "a xy z", new System.String[]{"ab", "cd", "xy", "z"});
		}
		
        [Test]
		public virtual void  TestAlphanumeric()
		{
			// alphanumeric tokens
			AssertAnalyzesTo(a, "B2B", new System.String[]{"b2b"});
			AssertAnalyzesTo(a, "2B", new System.String[]{"2b"});
		}
		
        [Test]
		public virtual void  TestUnderscores()
		{
			// underscores are delimiters, but not in email addresses (below)
			AssertAnalyzesTo(a, "word_having_underscore", new System.String[]{"word", "having", "underscore"});
			AssertAnalyzesTo(a, "word_with_underscore_and_stopwords", new System.String[]{"word", "underscore", "stopwords"});
		}
		
        [Test]
		public virtual void  TestDelimiters()
		{
			// other delimiters: "-", "/", ","
			AssertAnalyzesTo(a, "some-dashed-phrase", new System.String[]{"some", "dashed", "phrase"});
			AssertAnalyzesTo(a, "dogs,chase,cats", new System.String[]{"dogs", "chase", "cats"});
			AssertAnalyzesTo(a, "ac/dc", new System.String[]{"ac", "dc"});
		}
		
        [Test]
		public virtual void  TestApostrophes()
		{
			// internal apostrophes: O'Reilly, you're, O'Reilly's
			// possessives are actually removed by StardardFilter, not the tokenizer
			AssertAnalyzesTo(a, "O'Reilly", new System.String[]{"o'reilly"});
			AssertAnalyzesTo(a, "you're", new System.String[]{"you're"});
			AssertAnalyzesTo(a, "she's", new System.String[]{"she"});
			AssertAnalyzesTo(a, "Jim's", new System.String[]{"jim"});
			AssertAnalyzesTo(a, "don't", new System.String[]{"don't"});
			AssertAnalyzesTo(a, "O'Reilly's", new System.String[]{"o'reilly"});
		}
		
        [Test]
		public virtual void  TestTSADash()
		{
			// t and s had been stopwords in Lucene <= 2.0, which made it impossible
			// to correctly search for these terms:
			AssertAnalyzesTo(a, "s-class", new System.String[]{"s", "class"});
			AssertAnalyzesTo(a, "t-com", new System.String[]{"t", "com"});
			// 'a' is still a stopword:
			AssertAnalyzesTo(a, "a-class", new System.String[]{"class"});
		}
		
        [Test]
		public virtual void  TestCompanyNames()
		{
			// company names
			AssertAnalyzesTo(a, "AT&T", new System.String[]{"at&t"});
			AssertAnalyzesTo(a, "Excite@Home", new System.String[]{"excite@home"});
		}
		
        [Test]
		public virtual void  TestLucene1140()
		{
			try
			{
				StandardAnalyzer analyzer = new StandardAnalyzer(true);
				AssertAnalyzesTo(analyzer, "www.nutch.org.", new System.String[]{"www.nutch.org"}, new System.String[]{"<HOST>"});
			}
			catch (System.NullReferenceException e)
			{
				Assert.IsTrue(false, "Should not throw an NPE and it did");
			}
		}
		
        [Test]
		public virtual void  TestDomainNames()
		{
			// Don't reuse a because we alter its state
			// (setReplaceInvalidAcronym)
			
			// Current lucene should not show the bug
			StandardAnalyzer a2 = new StandardAnalyzer(Version.LUCENE_CURRENT);
			// domain names
			AssertAnalyzesTo(a2, "www.nutch.org", new System.String[]{"www.nutch.org"});
			//Notice the trailing .  See https://issues.apache.org/jira/browse/LUCENE-1068.
			// the following should be recognized as HOST:
			AssertAnalyzesTo(a2, "www.nutch.org.", new System.String[]{"www.nutch.org"}, new System.String[]{"<HOST>"});
			
			// 2.3 should show the bug
			a2 = new StandardAnalyzer(Version.LUCENE_23);
			AssertAnalyzesTo(a2, "www.nutch.org.", new System.String[]{"wwwnutchorg"}, new System.String[]{"<ACRONYM>"});
			
			// 2.4 should not show the bug
			a2 = new StandardAnalyzer(Version.LUCENE_24);
			AssertAnalyzesTo(a2, "www.nutch.org.", new System.String[]{"www.nutch.org"}, new System.String[]{"<HOST>"});
		}
		
        [Test]
		public virtual void  TestEMailAddresses()
		{
			// email addresses, possibly with underscores, periods, etc
			AssertAnalyzesTo(a, "test@example.com", new System.String[]{"test@example.com"});
			AssertAnalyzesTo(a, "first.lastname@example.com", new System.String[]{"first.lastname@example.com"});
			AssertAnalyzesTo(a, "first_lastname@example.com", new System.String[]{"first_lastname@example.com"});
		}
		
        [Test]
		public virtual void  TestNumeric()
		{
			// floating point, serial, model numbers, ip addresses, etc.
			// every other segment must have at least one digit
			AssertAnalyzesTo(a, "21.35", new System.String[]{"21.35"});
			AssertAnalyzesTo(a, "R2D2 C3PO", new System.String[]{"r2d2", "c3po"});
			AssertAnalyzesTo(a, "216.239.63.104", new System.String[]{"216.239.63.104"});
			AssertAnalyzesTo(a, "1-2-3", new System.String[]{"1-2-3"});
			AssertAnalyzesTo(a, "a1-b2-c3", new System.String[]{"a1-b2-c3"});
			AssertAnalyzesTo(a, "a1-b-c3", new System.String[]{"a1-b-c3"});
		}
		
        [Test]
		public virtual void  TestTextWithNumbers()
		{
			// numbers
			AssertAnalyzesTo(a, "David has 5000 bones", new System.String[]{"david", "has", "5000", "bones"});
		}
		
        [Test]
		public virtual void  TestVariousText()
		{
			// various
			AssertAnalyzesTo(a, "C embedded developers wanted", new System.String[]{"c", "embedded", "developers", "wanted"});
			AssertAnalyzesTo(a, "foo bar FOO BAR", new System.String[]{"foo", "bar", "foo", "bar"});
			AssertAnalyzesTo(a, "foo      bar .  FOO <> BAR", new System.String[]{"foo", "bar", "foo", "bar"});
			AssertAnalyzesTo(a, "\"QUOTED\" word", new System.String[]{"quoted", "word"});
		}
		
        [Test]
		public virtual void  TestAcronyms()
		{
			// acronyms have their dots stripped
			AssertAnalyzesTo(a, "U.S.A.", new System.String[]{"usa"});
		}
		
        [Test]
		public virtual void  TestCPlusPlusHash()
		{
			// It would be nice to change the grammar in StandardTokenizer.jj to make "C#" and "C++" end up as tokens.
			AssertAnalyzesTo(a, "C++", new System.String[]{"c"});
			AssertAnalyzesTo(a, "C#", new System.String[]{"c"});
		}
		
        [Test]
		public virtual void  TestKorean()
		{
			// Korean words
			AssertAnalyzesTo(a, "안녕하세요 한글입니다", new System.String[]{"안녕하세요", "한글입니다"});
		}
		
		// Compliance with the "old" JavaCC-based analyzer, see:
		// https://issues.apache.org/jira/browse/LUCENE-966#action_12516752
		
        [Test]
		public virtual void  TestComplianceFileName()
		{
			AssertAnalyzesTo(a, "2004.jpg", new System.String[]{"2004.jpg"}, new System.String[]{"<HOST>"});
		}
		
        [Test]
		public virtual void  TestComplianceNumericIncorrect()
		{
			AssertAnalyzesTo(a, "62.46", new System.String[]{"62.46"}, new System.String[]{"<HOST>"});
		}
		
        [Test]
		public virtual void  TestComplianceNumericLong()
		{
			AssertAnalyzesTo(a, "978-0-94045043-1", new System.String[]{"978-0-94045043-1"}, new System.String[]{"<NUM>"});
		}
		
        [Test]
		public virtual void  TestComplianceNumericFile()
		{
			AssertAnalyzesTo(a, "78academyawards/rules/rule02.html", new System.String[]{"78academyawards/rules/rule02.html"}, new System.String[]{"<NUM>"});
		}
		
        [Test]
		public virtual void  TestComplianceNumericWithUnderscores()
		{
			AssertAnalyzesTo(a, "2006-03-11t082958z_01_ban130523_rtridst_0_ozabs", new System.String[]{"2006-03-11t082958z_01_ban130523_rtridst_0_ozabs"}, new System.String[]{"<NUM>"});
		}
		
        [Test]
		public virtual void  TestComplianceNumericWithDash()
		{
			AssertAnalyzesTo(a, "mid-20th", new System.String[]{"mid-20th"}, new System.String[]{"<NUM>"});
		}
		
        [Test]
		public virtual void  TestComplianceManyTokens()
		{
			AssertAnalyzesTo(a, "/money.cnn.com/magazines/fortune/fortune_archive/2007/03/19/8402357/index.htm " + "safari-0-sheikh-zayed-grand-mosque.jpg", new System.String[]{"money.cnn.com", "magazines", "fortune", "fortune", "archive/2007/03/19/8402357", "index.htm", "safari-0-sheikh", "zayed", "grand", "mosque.jpg"}, new System.String[]{"<HOST>", "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", "<NUM>", "<HOST>", "<NUM>", "<ALPHANUM>", "<ALPHANUM>", "<HOST>"});
		}
		
		/// <deprecated> this should be removed in the 3.0. 
		/// </deprecated>
        [Test]
		public virtual void  TestDeprecatedAcronyms()
		{
			// test backward compatibility for applications that require the old behavior.
			// this should be removed once replaceDepAcronym is removed.
			AssertAnalyzesTo(a, "lucene.apache.org.", new System.String[]{"lucene.apache.org"}, new System.String[]{"<HOST>"});
		}
	}
}