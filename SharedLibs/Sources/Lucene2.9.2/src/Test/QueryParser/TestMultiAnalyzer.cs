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
using LowerCaseFilter = Lucene.Net.Analysis.LowerCaseFilter;
using TokenFilter = Lucene.Net.Analysis.TokenFilter;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using StandardTokenizer = Lucene.Net.Analysis.Standard.StandardTokenizer;
using OffsetAttribute = Lucene.Net.Analysis.Tokenattributes.OffsetAttribute;
using PositionIncrementAttribute = Lucene.Net.Analysis.Tokenattributes.PositionIncrementAttribute;
using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using TypeAttribute = Lucene.Net.Analysis.Tokenattributes.TypeAttribute;
using BaseTokenStreamTestCase = Lucene.Net.Analysis.BaseTokenStreamTestCase;
using Query = Lucene.Net.Search.Query;

namespace Lucene.Net.QueryParsers
{
	
	/// <summary> Test QueryParser's ability to deal with Analyzers that return more
	/// than one token per position or that return tokens with a position
	/// increment &gt; 1.
	/// 
	/// </summary>
    [TestFixture]
	public class TestMultiAnalyzer:BaseTokenStreamTestCase
	{
		
		private static int multiToken = 0;
		
		[Test]
		public virtual void  TestMultiAnalyzer_Rename()
		{
			
			QueryParser qp = new QueryParser("", new MultiAnalyzer(this));
			
			// trivial, no multiple tokens:
			Assert.AreEqual("foo", qp.Parse("foo").ToString());
			Assert.AreEqual("foo", qp.Parse("\"foo\"").ToString());
			Assert.AreEqual("foo foobar", qp.Parse("foo foobar").ToString());
			Assert.AreEqual("\"foo foobar\"", qp.Parse("\"foo foobar\"").ToString());
			Assert.AreEqual("\"foo foobar blah\"", qp.Parse("\"foo foobar blah\"").ToString());
			
			// two tokens at the same position:
			Assert.AreEqual("(multi multi2) foo", qp.Parse("multi foo").ToString());
			Assert.AreEqual("foo (multi multi2)", qp.Parse("foo multi").ToString());
			Assert.AreEqual("(multi multi2) (multi multi2)", qp.Parse("multi multi").ToString());
			Assert.AreEqual("+(foo (multi multi2)) +(bar (multi multi2))", qp.Parse("+(foo multi) +(bar multi)").ToString());
			Assert.AreEqual("+(foo (multi multi2)) field:\"bar (multi multi2)\"", qp.Parse("+(foo multi) field:\"bar multi\"").ToString());
			
			// phrases:
			Assert.AreEqual("\"(multi multi2) foo\"", qp.Parse("\"multi foo\"").ToString());
			Assert.AreEqual("\"foo (multi multi2)\"", qp.Parse("\"foo multi\"").ToString());
			Assert.AreEqual("\"foo (multi multi2) foobar (multi multi2)\"", qp.Parse("\"foo multi foobar multi\"").ToString());
			
			// fields:
			Assert.AreEqual("(field:multi field:multi2) field:foo", qp.Parse("field:multi field:foo").ToString());
			Assert.AreEqual("field:\"(multi multi2) foo\"", qp.Parse("field:\"multi foo\"").ToString());
			
			// three tokens at one position:
			Assert.AreEqual("triplemulti multi3 multi2", qp.Parse("triplemulti").ToString());
			Assert.AreEqual("foo (triplemulti multi3 multi2) foobar", qp.Parse("foo triplemulti foobar").ToString());
			
			// phrase with non-default slop:
			Assert.AreEqual("\"(multi multi2) foo\"~10", qp.Parse("\"multi foo\"~10").ToString());
			
			// phrase with non-default boost:
			Assert.AreEqual("\"(multi multi2) foo\"^2.0", qp.Parse("\"multi foo\"^2").ToString());
			
			// phrase after changing default slop
			qp.SetPhraseSlop(99);
			Assert.AreEqual("\"(multi multi2) foo\"~99 bar", qp.Parse("\"multi foo\" bar").ToString());
			Assert.AreEqual("\"(multi multi2) foo\"~99 \"foo bar\"~2", qp.Parse("\"multi foo\" \"foo bar\"~2").ToString());
			qp.SetPhraseSlop(0);
			
			// non-default operator:
			qp.SetDefaultOperator(QueryParser.AND_OPERATOR);
			Assert.AreEqual("+(multi multi2) +foo", qp.Parse("multi foo").ToString());
		}
		
		[Test]
		public virtual void  TestMultiAnalyzerWithSubclassOfQueryParser()
		{
			
			DumbQueryParser qp = new DumbQueryParser("", new MultiAnalyzer(this));
			qp.SetPhraseSlop(99); // modified default slop
			
			// direct call to (super's) getFieldQuery to demonstrate differnce
			// between phrase and multiphrase with modified default slop
			Assert.AreEqual("\"foo bar\"~99", qp.GetSuperFieldQuery("", "foo bar").ToString());
			Assert.AreEqual("\"(multi multi2) bar\"~99", qp.GetSuperFieldQuery("", "multi bar").ToString());
			
			
			// ask sublcass to parse phrase with modified default slop
			Assert.AreEqual("\"(multi multi2) foo\"~99 bar", qp.Parse("\"multi foo\" bar").ToString());
		}
		
		[Test]
		public virtual void  TestPosIncrementAnalyzer()
		{
			QueryParser qp = new QueryParser("", new PosIncrementAnalyzer(this));
			Assert.AreEqual("quick brown", qp.Parse("the quick brown").ToString());
			Assert.AreEqual("\"quick brown\"", qp.Parse("\"the quick brown\"").ToString());
			Assert.AreEqual("quick brown fox", qp.Parse("the quick brown fox").ToString());
			Assert.AreEqual("\"quick brown fox\"", qp.Parse("\"the quick brown fox\"").ToString());
		}
		
		/// <summary> Expands "multi" to "multi" and "multi2", both at the same position,
		/// and expands "triplemulti" to "triplemulti", "multi3", and "multi2".  
		/// </summary>
		private class MultiAnalyzer:Analyzer
		{
			private void  InitBlock(TestMultiAnalyzer enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestMultiAnalyzer enclosingInstance;
			public TestMultiAnalyzer Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public MultiAnalyzer(TestMultiAnalyzer enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				TokenStream result = new StandardTokenizer(reader);
				result = new TestFilter(enclosingInstance, result);
				result = new LowerCaseFilter(result);
				return result;
			}
		}
		
		private sealed class TestFilter:TokenFilter
		{
			private void  InitBlock(TestMultiAnalyzer enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestMultiAnalyzer enclosingInstance;
			public TestMultiAnalyzer Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			private System.String prevType;
			private int prevStartOffset;
			private int prevEndOffset;
			
			internal TermAttribute termAtt;
			internal PositionIncrementAttribute posIncrAtt;
			internal OffsetAttribute offsetAtt;
			internal TypeAttribute typeAtt;
			
			public TestFilter(TestMultiAnalyzer enclosingInstance, TokenStream in_Renamed):base(in_Renamed)
			{
				InitBlock(enclosingInstance);
				termAtt = (TermAttribute) AddAttribute(typeof(TermAttribute));
				posIncrAtt = (PositionIncrementAttribute) AddAttribute(typeof(PositionIncrementAttribute));
				offsetAtt = (OffsetAttribute) AddAttribute(typeof(OffsetAttribute));
				typeAtt = (TypeAttribute) AddAttribute(typeof(TypeAttribute));
			}
			
			public override bool IncrementToken()
			{
				if (Lucene.Net.QueryParsers.TestMultiAnalyzer.multiToken > 0)
				{
					termAtt.SetTermBuffer("multi" + (Lucene.Net.QueryParsers.TestMultiAnalyzer.multiToken + 1));
					offsetAtt.SetOffset(prevStartOffset, prevEndOffset);
					typeAtt.SetType(prevType);
					posIncrAtt.SetPositionIncrement(0);
					Lucene.Net.QueryParsers.TestMultiAnalyzer.multiToken--;
					return true;
				}
				else
				{
					bool next = input.IncrementToken();
					if (next == false)
					{
						return false;
					}
					prevType = typeAtt.Type();
					prevStartOffset = offsetAtt.StartOffset();
					prevEndOffset = offsetAtt.EndOffset();
					System.String text = termAtt.Term();
					if (text.Equals("triplemulti"))
					{
						Lucene.Net.QueryParsers.TestMultiAnalyzer.multiToken = 2;
						return true;
					}
					else if (text.Equals("multi"))
					{
						Lucene.Net.QueryParsers.TestMultiAnalyzer.multiToken = 1;
						return true;
					}
					else
					{
						return true;
					}
				}
			}
		}
		
		/// <summary> Analyzes "the quick brown" as: quick(incr=2) brown(incr=1).
		/// Does not work correctly for input other than "the quick brown ...".
		/// </summary>
		private class PosIncrementAnalyzer:Analyzer
		{
			private void  InitBlock(TestMultiAnalyzer enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestMultiAnalyzer enclosingInstance;
			public TestMultiAnalyzer Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public PosIncrementAnalyzer(TestMultiAnalyzer enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				TokenStream result = new StandardTokenizer(reader);
				result = new TestPosIncrementFilter(enclosingInstance, result);
				result = new LowerCaseFilter(result);
				return result;
			}
		}
		
		private sealed class TestPosIncrementFilter:TokenFilter
		{
			private void  InitBlock(TestMultiAnalyzer enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestMultiAnalyzer enclosingInstance;
			public TestMultiAnalyzer Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			internal TermAttribute termAtt;
			internal PositionIncrementAttribute posIncrAtt;
			
			public TestPosIncrementFilter(TestMultiAnalyzer enclosingInstance, TokenStream in_Renamed):base(in_Renamed)
			{
				InitBlock(enclosingInstance);
				termAtt = (TermAttribute) AddAttribute(typeof(TermAttribute));
				posIncrAtt = (PositionIncrementAttribute) AddAttribute(typeof(PositionIncrementAttribute));
			}
			
			public override bool IncrementToken()
			{
				while (input.IncrementToken())
				{
					if (termAtt.Term().Equals("the"))
					{
						// stopword, do nothing
					}
					else if (termAtt.Term().Equals("quick"))
					{
						posIncrAtt.SetPositionIncrement(2);
						return true;
					}
					else
					{
						posIncrAtt.SetPositionIncrement(1);
						return true;
					}
				}
				return false;
			}
		}
		
		/// <summary>a very simple subclass of QueryParser </summary>
		private sealed class DumbQueryParser:QueryParser
		{
			
			public DumbQueryParser(System.String f, Analyzer a):base(f, a)
			{
			}
			
			/// <summary>expose super's version </summary>
			public Query GetSuperFieldQuery(System.String f, System.String t)
			{
				return base.GetFieldQuery(f, t);
			}
			/// <summary>wrap super's version </summary>
			public /*protected internal*/ override Query GetFieldQuery(System.String f, System.String t)
			{
				return new DumbQueryWrapper(GetSuperFieldQuery(f, t));
			}
		}
		
		/// <summary> A very simple wrapper to prevent instanceof checks but uses
		/// the toString of the query it wraps.
		/// </summary>
		[Serializable]
		private sealed class DumbQueryWrapper:Query
		{
			
			private Query q;
			public DumbQueryWrapper(Query q):base()
			{
				this.q = q;
			}
			public override System.String ToString(System.String f)
			{
				return q.ToString(f);
			}
		}
	}
}