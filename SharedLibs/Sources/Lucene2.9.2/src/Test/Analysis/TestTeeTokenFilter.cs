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

using StandardFilter = Lucene.Net.Analysis.Standard.StandardFilter;
using StandardTokenizer = Lucene.Net.Analysis.Standard.StandardTokenizer;
using English = Lucene.Net.Util.English;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Analysis
{
	
	/// <summary> tests for the TeeTokenFilter and SinkTokenizer</summary>
    [TestFixture]
	public class TestTeeTokenFilter:LuceneTestCase
	{
		private class AnonymousClassSinkTokenizer:SinkTokenizer
		{
			private void  InitBlock(TestTeeTokenFilter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTeeTokenFilter enclosingInstance;
			public TestTeeTokenFilter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassSinkTokenizer(TestTeeTokenFilter enclosingInstance, System.Collections.IList Param1):base(Param1)
			{
				InitBlock(enclosingInstance);
			}
			public override void  Add(Token t)
			{
				if (t != null && t.Term().ToUpper().Equals("The".ToUpper()))
				{
					base.Add(t);
				}
			}
		}
		private class AnonymousClassSinkTokenizer1:SinkTokenizer
		{
			private void  InitBlock(TestTeeTokenFilter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTeeTokenFilter enclosingInstance;
			public TestTeeTokenFilter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassSinkTokenizer1(TestTeeTokenFilter enclosingInstance, System.Collections.IList Param1):base(Param1)
			{
				InitBlock(enclosingInstance);
			}
			public override void  Add(Token t)
			{
				if (t != null && t.Term().ToUpper().Equals("The".ToUpper()))
				{
					base.Add(t);
				}
			}
		}
		private class AnonymousClassSinkTokenizer2:SinkTokenizer
		{
			private void  InitBlock(TestTeeTokenFilter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTeeTokenFilter enclosingInstance;
			public TestTeeTokenFilter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassSinkTokenizer2(TestTeeTokenFilter enclosingInstance, System.Collections.IList Param1):base(Param1)
			{
				InitBlock(enclosingInstance);
			}
			public override void  Add(Token t)
			{
				if (t != null && t.Term().ToUpper().Equals("Dogs".ToUpper()))
				{
					base.Add(t);
				}
			}
		}
		protected internal System.Text.StringBuilder buffer1;
		protected internal System.Text.StringBuilder buffer2;
		protected internal System.String[] tokens1;
		protected internal System.String[] tokens2;
		
		
		public TestTeeTokenFilter(System.String s):base(s)
		{
		}

        public TestTeeTokenFilter()
        {
        }
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			tokens1 = new System.String[]{"The", "quick", "Burgundy", "Fox", "jumped", "over", "the", "lazy", "Red", "Dogs"};
			tokens2 = new System.String[]{"The", "Lazy", "Dogs", "should", "stay", "on", "the", "porch"};
			buffer1 = new System.Text.StringBuilder();
			
			for (int i = 0; i < tokens1.Length; i++)
			{
				buffer1.Append(tokens1[i]).Append(' ');
			}
			buffer2 = new System.Text.StringBuilder();
			for (int i = 0; i < tokens2.Length; i++)
			{
				buffer2.Append(tokens2[i]).Append(' ');
			}
		}
		
		[Test]
		public virtual void  Test()
		{
			
			SinkTokenizer sink1 = new AnonymousClassSinkTokenizer(this, null);
			TokenStream source = new TeeTokenFilter(new WhitespaceTokenizer(new System.IO.StringReader(buffer1.ToString())), sink1);
			int i = 0;
			Token reusableToken = new Token();
			for (Token nextToken = source.Next(reusableToken); nextToken != null; nextToken = source.Next(reusableToken))
			{
				Assert.IsTrue(nextToken.Term().Equals(tokens1[i]) == true, nextToken.Term() + " is not equal to " + tokens1[i]);
				i++;
			}
			Assert.IsTrue(i == tokens1.Length, i + " does not equal: " + tokens1.Length);
			Assert.IsTrue(sink1.GetTokens().Count == 2, "sink1 Size: " + sink1.GetTokens().Count + " is not: " + 2);
			i = 0;
			for (Token token = sink1.Next(reusableToken); token != null; token = sink1.Next(reusableToken))
			{
				Assert.IsTrue(token.Term().ToUpper().Equals("The".ToUpper()) == true, token.Term() + " is not equal to " + "The");
				i++;
			}
			Assert.IsTrue(i == sink1.GetTokens().Count, i + " does not equal: " + sink1.GetTokens().Count);
		}
		
		[Test]
		public virtual void  TestMultipleSources()
		{
			SinkTokenizer theDetector = new AnonymousClassSinkTokenizer1(this, null);
			SinkTokenizer dogDetector = new AnonymousClassSinkTokenizer2(this, null);
			TokenStream source1 = new CachingTokenFilter(new TeeTokenFilter(new TeeTokenFilter(new WhitespaceTokenizer(new System.IO.StringReader(buffer1.ToString())), theDetector), dogDetector));
			TokenStream source2 = new TeeTokenFilter(new TeeTokenFilter(new WhitespaceTokenizer(new System.IO.StringReader(buffer2.ToString())), theDetector), dogDetector);
			int i = 0;
			Token reusableToken = new Token();
			for (Token nextToken = source1.Next(reusableToken); nextToken != null; nextToken = source1.Next(reusableToken))
			{
				Assert.IsTrue(nextToken.Term().Equals(tokens1[i]) == true, nextToken.Term() + " is not equal to " + tokens1[i]);
				i++;
			}
			Assert.IsTrue(i == tokens1.Length, i + " does not equal: " + tokens1.Length);
			Assert.IsTrue(theDetector.GetTokens().Count == 2, "theDetector Size: " + theDetector.GetTokens().Count + " is not: " + 2);
			Assert.IsTrue(dogDetector.GetTokens().Count == 1, "dogDetector Size: " + dogDetector.GetTokens().Count + " is not: " + 1);
			i = 0;
			for (Token nextToken = source2.Next(reusableToken); nextToken != null; nextToken = source2.Next(reusableToken))
			{
				Assert.IsTrue(nextToken.Term().Equals(tokens2[i]) == true, nextToken.Term() + " is not equal to " + tokens2[i]);
				i++;
			}
			Assert.IsTrue(i == tokens2.Length, i + " does not equal: " + tokens2.Length);
			Assert.IsTrue(theDetector.GetTokens().Count == 4, "theDetector Size: " + theDetector.GetTokens().Count + " is not: " + 4);
			Assert.IsTrue(dogDetector.GetTokens().Count == 2, "dogDetector Size: " + dogDetector.GetTokens().Count + " is not: " + 2);
			i = 0;
			for (Token nextToken = theDetector.Next(reusableToken); nextToken != null; nextToken = theDetector.Next(reusableToken))
			{
				Assert.IsTrue(nextToken.Term().ToUpper().Equals("The".ToUpper()) == true, nextToken.Term() + " is not equal to " + "The");
				i++;
			}
			Assert.IsTrue(i == theDetector.GetTokens().Count, i + " does not equal: " + theDetector.GetTokens().Count);
			i = 0;
			for (Token nextToken = dogDetector.Next(reusableToken); nextToken != null; nextToken = dogDetector.Next(reusableToken))
			{
				Assert.IsTrue(nextToken.Term().ToUpper().Equals("Dogs".ToUpper()) == true, nextToken.Term() + " is not equal to " + "Dogs");
				i++;
			}
			Assert.IsTrue(i == dogDetector.GetTokens().Count, i + " does not equal: " + dogDetector.GetTokens().Count);
			source1.Reset();
			TokenStream lowerCasing = new LowerCaseFilter(source1);
			i = 0;
			for (Token nextToken = lowerCasing.Next(reusableToken); nextToken != null; nextToken = lowerCasing.Next(reusableToken))
			{
				Assert.IsTrue(nextToken.Term().Equals(tokens1[i].ToLower()) == true, nextToken.Term() + " is not equal to " + tokens1[i].ToLower());
				i++;
			}
			Assert.IsTrue(i == tokens1.Length, i + " does not equal: " + tokens1.Length);
		}
		
		/// <summary> Not an explicit test, just useful to print out some info on performance
		/// 
		/// </summary>
		/// <throws>  Exception </throws>
		public virtual void  Performance()
		{
			int[] tokCount = new int[]{100, 500, 1000, 2000, 5000, 10000};
			int[] modCounts = new int[]{1, 2, 5, 10, 20, 50, 100, 200, 500};
			for (int k = 0; k < tokCount.Length; k++)
			{
				System.Text.StringBuilder buffer = new System.Text.StringBuilder();
				System.Console.Out.WriteLine("-----Tokens: " + tokCount[k] + "-----");
				for (int i = 0; i < tokCount[k]; i++)
				{
					buffer.Append(English.IntToEnglish(i).ToUpper()).Append(' ');
				}
				//make sure we produce the same tokens
				ModuloSinkTokenizer sink = new ModuloSinkTokenizer(this, tokCount[k], 100);
				Token reusableToken = new Token();
				TokenStream stream = new TeeTokenFilter(new StandardFilter(new StandardTokenizer(new System.IO.StringReader(buffer.ToString()))), sink);
				while (stream.Next(reusableToken) != null)
				{
				}
				stream = new ModuloTokenFilter(this, new StandardFilter(new StandardTokenizer(new System.IO.StringReader(buffer.ToString()))), 100);
				System.Collections.IList tmp = new System.Collections.ArrayList();
				for (Token nextToken = stream.Next(reusableToken); nextToken != null; nextToken = stream.Next(reusableToken))
				{
					tmp.Add(nextToken.Clone());
				}
				System.Collections.IList sinkList = sink.GetTokens();
				Assert.IsTrue(tmp.Count == sinkList.Count, "tmp Size: " + tmp.Count + " is not: " + sinkList.Count);
				for (int i = 0; i < tmp.Count; i++)
				{
					Token tfTok = (Token) tmp[i];
					Token sinkTok = (Token) sinkList[i];
					Assert.IsTrue(tfTok.Term().Equals(sinkTok.Term()) == true, tfTok.Term() + " is not equal to " + sinkTok.Term() + " at token: " + i);
				}
				//simulate two fields, each being analyzed once, for 20 documents
				
				for (int j = 0; j < modCounts.Length; j++)
				{
					int tfPos = 0;
					long start = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
					for (int i = 0; i < 20; i++)
					{
						stream = new StandardFilter(new StandardTokenizer(new System.IO.StringReader(buffer.ToString())));
						for (Token nextToken = stream.Next(reusableToken); nextToken != null; nextToken = stream.Next(reusableToken))
						{
							tfPos += nextToken.GetPositionIncrement();
						}
						stream = new ModuloTokenFilter(this, new StandardFilter(new StandardTokenizer(new System.IO.StringReader(buffer.ToString()))), modCounts[j]);
						for (Token nextToken = stream.Next(reusableToken); nextToken != null; nextToken = stream.Next(reusableToken))
						{
							tfPos += nextToken.GetPositionIncrement();
						}
					}
					long finish = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
					System.Console.Out.WriteLine("ModCount: " + modCounts[j] + " Two fields took " + (finish - start) + " ms");
					int sinkPos = 0;
					//simulate one field with one sink
					start = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
					for (int i = 0; i < 20; i++)
					{
						sink = new ModuloSinkTokenizer(this, tokCount[k], modCounts[j]);
						stream = new TeeTokenFilter(new StandardFilter(new StandardTokenizer(new System.IO.StringReader(buffer.ToString()))), sink);
						for (Token nextToken = stream.Next(reusableToken); nextToken != null; nextToken = stream.Next(reusableToken))
						{
							sinkPos += nextToken.GetPositionIncrement();
						}
						//System.out.println("Modulo--------");
						stream = sink;
						for (Token nextToken = stream.Next(reusableToken); nextToken != null; nextToken = stream.Next(reusableToken))
						{
							sinkPos += nextToken.GetPositionIncrement();
						}
					}
					finish = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
					System.Console.Out.WriteLine("ModCount: " + modCounts[j] + " Tee fields took " + (finish - start) + " ms");
					Assert.IsTrue(sinkPos == tfPos, sinkPos + " does not equal: " + tfPos);
				}
				System.Console.Out.WriteLine("- End Tokens: " + tokCount[k] + "-----");
			}
		}
		
		
		internal class ModuloTokenFilter:TokenFilter
		{
			private void  InitBlock(TestTeeTokenFilter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTeeTokenFilter enclosingInstance;
			public TestTeeTokenFilter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			internal int modCount;
			
			internal ModuloTokenFilter(TestTeeTokenFilter enclosingInstance, TokenStream input, int mc):base(input)
			{
				InitBlock(enclosingInstance);
				modCount = mc;
			}
			
			internal int count = 0;
			
			//return every 100 tokens
			public override Token Next(Token reusableToken)
			{
				Token nextToken = null;
				for (nextToken = input.Next(reusableToken); nextToken != null && count % modCount != 0; nextToken = input.Next(reusableToken))
				{
					count++;
				}
				count++;
				return nextToken;
			}
		}
		
		internal class ModuloSinkTokenizer:SinkTokenizer
		{
			private void  InitBlock(TestTeeTokenFilter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTeeTokenFilter enclosingInstance;
			public TestTeeTokenFilter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal int count = 0;
			internal int modCount;
			
			
			internal ModuloSinkTokenizer(TestTeeTokenFilter enclosingInstance, int numToks, int mc)
			{
				InitBlock(enclosingInstance);
				modCount = mc;
				lst = new System.Collections.ArrayList(numToks % mc);
			}
			
			public override void  Add(Token t)
			{
				if (t != null && count % modCount == 0)
				{
					base.Add(t);
				}
				count++;
			}
		}
	}
}