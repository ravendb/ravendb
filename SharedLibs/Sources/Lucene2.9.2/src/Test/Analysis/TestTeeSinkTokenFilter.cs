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
using PositionIncrementAttribute = Lucene.Net.Analysis.Tokenattributes.PositionIncrementAttribute;
using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using AttributeSource = Lucene.Net.Util.AttributeSource;
using English = Lucene.Net.Util.English;

namespace Lucene.Net.Analysis
{
	
	/// <summary> tests for the TestTeeSinkTokenFilter</summary>
    [TestFixture]
	public class TestTeeSinkTokenFilter:BaseTokenStreamTestCase
	{
		public class AnonymousClassSinkFilter:TeeSinkTokenFilter.SinkFilter
		{
			public override bool Accept(AttributeSource a)
			{
				TermAttribute termAtt = (TermAttribute) a.GetAttribute(typeof(TermAttribute));
				return termAtt.Term().ToUpper().Equals("The".ToUpper());
			}
		}
		public class AnonymousClassSinkFilter1:TeeSinkTokenFilter.SinkFilter
		{
			public override bool Accept(AttributeSource a)
			{
				TermAttribute termAtt = (TermAttribute) a.GetAttribute(typeof(TermAttribute));
				return termAtt.Term().ToUpper().Equals("Dogs".ToUpper());
			}
		}
		protected internal System.Text.StringBuilder buffer1;
		protected internal System.Text.StringBuilder buffer2;
		protected internal System.String[] tokens1;
		protected internal System.String[] tokens2;
		
		
		public TestTeeSinkTokenFilter(System.String s):base(s)
		{
		}

        public TestTeeSinkTokenFilter()
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
		
		internal static readonly TeeSinkTokenFilter.SinkFilter theFilter;
		
		internal static readonly TeeSinkTokenFilter.SinkFilter dogFilter;
		
		
		[Test]
		public virtual void  TestGeneral()
		{
			TeeSinkTokenFilter source = new TeeSinkTokenFilter(new WhitespaceTokenizer(new System.IO.StringReader(buffer1.ToString())));
			TokenStream sink1 = source.NewSinkTokenStream();
			TokenStream sink2 = source.NewSinkTokenStream(theFilter);

            source.AddAttribute(typeof(CheckClearAttributesAttribute));
            sink1.AddAttribute(typeof(CheckClearAttributesAttribute));
            sink2.AddAttribute(typeof(CheckClearAttributesAttribute));
    
            AssertTokenStreamContents(source, tokens1);
            AssertTokenStreamContents(sink1, tokens1);
		}
		
		[Test]
		public virtual void  TestMultipleSources()
		{
			TeeSinkTokenFilter tee1 = new TeeSinkTokenFilter(new WhitespaceTokenizer(new System.IO.StringReader(buffer1.ToString())));
			TeeSinkTokenFilter.SinkTokenStream dogDetector = tee1.NewSinkTokenStream(dogFilter);
			TeeSinkTokenFilter.SinkTokenStream theDetector = tee1.NewSinkTokenStream(theFilter);
			TokenStream source1 = new CachingTokenFilter(tee1);
			
             
            tee1.AddAttribute(typeof(CheckClearAttributesAttribute));
            dogDetector.AddAttribute(typeof(CheckClearAttributesAttribute));
            theDetector.AddAttribute(typeof(CheckClearAttributesAttribute));


			TeeSinkTokenFilter tee2 = new TeeSinkTokenFilter(new WhitespaceTokenizer(new System.IO.StringReader(buffer2.ToString())));
			tee2.AddSinkTokenStream(dogDetector);
			tee2.AddSinkTokenStream(theDetector);
			TokenStream source2 = tee2;

            AssertTokenStreamContents(source1, tokens1);
            AssertTokenStreamContents(source2, tokens2);

            AssertTokenStreamContents(theDetector, new String[] { "The", "the", "The", "the" });
            			
			source1.Reset();
			TokenStream lowerCasing = new LowerCaseFilter(source1);
            String[] lowerCaseTokens = new String[tokens1.Length];
            for (int i = 0; i < tokens1.Length; i++)
                lowerCaseTokens[i] = tokens1[i].ToLower();

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
				TeeSinkTokenFilter teeStream = new TeeSinkTokenFilter(new StandardFilter(new StandardTokenizer(new System.IO.StringReader(buffer.ToString()))));
				TokenStream sink = teeStream.NewSinkTokenStream(new ModuloSinkFilter(this, 100));
				teeStream.ConsumeAllTokens();
				TokenStream stream = new ModuloTokenFilter(this, new StandardFilter(new StandardTokenizer(new System.IO.StringReader(buffer.ToString()))), 100);
				TermAttribute tfTok = (TermAttribute) stream.AddAttribute(typeof(TermAttribute));
				TermAttribute sinkTok = (TermAttribute) sink.AddAttribute(typeof(TermAttribute));
				for (int i = 0; stream.IncrementToken(); i++)
				{
					Assert.IsTrue(sink.IncrementToken());
					Assert.IsTrue(tfTok.Equals(sinkTok) == true, tfTok + " is not equal to " + sinkTok + " at token: " + i);
				}
				
				//simulate two fields, each being analyzed once, for 20 documents
				for (int j = 0; j < modCounts.Length; j++)
				{
					int tfPos = 0;
					long start = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
					for (int i = 0; i < 20; i++)
					{
						stream = new StandardFilter(new StandardTokenizer(new System.IO.StringReader(buffer.ToString())));
						PositionIncrementAttribute posIncrAtt = (PositionIncrementAttribute) stream.GetAttribute(typeof(PositionIncrementAttribute));
						while (stream.IncrementToken())
						{
							tfPos += posIncrAtt.GetPositionIncrement();
						}
						stream = new ModuloTokenFilter(this, new StandardFilter(new StandardTokenizer(new System.IO.StringReader(buffer.ToString()))), modCounts[j]);
						posIncrAtt = (PositionIncrementAttribute) stream.GetAttribute(typeof(PositionIncrementAttribute));
						while (stream.IncrementToken())
						{
							tfPos += posIncrAtt.GetPositionIncrement();
						}
					}
					long finish = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
					System.Console.Out.WriteLine("ModCount: " + modCounts[j] + " Two fields took " + (finish - start) + " ms");
					int sinkPos = 0;
					//simulate one field with one sink
					start = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
					for (int i = 0; i < 20; i++)
					{
						teeStream = new TeeSinkTokenFilter(new StandardFilter(new StandardTokenizer(new System.IO.StringReader(buffer.ToString()))));
						sink = teeStream.NewSinkTokenStream(new ModuloSinkFilter(this, modCounts[j]));
						PositionIncrementAttribute posIncrAtt = (PositionIncrementAttribute) teeStream.GetAttribute(typeof(PositionIncrementAttribute));
						while (teeStream.IncrementToken())
						{
							sinkPos += posIncrAtt.GetPositionIncrement();
						}
						//System.out.println("Modulo--------");
						posIncrAtt = (PositionIncrementAttribute) sink.GetAttribute(typeof(PositionIncrementAttribute));
						while (sink.IncrementToken())
						{
							sinkPos += posIncrAtt.GetPositionIncrement();
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
			private void  InitBlock(TestTeeSinkTokenFilter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTeeSinkTokenFilter enclosingInstance;
			public TestTeeSinkTokenFilter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			internal int modCount;
			
			internal ModuloTokenFilter(TestTeeSinkTokenFilter enclosingInstance, TokenStream input, int mc):base(input)
			{
				InitBlock(enclosingInstance);
				modCount = mc;
			}
			
			internal int count = 0;
			
			//return every 100 tokens
			public override bool IncrementToken()
			{
				bool hasNext;
				for (hasNext = input.IncrementToken(); hasNext && count % modCount != 0; hasNext = input.IncrementToken())
				{
					count++;
				}
				count++;
				return hasNext;
			}
		}
		
		internal class ModuloSinkFilter:TeeSinkTokenFilter.SinkFilter
		{
			private void  InitBlock(TestTeeSinkTokenFilter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTeeSinkTokenFilter enclosingInstance;
			public TestTeeSinkTokenFilter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal int count = 0;
			internal int modCount;
			
			internal ModuloSinkFilter(TestTeeSinkTokenFilter enclosingInstance, int mc)
			{
				InitBlock(enclosingInstance);
				modCount = mc;
			}
			
			public override bool Accept(AttributeSource a)
			{
				bool b = (a != null && count % modCount == 0);
				count++;
				return b;
			}
		}
		static TestTeeSinkTokenFilter()
		{
			theFilter = new AnonymousClassSinkFilter();
			dogFilter = new AnonymousClassSinkFilter1();
		}
	}
}