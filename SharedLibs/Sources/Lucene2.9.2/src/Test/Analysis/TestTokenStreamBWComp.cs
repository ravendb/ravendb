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

using Lucene.Net.Analysis.Tokenattributes;
using Payload = Lucene.Net.Index.Payload;
using Attribute = Lucene.Net.Util.Attribute;
using AttributeImpl = Lucene.Net.Util.AttributeImpl;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Analysis
{
	
	/// <summary>This class tests some special cases of backwards compatibility when using the new TokenStream API with old analyzers </summary>
    [TestFixture]
	public class TestTokenStreamBWComp:LuceneTestCase
	{
		private class AnonymousClassTokenFilter:TokenFilter
		{
			private void  InitBlock(TestTokenStreamBWComp enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTokenStreamBWComp enclosingInstance;
			public TestTokenStreamBWComp Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal AnonymousClassTokenFilter(TestTokenStreamBWComp enclosingInstance, Lucene.Net.Analysis.TokenStream Param1):base(Param1)
			{
				InitBlock(enclosingInstance);
			}
			// we implement nothing, only un-abstract it
		}
		
		private System.String doc = "This is the new TokenStream api";
		private System.String[] stopwords = new System.String[]{"is", "the", "this"};
        private static System.String[] results = new System.String[]{"new", "tokenstream", "api"};
		
		[Serializable]
		public class POSToken:Token
		{
			public const int PROPERNOUN = 1;
			public const int NO_NOUN = 2;
			
			private int partOfSpeech;
			
			public virtual void  SetPartOfSpeech(int pos)
			{
				partOfSpeech = pos;
			}
			
			public virtual int GetPartOfSpeech()
			{
				return this.partOfSpeech;
			}
		}
		
		internal class PartOfSpeechTaggingFilter:TokenFilter
		{
			
			protected internal PartOfSpeechTaggingFilter(TokenStream input):base(input)
			{
			}
			
			public override Token Next()
			{
				Token t = input.Next();
				if (t == null)
					return null;
				
				POSToken pt = new POSToken();
				pt.Reinit(t);
				if (pt.TermLength() > 0)
				{
					if (System.Char.IsUpper(pt.TermBuffer()[0]))
					{
						pt.SetPartOfSpeech(Lucene.Net.Analysis.TestTokenStreamBWComp.POSToken.PROPERNOUN);
					}
					else
					{
						pt.SetPartOfSpeech(Lucene.Net.Analysis.TestTokenStreamBWComp.POSToken.NO_NOUN);
					}
				}
				return pt;
			}
		}
		
		internal class PartOfSpeechAnnotatingFilter:TokenFilter
		{
			public const byte PROPER_NOUN_ANNOTATION = 1;
			
			
			protected internal PartOfSpeechAnnotatingFilter(TokenStream input):base(input)
			{
			}
			
			public override Token Next()
			{
				Token t = input.Next();
				if (t == null)
					return null;
				
				if (t is POSToken)
				{
					POSToken pt = (POSToken) t;
					if (pt.GetPartOfSpeech() == Lucene.Net.Analysis.TestTokenStreamBWComp.POSToken.PROPERNOUN)
					{
						pt.SetPayload(new Payload(new byte[]{PROPER_NOUN_ANNOTATION}));
					}
					return pt;
				}
				else
				{
					return t;
				}
			}
		}
		
		// test the chain: The one and only term "TokenStream" should be declared as proper noun:
		
        [Test]
		public virtual void  TestTeeSinkCustomTokenNewAPI()
		{
			TestTeeSinkCustomToken(0);
		}
		
        [Test]
		public virtual void  TestTeeSinkCustomTokenOldAPI()
		{
			TestTeeSinkCustomToken(1);
		}
		
        [Test]
		public virtual void  TestTeeSinkCustomTokenVeryOldAPI()
		{
			TestTeeSinkCustomToken(2);
		}
		
		private void  TestTeeSinkCustomToken(int api)
		{
			TokenStream stream = new WhitespaceTokenizer(new System.IO.StringReader(doc));
			stream = new PartOfSpeechTaggingFilter(stream);
			stream = new LowerCaseFilter(stream);
			stream = new StopFilter(stream, stopwords);
			
			SinkTokenizer sink = new SinkTokenizer();
			TokenStream stream1 = new PartOfSpeechAnnotatingFilter(sink);
			
			stream = new TeeTokenFilter(stream, sink);
			stream = new PartOfSpeechAnnotatingFilter(stream);
			
			switch (api)
			{
				
				case 0: 
					ConsumeStreamNewAPI(stream);
					ConsumeStreamNewAPI(stream1);
					break;
				
				case 1: 
					ConsumeStreamOldAPI(stream);
					ConsumeStreamOldAPI(stream1);
					break;
				
				case 2: 
					ConsumeStreamVeryOldAPI(stream);
					ConsumeStreamVeryOldAPI(stream1);
					break;
				}
		}
		
		// test caching the special custom POSToken works in all cases
		
        [Test]
		public virtual void  TestCachingCustomTokenNewAPI()
		{
			TestTeeSinkCustomToken(0);
		}
		
        [Test]
		public virtual void  TestCachingCustomTokenOldAPI()
		{
			TestTeeSinkCustomToken(1);
		}
		
        [Test]
		public virtual void  TestCachingCustomTokenVeryOldAPI()
		{
			TestTeeSinkCustomToken(2);
		}
		
        [Test]
		public virtual void  TestCachingCustomTokenMixed()
		{
			TestTeeSinkCustomToken(3);
		}
		
		private void  TestCachingCustomToken(int api)
		{
			TokenStream stream = new WhitespaceTokenizer(new System.IO.StringReader(doc));
			stream = new PartOfSpeechTaggingFilter(stream);
			stream = new LowerCaseFilter(stream);
			stream = new StopFilter(stream, stopwords);
			stream = new CachingTokenFilter(stream); // <- the caching is done before the annotating!
			stream = new PartOfSpeechAnnotatingFilter(stream);
			
			switch (api)
			{
				
				case 0: 
					ConsumeStreamNewAPI(stream);
					ConsumeStreamNewAPI(stream);
					break;
				
				case 1: 
					ConsumeStreamOldAPI(stream);
					ConsumeStreamOldAPI(stream);
					break;
				
				case 2: 
					ConsumeStreamVeryOldAPI(stream);
					ConsumeStreamVeryOldAPI(stream);
					break;
				
				case 3: 
					ConsumeStreamNewAPI(stream);
					ConsumeStreamOldAPI(stream);
					ConsumeStreamVeryOldAPI(stream);
					ConsumeStreamNewAPI(stream);
					ConsumeStreamVeryOldAPI(stream);
					break;
				}
		}
		
		private static void  ConsumeStreamNewAPI(TokenStream stream)
		{
			stream.Reset();
			PayloadAttribute payloadAtt = (PayloadAttribute) stream.AddAttribute(typeof(PayloadAttribute));
			TermAttribute termAtt = (TermAttribute) stream.AddAttribute(typeof(TermAttribute));
			
			int i = 0;
			while (stream.IncrementToken())
			{
				System.String term = termAtt.Term();
				Payload p = payloadAtt.GetPayload();
				if (p != null && p.GetData().Length == 1 && p.GetData()[0] == PartOfSpeechAnnotatingFilter.PROPER_NOUN_ANNOTATION)
				{
					Assert.IsTrue("tokenstream".Equals(term), "only TokenStream is a proper noun");
				}
				else
				{
					Assert.IsFalse("tokenstream".Equals(term), "all other tokens (if this test fails, the special POSToken subclass is not correctly passed through the chain)");
				}
				Assert.AreEqual(results[i], term);
				i++;
			}
		}
		
		private static void  ConsumeStreamOldAPI(TokenStream stream)
		{
			stream.Reset();
			Token reusableToken = new Token();
			
			int i = 0;
			while ((reusableToken = stream.Next(reusableToken)) != null)
			{
				System.String term = reusableToken.Term();
				Payload p = reusableToken.GetPayload();
				if (p != null && p.GetData().Length == 1 && p.GetData()[0] == PartOfSpeechAnnotatingFilter.PROPER_NOUN_ANNOTATION)
				{
					Assert.IsTrue("tokenstream".Equals(term), "only TokenStream is a proper noun");
				}
				else
				{
					Assert.IsFalse("tokenstream".Equals(term), "all other tokens (if this test fails, the special POSToken subclass is not correctly passed through the chain)");
				}
				Assert.AreEqual(results[i], term);
				i++;
			}
		}
		
		private static void  ConsumeStreamVeryOldAPI(TokenStream stream)
		{
			stream.Reset();
			
			Token token;
			int i = 0;
			while ((token = stream.Next()) != null)
			{
				System.String term = token.Term();
				Payload p = token.GetPayload();
				if (p != null && p.GetData().Length == 1 && p.GetData()[0] == PartOfSpeechAnnotatingFilter.PROPER_NOUN_ANNOTATION)
				{
					Assert.IsTrue("tokenstream".Equals(term), "only TokenStream is a proper noun");
				}
				else
				{
					Assert.IsFalse("tokenstream".Equals(term), "all other tokens (if this test fails, the special POSToken subclass is not correctly passed through the chain)");
				}
				Assert.AreEqual(results[i], term);
				i++;
			}
		}
		
		// test if tokenization fails, if only the new API is allowed and an old TokenStream is in the chain
        [Test]
		public virtual void  TestOnlyNewAPI()
		{
			TokenStream.SetOnlyUseNewAPI(true);
			try
			{
				
				// this should fail with UOE
				try
				{
					TokenStream stream = new WhitespaceTokenizer(new System.IO.StringReader(doc));
					stream = new PartOfSpeechTaggingFilter(stream); // <-- this one is evil!
					stream = new LowerCaseFilter(stream);
					stream = new StopFilter(stream, stopwords);
					while (stream.IncrementToken())
						;
					Assert.Fail("If only the new API is allowed, this should fail with an UOE");
				}
				catch (System.NotSupportedException uoe)
				{
					Assert.IsTrue((typeof(PartOfSpeechTaggingFilter).FullName + " does not implement incrementToken() which is needed for onlyUseNewAPI.").Equals(uoe.Message));
				}
				
				// this should pass, as all core token streams support the new API
				TokenStream stream2 = new WhitespaceTokenizer(new System.IO.StringReader(doc));
				stream2 = new LowerCaseFilter(stream2);
				stream2 = new StopFilter(stream2, stopwords);
				while (stream2.IncrementToken())
					;
				
				// Test, if all attributes are implemented by their implementation, not Token/TokenWrapper
				Assert.IsTrue(stream2.AddAttribute(typeof(TermAttribute)) is TermAttributeImpl, "TermAttribute is implemented by TermAttributeImpl");
				Assert.IsTrue(stream2.AddAttribute(typeof(OffsetAttribute)) is OffsetAttributeImpl, "OffsetAttribute is implemented by OffsetAttributeImpl");
				Assert.IsTrue(stream2.AddAttribute(typeof(Lucene.Net.Analysis.Tokenattributes.FlagsAttribute)) is FlagsAttributeImpl, "FlagsAttribute is implemented by FlagsAttributeImpl");
				Assert.IsTrue(stream2.AddAttribute(typeof(PayloadAttribute)) is PayloadAttributeImpl, "PayloadAttribute is implemented by PayloadAttributeImpl");
				Assert.IsTrue(stream2.AddAttribute(typeof(PositionIncrementAttribute)) is PositionIncrementAttributeImpl, "PositionIncrementAttribute is implemented by PositionIncrementAttributeImpl");
				Assert.IsTrue(stream2.AddAttribute(typeof(TypeAttribute)) is TypeAttributeImpl, "TypeAttribute is implemented by TypeAttributeImpl");
				Assert.IsTrue(stream2.AddAttribute(typeof(SenselessAttribute)) is SenselessAttributeImpl, "SenselessAttribute is not implemented by SenselessAttributeImpl");
				
				// try to call old API, this should fail
				try
				{
					stream2.Reset();
					Token reusableToken = new Token();
					while ((reusableToken = stream2.Next(reusableToken)) != null)
						;
					Assert.Fail("If only the new API is allowed, this should fail with an UOE");
				}
				catch (System.NotSupportedException uoe)
				{
					Assert.IsTrue("This TokenStream only supports the new Attributes API.".Equals(uoe.Message));
				}
				try
				{
					stream2.Reset();
					while (stream2.Next() != null)
						;
					Assert.Fail("If only the new API is allowed, this should fail with an UOE");
				}
				catch (System.NotSupportedException uoe)
				{
					Assert.IsTrue("This TokenStream only supports the new Attributes API.".Equals(uoe.Message));
				}
				
				// Test if the wrapper API (onlyUseNewAPI==false) uses TokenWrapper
				// as attribute instance.
				// TokenWrapper encapsulates a Token instance that can be exchanged
				// by another Token instance without changing the AttributeImpl instance
				// itsself.
				TokenStream.SetOnlyUseNewAPI(false);
				stream2 = new WhitespaceTokenizer(new System.IO.StringReader(doc));
				Assert.IsTrue(stream2.AddAttribute(typeof(TermAttribute)) is TokenWrapper, "TermAttribute is implemented by TokenWrapper");
				Assert.IsTrue(stream2.AddAttribute(typeof(OffsetAttribute)) is TokenWrapper, "OffsetAttribute is implemented by TokenWrapper");
				Assert.IsTrue(stream2.AddAttribute(typeof(Lucene.Net.Analysis.Tokenattributes.FlagsAttribute)) is TokenWrapper, "FlagsAttribute is implemented by TokenWrapper");
				Assert.IsTrue(stream2.AddAttribute(typeof(PayloadAttribute)) is TokenWrapper, "PayloadAttribute is implemented by TokenWrapper");
				Assert.IsTrue(stream2.AddAttribute(typeof(PositionIncrementAttribute)) is TokenWrapper, "PositionIncrementAttribute is implemented by TokenWrapper");
				Assert.IsTrue(stream2.AddAttribute(typeof(TypeAttribute)) is TokenWrapper, "TypeAttribute is implemented by TokenWrapper");
				// This one is not implemented by TokenWrapper:
				Assert.IsTrue(stream2.AddAttribute(typeof(SenselessAttribute)) is SenselessAttributeImpl, "SenselessAttribute is not implemented by SenselessAttributeImpl");
			}
			finally
			{
				TokenStream.SetOnlyUseNewAPI(false);
			}
		}
		
        [Test]
		public virtual void  TestOverridesAny()
		{
			try
			{
				TokenStream stream = new WhitespaceTokenizer(new System.IO.StringReader(doc));
				stream = new AnonymousClassTokenFilter(this, stream);
				stream = new LowerCaseFilter(stream);
				stream = new StopFilter(stream, stopwords);
				while (stream.IncrementToken())
					;
				Assert.Fail("One TokenFilter does not override any of the required methods, so it should fail.");
			}
			catch (System.NotSupportedException uoe)
			{
				Assert.IsTrue(uoe.Message.EndsWith("does not implement any of incrementToken(), next(Token), next()."));
			}
		}
	}
		
	public interface SenselessAttribute:Attribute
	{
	}
	
	public sealed class SenselessAttributeImpl:AttributeImpl, SenselessAttribute
	{
		public override void  CopyTo(AttributeImpl target)
		{
		}
		
		public override void  Clear()
		{
		}
		
		public  override bool Equals(System.Object o)
		{
			return (o is SenselessAttributeImpl);
		}
		
		public override int GetHashCode()
		{
			return 0;
		}
	}
}