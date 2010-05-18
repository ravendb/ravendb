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

using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using TypeAttribute = Lucene.Net.Analysis.Tokenattributes.TypeAttribute;
using NumericUtils = Lucene.Net.Util.NumericUtils;

namespace Lucene.Net.Analysis
{
	
    [TestFixture]
	public class TestNumericTokenStream:BaseTokenStreamTestCase
	{
		
		internal const long lvalue = 4573245871874382L;
		internal const int ivalue = 123456;
		
        [Test]
		public virtual void  TestLongStream()
		{
			NumericTokenStream stream = new NumericTokenStream().SetLongValue(lvalue);
			// use getAttribute to test if attributes really exist, if not an IAE will be throwed
			TermAttribute termAtt = (TermAttribute) stream.GetAttribute(typeof(TermAttribute));
			TypeAttribute typeAtt = (TypeAttribute) stream.GetAttribute(typeof(TypeAttribute));
			for (int shift = 0; shift < 64; shift += NumericUtils.PRECISION_STEP_DEFAULT)
			{
				Assert.IsTrue(stream.IncrementToken(), "New token is available");
				Assert.AreEqual(NumericUtils.LongToPrefixCoded(lvalue, shift), termAtt.Term(), "Term is correctly encoded");
				Assert.AreEqual((shift == 0)?NumericTokenStream.TOKEN_TYPE_FULL_PREC:NumericTokenStream.TOKEN_TYPE_LOWER_PREC, typeAtt.Type(), "Type correct");
			}
			Assert.IsFalse(stream.IncrementToken(), "No more tokens available");
		}
		
        [Test]
		public virtual void  TestIntStream()
		{
			NumericTokenStream stream = new NumericTokenStream().SetIntValue(ivalue);
			// use getAttribute to test if attributes really exist, if not an IAE will be throwed
			TermAttribute termAtt = (TermAttribute) stream.GetAttribute(typeof(TermAttribute));
			TypeAttribute typeAtt = (TypeAttribute) stream.GetAttribute(typeof(TypeAttribute));
			for (int shift = 0; shift < 32; shift += NumericUtils.PRECISION_STEP_DEFAULT)
			{
				Assert.IsTrue(stream.IncrementToken(), "New token is available");
				Assert.AreEqual(NumericUtils.IntToPrefixCoded(ivalue, shift), termAtt.Term(), "Term is correctly encoded");
				Assert.AreEqual((shift == 0)?NumericTokenStream.TOKEN_TYPE_FULL_PREC:NumericTokenStream.TOKEN_TYPE_LOWER_PREC, typeAtt.Type(), "Type correct");
			}
			Assert.IsFalse(stream.IncrementToken(), "No more tokens available");
		}
		
        [Test]
		public virtual void  TestNotInitialized()
		{
			NumericTokenStream stream = new NumericTokenStream();
			
			try
			{
				stream.Reset();
				Assert.Fail("reset() should not succeed.");
			}
			catch (System.SystemException e)
			{
				// pass
			}
			
			try
			{
				stream.IncrementToken();
				Assert.Fail("incrementToken() should not succeed.");
			}
			catch (System.SystemException e)
			{
				// pass
			}
		}
	}
}