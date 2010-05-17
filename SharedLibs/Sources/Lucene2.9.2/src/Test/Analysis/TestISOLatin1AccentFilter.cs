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

namespace Lucene.Net.Analysis
{
	
    [TestFixture]
	public class TestISOLatin1AccentFilter:BaseTokenStreamTestCase
	{
        [Test]
		public virtual void  TestU()
		{
			TokenStream stream = new WhitespaceTokenizer(new System.IO.StringReader("Des mot clés À LA CHAÎNE À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ĳ Ð Ñ Ò Ó Ô Õ Ö Ø Œ Þ Ù Ú Û Ü Ý Ÿ à á â ã ä å æ ç è é ê ë ì í î ï ĳ ð ñ ò ó ô õ ö ø œ ß þ ù ú û ü ý ÿ ﬁ ﬂ"));
			ISOLatin1AccentFilter filter = new ISOLatin1AccentFilter(stream);
			TermAttribute termAtt = (TermAttribute) filter.GetAttribute(typeof(TermAttribute));
			AssertTermEquals("Des", filter, termAtt);
			AssertTermEquals("mot", filter, termAtt);
			AssertTermEquals("cles", filter, termAtt);
			AssertTermEquals("A", filter, termAtt);
			AssertTermEquals("LA", filter, termAtt);
			AssertTermEquals("CHAINE", filter, termAtt);
			AssertTermEquals("A", filter, termAtt);
			AssertTermEquals("A", filter, termAtt);
			AssertTermEquals("A", filter, termAtt);
			AssertTermEquals("A", filter, termAtt);
			AssertTermEquals("A", filter, termAtt);
			AssertTermEquals("A", filter, termAtt);
			AssertTermEquals("AE", filter, termAtt);
			AssertTermEquals("C", filter, termAtt);
			AssertTermEquals("E", filter, termAtt);
			AssertTermEquals("E", filter, termAtt);
			AssertTermEquals("E", filter, termAtt);
			AssertTermEquals("E", filter, termAtt);
			AssertTermEquals("I", filter, termAtt);
			AssertTermEquals("I", filter, termAtt);
			AssertTermEquals("I", filter, termAtt);
			AssertTermEquals("I", filter, termAtt);
			AssertTermEquals("IJ", filter, termAtt);
			AssertTermEquals("D", filter, termAtt);
			AssertTermEquals("N", filter, termAtt);
			AssertTermEquals("O", filter, termAtt);
			AssertTermEquals("O", filter, termAtt);
			AssertTermEquals("O", filter, termAtt);
			AssertTermEquals("O", filter, termAtt);
			AssertTermEquals("O", filter, termAtt);
			AssertTermEquals("O", filter, termAtt);
			AssertTermEquals("OE", filter, termAtt);
			AssertTermEquals("TH", filter, termAtt);
			AssertTermEquals("U", filter, termAtt);
			AssertTermEquals("U", filter, termAtt);
			AssertTermEquals("U", filter, termAtt);
			AssertTermEquals("U", filter, termAtt);
			AssertTermEquals("Y", filter, termAtt);
			AssertTermEquals("Y", filter, termAtt);
			AssertTermEquals("a", filter, termAtt);
			AssertTermEquals("a", filter, termAtt);
			AssertTermEquals("a", filter, termAtt);
			AssertTermEquals("a", filter, termAtt);
			AssertTermEquals("a", filter, termAtt);
			AssertTermEquals("a", filter, termAtt);
			AssertTermEquals("ae", filter, termAtt);
			AssertTermEquals("c", filter, termAtt);
			AssertTermEquals("e", filter, termAtt);
			AssertTermEquals("e", filter, termAtt);
			AssertTermEquals("e", filter, termAtt);
			AssertTermEquals("e", filter, termAtt);
			AssertTermEquals("i", filter, termAtt);
			AssertTermEquals("i", filter, termAtt);
			AssertTermEquals("i", filter, termAtt);
			AssertTermEquals("i", filter, termAtt);
			AssertTermEquals("ij", filter, termAtt);
			AssertTermEquals("d", filter, termAtt);
			AssertTermEquals("n", filter, termAtt);
			AssertTermEquals("o", filter, termAtt);
			AssertTermEquals("o", filter, termAtt);
			AssertTermEquals("o", filter, termAtt);
			AssertTermEquals("o", filter, termAtt);
			AssertTermEquals("o", filter, termAtt);
			AssertTermEquals("o", filter, termAtt);
			AssertTermEquals("oe", filter, termAtt);
			AssertTermEquals("ss", filter, termAtt);
			AssertTermEquals("th", filter, termAtt);
			AssertTermEquals("u", filter, termAtt);
			AssertTermEquals("u", filter, termAtt);
			AssertTermEquals("u", filter, termAtt);
			AssertTermEquals("u", filter, termAtt);
			AssertTermEquals("y", filter, termAtt);
			AssertTermEquals("y", filter, termAtt);
			AssertTermEquals("fi", filter, termAtt);
			AssertTermEquals("fl", filter, termAtt);
			Assert.IsFalse(filter.IncrementToken());
		}
		
		internal virtual void  AssertTermEquals(System.String expected, TokenStream stream, TermAttribute termAtt)
		{
			Assert.IsTrue(stream.IncrementToken());
			Assert.AreEqual(expected, termAtt.Term());
		}
	}
}