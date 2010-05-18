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

namespace Lucene.Net.Analysis
{
	
    [TestFixture]
	public class TestMappingCharFilter:BaseTokenStreamTestCase
	{
		
		internal NormalizeCharMap normMap;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			normMap = new NormalizeCharMap();
			
			normMap.Add("aa", "a");
			normMap.Add("bbb", "b");
			normMap.Add("cccc", "cc");
			
			normMap.Add("h", "i");
			normMap.Add("j", "jj");
			normMap.Add("k", "kkk");
			normMap.Add("ll", "llll");
			
			normMap.Add("empty", "");
		}
		
        [Test]
		public virtual void  TestReaderReset()
		{
			CharStream cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("x")));
			char[] buf = new char[10];
			int len = cs.Read(buf, 0, 10);
			Assert.AreEqual(1, len);
			Assert.AreEqual('x', buf[0]);
			len = cs.Read(buf, 0, 10);
			Assert.AreEqual(- 1, len);
			
			// rewind
			cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("x")));
			len = cs.Read(buf, 0, 10);
			Assert.AreEqual(1, len);
			Assert.AreEqual('x', buf[0]);
		}
		
        [Test]
		public virtual void  TestNothingChange()
		{
			CharStream cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("x")));
			TokenStream ts = new WhitespaceTokenizer(cs);
			AssertTokenStreamContents(ts, new System.String[]{"x"}, new int[]{0}, new int[]{1});
		}
		
        [Test]
		public virtual void  Test1to1()
		{
			CharStream cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("h")));
			TokenStream ts = new WhitespaceTokenizer(cs);
			AssertTokenStreamContents(ts, new System.String[]{"i"}, new int[]{0}, new int[]{1});
		}
		
        [Test]
		public virtual void  Test1to2()
		{
			CharStream cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("j")));
			TokenStream ts = new WhitespaceTokenizer(cs);
			AssertTokenStreamContents(ts, new System.String[]{"jj"}, new int[]{0}, new int[]{1});
		}
		
        [Test]
		public virtual void  Test1to3()
		{
			CharStream cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("k")));
			TokenStream ts = new WhitespaceTokenizer(cs);
			AssertTokenStreamContents(ts, new System.String[]{"kkk"}, new int[]{0}, new int[]{1});
		}
		
        [Test]
		public virtual void  Test2to4()
		{
			CharStream cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("ll")));
			TokenStream ts = new WhitespaceTokenizer(cs);
			AssertTokenStreamContents(ts, new System.String[]{"llll"}, new int[]{0}, new int[]{2});
		}
		
        [Test]
		public virtual void  Test2to1()
		{
			CharStream cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("aa")));
			TokenStream ts = new WhitespaceTokenizer(cs);
			AssertTokenStreamContents(ts, new System.String[]{"a"}, new int[]{0}, new int[]{2});
		}
		
        [Test]
		public virtual void  Test3to1()
		{
			CharStream cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("bbb")));
			TokenStream ts = new WhitespaceTokenizer(cs);
			AssertTokenStreamContents(ts, new System.String[]{"b"}, new int[]{0}, new int[]{3});
		}
		
        [Test]
		public virtual void  Test4to2()
		{
			CharStream cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("cccc")));
			TokenStream ts = new WhitespaceTokenizer(cs);
			AssertTokenStreamContents(ts, new System.String[]{"cc"}, new int[]{0}, new int[]{4});
		}
		
        [Test]
		public virtual void  Test5to0()
		{
			CharStream cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("empty")));
			TokenStream ts = new WhitespaceTokenizer(cs);
			AssertTokenStreamContents(ts, new System.String[0]);
		}
		
		//
		//                1111111111222
		//      01234567890123456789012
		//(in)  h i j k ll cccc bbb aa
		//
		//                1111111111222
		//      01234567890123456789012
		//(out) i i jj kkk llll cc b a
		//
		//    h, 0, 1 =>    i, 0, 1
		//    i, 2, 3 =>    i, 2, 3
		//    j, 4, 5 =>   jj, 4, 5
		//    k, 6, 7 =>  kkk, 6, 7
		//   ll, 8,10 => llll, 8,10
		// cccc,11,15 =>   cc,11,15
		//  bbb,16,19 =>    b,16,19
		//   aa,20,22 =>    a,20,22
		//
        [Test]
		public virtual void  TestTokenStream()
		{
			CharStream cs = new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("h i j k ll cccc bbb aa")));
			TokenStream ts = new WhitespaceTokenizer(cs);
			AssertTokenStreamContents(ts, new System.String[]{"i", "i", "jj", "kkk", "llll", "cc", "b", "a"}, new int[]{0, 2, 4, 6, 8, 11, 16, 20}, new int[]{1, 3, 5, 7, 10, 15, 19, 22});
		}
		
		//
		//
		//        0123456789
		//(in)    aaaa ll h
		//(out-1) aa llll i
		//(out-2) a llllllll i
		//
		// aaaa,0,4 => a,0,4
		//   ll,5,7 => llllllll,5,7
		//    h,8,9 => i,8,9
        [Test]
		public virtual void  TestChained()
		{
			CharStream cs = new MappingCharFilter(normMap, new MappingCharFilter(normMap, CharReader.Get(new System.IO.StringReader("aaaa ll h"))));
			TokenStream ts = new WhitespaceTokenizer(cs);
			AssertTokenStreamContents(ts, new System.String[]{"a", "llllllll", "i"}, new int[]{0, 5, 8}, new int[]{4, 7, 9});
		}
	}
}