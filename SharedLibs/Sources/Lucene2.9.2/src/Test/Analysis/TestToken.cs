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

using Payload = Lucene.Net.Index.Payload;
using TestSimpleAttributeImpls = Lucene.Net.Analysis.Tokenattributes.TestSimpleAttributeImpls;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Analysis
{
	
    [TestFixture]
	public class TestToken:LuceneTestCase
	{
        public TestToken()
        {
        }
		
		public TestToken(System.String name):base(name)
		{
		}
		
        [Test]
		public virtual void  TestCtor()
		{
			Token t = new Token();
			char[] content = "hello".ToCharArray();
			t.SetTermBuffer(content, 0, content.Length);
			char[] buf = t.TermBuffer();
			Assert.AreNotEqual(t.TermBuffer(), content);
			Assert.AreEqual("hello", t.Term());
			Assert.AreEqual("word", t.Type());
			Assert.AreEqual(0, t.GetFlags());
			
			t = new Token(6, 22);
			t.SetTermBuffer(content, 0, content.Length);
			Assert.AreEqual("hello", t.Term());
			Assert.AreEqual("(hello,6,22)", t.ToString());
			Assert.AreEqual("word", t.Type());
			Assert.AreEqual(0, t.GetFlags());
			
			t = new Token(6, 22, 7);
			t.SetTermBuffer(content, 0, content.Length);
			Assert.AreEqual("hello", t.Term());
			Assert.AreEqual("(hello,6,22)", t.ToString());
			Assert.AreEqual(7, t.GetFlags());
			
			t = new Token(6, 22, "junk");
			t.SetTermBuffer(content, 0, content.Length);
			Assert.AreEqual("hello", t.Term());
			Assert.AreEqual("(hello,6,22,type=junk)", t.ToString());
			Assert.AreEqual(0, t.GetFlags());
		}
		
        [Test]
		public virtual void  TestResize()
		{
			Token t = new Token();
			char[] content = "hello".ToCharArray();
			t.SetTermBuffer(content, 0, content.Length);
			for (int i = 0; i < 2000; i++)
			{
				t.ResizeTermBuffer(i);
				Assert.IsTrue(i <= t.TermBuffer().Length);
				Assert.AreEqual("hello", t.Term());
			}
		}
		
        [Test]
		public virtual void  TestGrow()
		{
			Token t = new Token();
			System.Text.StringBuilder buf = new System.Text.StringBuilder("ab");
			for (int i = 0; i < 20; i++)
			{
				char[] content = buf.ToString().ToCharArray();
				t.SetTermBuffer(content, 0, content.Length);
				Assert.AreEqual(buf.Length, t.TermLength());
				Assert.AreEqual(buf.ToString(), t.Term());
				buf.Append(buf.ToString());
			}
			Assert.AreEqual(1048576, t.TermLength());
			Assert.AreEqual(1179654, t.TermBuffer().Length);
			
			// now as a string, first variant
			t = new Token();
			buf = new System.Text.StringBuilder("ab");
			for (int i = 0; i < 20; i++)
			{
				System.String content = buf.ToString();
				t.SetTermBuffer(content, 0, content.Length);
				Assert.AreEqual(content.Length, t.TermLength());
				Assert.AreEqual(content, t.Term());
				buf.Append(content);
			}
			Assert.AreEqual(1048576, t.TermLength());
			Assert.AreEqual(1179654, t.TermBuffer().Length);
			
			// now as a string, second variant
			t = new Token();
			buf = new System.Text.StringBuilder("ab");
			for (int i = 0; i < 20; i++)
			{
				System.String content = buf.ToString();
				t.SetTermBuffer(content);
				Assert.AreEqual(content.Length, t.TermLength());
				Assert.AreEqual(content, t.Term());
				buf.Append(content);
			}
			Assert.AreEqual(1048576, t.TermLength());
			Assert.AreEqual(1179654, t.TermBuffer().Length);
			
			// Test for slow growth to a long term
			t = new Token();
			buf = new System.Text.StringBuilder("a");
			for (int i = 0; i < 20000; i++)
			{
				System.String content = buf.ToString();
				t.SetTermBuffer(content);
				Assert.AreEqual(content.Length, t.TermLength());
				Assert.AreEqual(content, t.Term());
				buf.Append("a");
			}
			Assert.AreEqual(20000, t.TermLength());
			Assert.AreEqual(20167, t.TermBuffer().Length);
			
			// Test for slow growth to a long term
			t = new Token();
			buf = new System.Text.StringBuilder("a");
			for (int i = 0; i < 20000; i++)
			{
				System.String content = buf.ToString();
				t.SetTermBuffer(content);
				Assert.AreEqual(content.Length, t.TermLength());
				Assert.AreEqual(content, t.Term());
				buf.Append("a");
			}
			Assert.AreEqual(20000, t.TermLength());
			Assert.AreEqual(20167, t.TermBuffer().Length);
		}
		
        [Test]
		public virtual void  TestToString()
		{
			char[] b = new char[]{'a', 'l', 'o', 'h', 'a'};
			Token t = new Token("", 0, 5);
			t.SetTermBuffer(b, 0, 5);
			Assert.AreEqual("(aloha,0,5)", t.ToString());
			
			t.SetTermText("hi there");
			Assert.AreEqual("(hi there,0,5)", t.ToString());
		}
		
        [Test]
		public virtual void  TestTermBufferEquals()
		{
			Token t1a = new Token();
			char[] content1a = "hello".ToCharArray();
			t1a.SetTermBuffer(content1a, 0, 5);
			Token t1b = new Token();
			char[] content1b = "hello".ToCharArray();
			t1b.SetTermBuffer(content1b, 0, 5);
			Token t2 = new Token();
			char[] content2 = "hello2".ToCharArray();
			t2.SetTermBuffer(content2, 0, 6);
			Assert.IsTrue(t1a.Equals(t1b));
			Assert.IsFalse(t1a.Equals(t2));
			Assert.IsFalse(t2.Equals(t1b));
		}
		
        [Test]
		public virtual void  TestMixedStringArray()
		{
			Token t = new Token("hello", 0, 5);
			Assert.AreEqual(t.TermText(), "hello");
			Assert.AreEqual(t.TermLength(), 5);
			Assert.AreEqual(t.Term(), "hello");
			t.SetTermText("hello2");
			Assert.AreEqual(t.TermLength(), 6);
			Assert.AreEqual(t.Term(), "hello2");
			t.SetTermBuffer("hello3".ToCharArray(), 0, 6);
			Assert.AreEqual(t.TermText(), "hello3");
			
			// Make sure if we get the buffer and change a character
			// that termText() reflects the change
			char[] buffer = t.TermBuffer();
			buffer[1] = 'o';
			Assert.AreEqual(t.TermText(), "hollo3");
		}
		
        [Test]
		public virtual void  TestClone()
		{
			Token t = new Token(0, 5);
			char[] content = "hello".ToCharArray();
			t.SetTermBuffer(content, 0, 5);
			char[] buf = t.TermBuffer();
			Token copy = (Token) TestSimpleAttributeImpls.AssertCloneIsEqual(t);
			Assert.AreEqual(t.Term(), copy.Term());
            Assert.AreNotSame(buf, copy.TermBuffer());
			
			Payload pl = new Payload(new byte[]{1, 2, 3, 4});
			t.SetPayload(pl);
			copy = (Token) TestSimpleAttributeImpls.AssertCloneIsEqual(t);
			Assert.AreEqual(pl, copy.GetPayload());
			Assert.AreNotSame(pl, copy.GetPayload());
		}
		
        [Test]
		public virtual void  TestCopyTo()
		{
			Token t = new Token();
			Token copy = (Token) TestSimpleAttributeImpls.AssertCopyIsEqual(t);
			Assert.AreEqual("", t.Term());
			Assert.AreEqual("", copy.Term());
			
			t = new Token(0, 5);
			char[] content = "hello".ToCharArray();
			t.SetTermBuffer(content, 0, 5);
			char[] buf = t.TermBuffer();
			copy = (Token) TestSimpleAttributeImpls.AssertCopyIsEqual(t);
			Assert.AreEqual(t.Term(), copy.Term());
			Assert.AreNotSame(buf, copy.TermBuffer());
			
			Payload pl = new Payload(new byte[]{1, 2, 3, 4});
			t.SetPayload(pl);
			copy = (Token) TestSimpleAttributeImpls.AssertCopyIsEqual(t);
			Assert.AreEqual(pl, copy.GetPayload());
            Assert.AreNotSame(pl, copy.GetPayload());
		}
	}
}