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

using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Analysis
{
	
    [TestFixture]
	public class TestCharArraySet:LuceneTestCase
	{
		
		internal static readonly System.String[] TEST_STOP_WORDS = new System.String[]{"a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"};
		
		
        [Test]
		public virtual void  TestRehash()
		{
			CharArraySet cas = new CharArraySet(0, true);
			for (int i = 0; i < TEST_STOP_WORDS.Length; i++)
				cas.Add(TEST_STOP_WORDS[i]);
			Assert.AreEqual(TEST_STOP_WORDS.Length, cas.Count);
			for (int i = 0; i < TEST_STOP_WORDS.Length; i++)
				Assert.IsTrue(cas.Contains(TEST_STOP_WORDS[i]));
		}

        [Test]
		public virtual void  TestNonZeroOffset()
		{
			System.String[] words = new System.String[]{"Hello", "World", "this", "is", "a", "test"};
			char[] findme = "xthisy".ToCharArray();
			CharArraySet set_Renamed = new CharArraySet(10, true);
			for (int i = 0; i < words.Length; i++) { set_Renamed.Add(words[i]); }
			Assert.IsTrue(set_Renamed.Contains(findme, 1, 4));
			Assert.IsTrue(set_Renamed.Contains(new System.String(findme, 1, 4)));
			
			// test unmodifiable
			set_Renamed = CharArraySet.UnmodifiableSet(set_Renamed);
			Assert.IsTrue(set_Renamed.Contains(findme, 1, 4));
			Assert.IsTrue(set_Renamed.Contains(new System.String(findme, 1, 4)));
		}
		
        [Test]
		public virtual void  TestObjectContains()
		{
			CharArraySet set_Renamed = new CharArraySet(10, true);
			System.Int32 val = 1;
			set_Renamed.Add((System.Object) val);
			Assert.IsTrue(set_Renamed.Contains((System.Object) val));
			Assert.IsTrue(set_Renamed.Contains((System.Object) 1));
			// test unmodifiable
			set_Renamed = CharArraySet.UnmodifiableSet(set_Renamed);
			Assert.IsTrue(set_Renamed.Contains((System.Object) val));
			Assert.IsTrue(set_Renamed.Contains((System.Object) 1));
		}
		
        [Test]
		public virtual void  TestClear()
		{
			CharArraySet set_Renamed = new CharArraySet(10, true);
			for (int i = 0; i < TEST_STOP_WORDS.Length; i++) { set_Renamed.Add(TEST_STOP_WORDS[i]); }
			Assert.AreEqual(TEST_STOP_WORDS.Length, set_Renamed.Count, "Not all words added");
			try
			{
				set_Renamed.Clear();
				Assert.Fail("remove is not supported");
			}
			catch (System.NotSupportedException e)
			{
				// expected
				Assert.AreEqual(TEST_STOP_WORDS.Length, set_Renamed.Count, "Not all words added");
			}
		}
		
        [Test]
		public virtual void  TestModifyOnUnmodifiable()
		{
            //System.Diagnostics.Debugger.Break();
            CharArraySet set_Renamed = new CharArraySet(10, true);
			set_Renamed.AddAll(TEST_STOP_WORDS);
			int size = set_Renamed.Count;
			set_Renamed = CharArraySet.UnmodifiableSet(set_Renamed);
			Assert.AreEqual(size, set_Renamed.Count, "Set size changed due to UnmodifiableSet call");
			System.String NOT_IN_SET = "SirGallahad";
			Assert.IsFalse(set_Renamed.Contains(NOT_IN_SET), "Test String already exists in set");
			
			try
			{
				set_Renamed.Add(NOT_IN_SET.ToCharArray());
				Assert.Fail("Modified unmodifiable set");
			}
			catch (System.NotSupportedException e)
			{
				// expected
				Assert.IsFalse(set_Renamed.Contains(NOT_IN_SET), "Test String has been added to unmodifiable set");
				Assert.AreEqual(size, set_Renamed.Count, "Size of unmodifiable set has changed");
			}
			
			try
			{
				set_Renamed.Add(NOT_IN_SET);
				Assert.Fail("Modified unmodifiable set");
			}
			catch (System.NotSupportedException e)
			{
				// expected
				Assert.IsFalse(set_Renamed.Contains(NOT_IN_SET), "Test String has been added to unmodifiable set");
				Assert.AreEqual(size, set_Renamed.Count, "Size of unmodifiable set has changed");
			}
			
			try
			{
				set_Renamed.Add(new System.Text.StringBuilder(NOT_IN_SET));
				Assert.Fail("Modified unmodifiable set");
			}
			catch (System.NotSupportedException e)
			{
				// expected
				Assert.IsFalse(set_Renamed.Contains(NOT_IN_SET), "Test String has been added to unmodifiable set");
				Assert.AreEqual(size, set_Renamed.Count, "Size of unmodifiable set has changed");
			}
			
			try
			{
				set_Renamed.Clear();
				Assert.Fail("Modified unmodifiable set");
			}
			catch (System.NotSupportedException e)
			{
				// expected
				Assert.IsFalse(set_Renamed.Contains(NOT_IN_SET), "Changed unmodifiable set");
				Assert.AreEqual(size, set_Renamed.Count, "Size of unmodifiable set has changed");
			}
			try
			{
				set_Renamed.Add((System.Object) NOT_IN_SET);
				Assert.Fail("Modified unmodifiable set");
			}
			catch (System.NotSupportedException e)
			{
				// expected
				Assert.IsFalse(set_Renamed.Contains(NOT_IN_SET), "Test String has been added to unmodifiable set");
				Assert.AreEqual(size, set_Renamed.Count, "Size of unmodifiable set has changed");
			}
			try
			{
				set_Renamed.RemoveAll(new System.Collections.ArrayList(TEST_STOP_WORDS));
				Assert.Fail("Modified unmodifiable set");
			}
			catch (System.NotSupportedException e)
			{
				// expected
				Assert.AreEqual(size, set_Renamed.Count, "Size of unmodifiable set has changed");
			}
			
			try
			{
                set_Renamed.RetainAll(new System.Collections.ArrayList(new System.String[] { NOT_IN_SET }));
				Assert.Fail("Modified unmodifiable set");
			}
			catch (System.NotSupportedException e)
			{
				// expected
				Assert.AreEqual(size, set_Renamed.Count, "Size of unmodifiable set has changed");
			}
			
			try
			{
				set_Renamed.AddAll(new System.Collections.ArrayList(new System.String[] { NOT_IN_SET }));
				Assert.Fail("Modified unmodifiable set");
			}
			catch (System.NotSupportedException e)
			{
				// expected
				Assert.IsFalse(set_Renamed.Contains(NOT_IN_SET), "Test String has been added to unmodifiable set");
			}
			
			for (int i = 0; i < TEST_STOP_WORDS.Length; i++)
			{
				Assert.IsTrue(set_Renamed.Contains(TEST_STOP_WORDS[i]));
			}
		}
		
        [Test]
		public virtual void  TestUnmodifiableSet()
		{
			CharArraySet set_Renamed = new CharArraySet(10, true);
			set_Renamed.AddAll(new System.Collections.ArrayList(TEST_STOP_WORDS));
			int size = set_Renamed.Count;
			set_Renamed = CharArraySet.UnmodifiableSet(set_Renamed);
			Assert.AreEqual(size, set_Renamed.Count, "Set size changed due to UnmodifiableSet call");
			
			try
			{
				CharArraySet.UnmodifiableSet(null);
				Assert.Fail("can not make null unmodifiable");
			}
			catch (System.NullReferenceException e)
			{
				// expected
			}
		}
	}
}