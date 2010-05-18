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

namespace Lucene.Net.Documents
{
	
	[TestFixture]
	public class TestNumberTools:LuceneTestCase
	{
		[Test]
		public virtual void  TestNearZero()
		{
			for (int i = - 100; i <= 100; i++)
			{
				for (int j = - 100; j <= 100; j++)
				{
					SubtestTwoLongs(i, j);
				}
			}
		}
		
		[Test]
		public virtual void  TestMax()
		{
			// make sure the constants convert to their equivelents
			Assert.AreEqual(System.Int64.MaxValue, NumberTools.StringToLong(NumberTools.MAX_STRING_VALUE));
			Assert.AreEqual(NumberTools.MAX_STRING_VALUE, NumberTools.LongToString(System.Int64.MaxValue));
			
			// test near MAX, too
			for (long l = System.Int64.MaxValue; l > System.Int64.MaxValue - 10000; l--)
			{
				SubtestTwoLongs(l, l - 1);
			}
		}
		
		[Test]
		public virtual void  TestMin()
		{
			// make sure the constants convert to their equivelents
			Assert.AreEqual(System.Int64.MinValue, NumberTools.StringToLong(NumberTools.MIN_STRING_VALUE));
			Assert.AreEqual(NumberTools.MIN_STRING_VALUE, NumberTools.LongToString(System.Int64.MinValue));
			
			// test near MIN, too
			for (long l = System.Int64.MinValue; l < System.Int64.MinValue + 10000; l++)
			{
				SubtestTwoLongs(l, l + 1);
			}
		}
		
		private static void  SubtestTwoLongs(long i, long j)
		{
			// convert to strings
			System.String a = NumberTools.LongToString(i);
			System.String b = NumberTools.LongToString(j);
			
			// are they the right length?
			Assert.AreEqual(NumberTools.STR_SIZE, a.Length);
			Assert.AreEqual(NumberTools.STR_SIZE, b.Length);
			
			// are they the right order?
			if (i < j)
			{
				Assert.IsTrue(String.CompareOrdinal(a, b) < 0);
			}
			else if (i > j)
			{
				Assert.IsTrue(String.CompareOrdinal(a, b) > 0);
			}
			else
			{
				Assert.AreEqual(a, b);
			}
			
			// can we convert them back to longs?
			long i2 = NumberTools.StringToLong(a);
			long j2 = NumberTools.StringToLong(b);
			
			Assert.AreEqual(i, i2);
			Assert.AreEqual(j, j2);
		}
	}
}