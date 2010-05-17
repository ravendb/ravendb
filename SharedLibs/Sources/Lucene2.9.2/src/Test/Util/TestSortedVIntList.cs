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

using DocIdSetIterator = Lucene.Net.Search.DocIdSetIterator;

namespace Lucene.Net.Util
{
	
	[TestFixture]
	public class TestSortedVIntList:LuceneTestCase
	{
		
		internal virtual void  TstIterator(SortedVIntList vintList, int[] ints)
		{
			for (int i = 0; i < ints.Length; i++)
			{
				if ((i > 0) && (ints[i - 1] == ints[i]))
				{
					return ; // DocNrSkipper should not skip to same document.
				}
			}
			DocIdSetIterator m = vintList.Iterator();
			for (int i = 0; i < ints.Length; i++)
			{
				Assert.IsTrue(m.NextDoc() != DocIdSetIterator.NO_MORE_DOCS, "No end of Matcher at: " + i);
				Assert.AreEqual(ints[i], m.DocID());
			}
			Assert.IsTrue(m.NextDoc() == DocIdSetIterator.NO_MORE_DOCS, "End of Matcher");
		}
		
		internal virtual void  TstVIntList(SortedVIntList vintList, int[] ints, int expectedByteSize)
		{
			Assert.AreEqual(ints.Length, vintList.Size(), "Size");
			Assert.AreEqual(expectedByteSize, vintList.GetByteSize(), "Byte size");
			TstIterator(vintList, ints);
		}
		
		public virtual void  TstViaBitSet(int[] ints, int expectedByteSize)
		{
			int MAX_INT_FOR_BITSET = 1024 * 1024;
			//mgarski - BitArray cannot grow, so make as large as we would need it to be
			System.Collections.BitArray bs = new System.Collections.BitArray(MAX_INT_FOR_BITSET);
			for (int i = 0; i < ints.Length; i++)
			{
				if (ints[i] > MAX_INT_FOR_BITSET)
				{
					return ; // BitSet takes too much memory
				}
				if ((i > 0) && (ints[i - 1] == ints[i]))
				{
					return ; // BitSet cannot store duplicate.
				}
				bs.Set(ints[i], true);
			}
			SortedVIntList svil = new SortedVIntList(bs);
			TstVIntList(svil, ints, expectedByteSize);
			TstVIntList(new SortedVIntList(svil.Iterator()), ints, expectedByteSize);
		}
		
		private const int VB1 = 0x7F;
		private const int BIT_SHIFT = 7;
		private static readonly int VB2 = (VB1 << BIT_SHIFT) | VB1;
		private static readonly int VB3 = (VB2 << BIT_SHIFT) | VB1;
		private static readonly int VB4 = (VB3 << BIT_SHIFT) | VB1;
		
		private int VIntByteSize(int i)
		{
			System.Diagnostics.Debug.Assert(i >= 0);
			if (i <= VB1)
				return 1;
			if (i <= VB2)
				return 2;
			if (i <= VB3)
				return 3;
			if (i <= VB4)
				return 4;
			return 5;
		}
		
		private int VIntListByteSize(int[] ints)
		{
			int byteSize = 0;
			int last = 0;
			for (int i = 0; i < ints.Length; i++)
			{
				byteSize += VIntByteSize(ints[i] - last);
				last = ints[i];
			}
			return byteSize;
		}
		
		public virtual void  TstInts(int[] ints)
		{
			int expectedByteSize = VIntListByteSize(ints);
			try
			{
				TstVIntList(new SortedVIntList(ints), ints, expectedByteSize);
				TstViaBitSet(ints, expectedByteSize);
			}
			catch (System.IO.IOException ioe)
			{
				throw new System.SystemException("", ioe);
			}
		}
		
		public virtual void  TstIllegalArgExc(int[] ints)
		{
			try
			{
				new SortedVIntList(ints);
			}
			catch (System.ArgumentException e)
			{
				return ;
			}
			Assert.Fail("Expected IllegalArgumentException");
		}
		
		private int[] FibArray(int a, int b, int size)
		{
			int[] fib = new int[size];
			fib[0] = a;
			fib[1] = b;
			for (int i = 2; i < size; i++)
			{
				fib[i] = fib[i - 1] + fib[i - 2];
			}
			return fib;
		}
		
		private int[] ReverseDiffs(int[] ints)
		{
			// reverse the order of the successive differences
			int[] res = new int[ints.Length];
			for (int i = 0; i < ints.Length; i++)
			{
				res[i] = ints[ints.Length - 1] + (ints[0] - ints[ints.Length - 1 - i]);
			}
			return res;
		}
		
		[Test]
		public virtual void  Test01()
		{
			TstInts(new int[]{});
		}
		[Test]
		public virtual void  Test02()
		{
			TstInts(new int[]{0});
		}
		[Test]
		public virtual void  Test04a()
		{
			TstInts(new int[]{0, VB2 - 1});
		}
		[Test]
		public virtual void  Test04b()
		{
			TstInts(new int[]{0, VB2});
		}
		[Test]
		public virtual void  Test04c()
		{
			TstInts(new int[]{0, VB2 + 1});
		}
		[Test]
		public virtual void  Test05()
		{
			TstInts(FibArray(0, 1, 7)); // includes duplicate value 1
		}
		[Test]
		public virtual void  Test05b()
		{
			TstInts(ReverseDiffs(FibArray(0, 1, 7)));
		}
		[Test]
		public virtual void  Test06()
		{
			TstInts(FibArray(1, 2, 45)); // no duplicates, size 46 exceeds max int.
		}
		[Test]
		public virtual void  Test06b()
		{
			TstInts(ReverseDiffs(FibArray(1, 2, 45)));
		}
		[Test]
		public virtual void  Test07a()
		{
			TstInts(new int[]{0, VB3});
		}
		[Test]
		public virtual void  Test07b()
		{
			TstInts(new int[]{1, VB3 + 2});
		}
		[Test]
		public virtual void  Test07c()
		{
			TstInts(new int[]{2, VB3 + 4});
		}
		[Test]
		public virtual void  Test08a()
		{
			TstInts(new int[]{0, VB4 + 1});
		}
		[Test]
		public virtual void  Test08b()
		{
			TstInts(new int[]{1, VB4 + 1});
		}
		[Test]
		public virtual void  Test08c()
		{
			TstInts(new int[]{2, VB4 + 1});
		}
		
		[Test]
		public virtual void  Test10()
		{
			TstIllegalArgExc(new int[]{- 1});
		}
		[Test]
		public virtual void  Test11()
		{
			TstIllegalArgExc(new int[]{1, 0});
		}
		[Test]
		public virtual void  Test12()
		{
			TstIllegalArgExc(new int[]{0, 1, 1, 2, 3, 5, 8, 0});
		}
	}
}