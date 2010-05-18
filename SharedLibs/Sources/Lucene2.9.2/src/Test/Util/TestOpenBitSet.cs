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
	
	/// <version>  $Id$
	/// </version>
	[TestFixture]
	public class TestOpenBitSet:LuceneTestCase
	{
		internal System.Random rand;
		
		internal virtual void  DoGet(System.Collections.BitArray a, OpenBitSet b)
		{
			int max = a.Count;
			for (int i = 0; i < max; i++)
			{
				if (a.Get(i) != b.Get(i))
				{
					Assert.Fail("mismatch: BitSet=[" + i + "]=" + a.Get(i));
				}
			}
		}
		
		internal virtual void  DoNextSetBit(System.Collections.BitArray a, OpenBitSet b)
		{
			int aa = - 1, bb = - 1;
			do 
			{
				aa = SupportClass.BitSetSupport.NextSetBit(a, aa + 1);
				bb = b.NextSetBit(bb + 1);
				Assert.AreEqual(aa, bb);
			}
			while (aa >= 0);
		}
		
		// test interleaving different OpenBitSetIterator.next()/skipTo()
		internal virtual void  DoIterate(System.Collections.BitArray a, OpenBitSet b, int mode)
		{
			if (mode == 1)
				DoIterate1(a, b);
			if (mode == 2)
				DoIterate2(a, b);
		}
		
		internal virtual void  DoIterate1(System.Collections.BitArray a, OpenBitSet b)
		{
			int aa = - 1, bb = - 1;
			OpenBitSetIterator iterator = new OpenBitSetIterator(b);
			do 
			{
				aa = SupportClass.BitSetSupport.NextSetBit(a, aa + 1);
				bb = rand.NextDouble() > 0.5 ? iterator.NextDoc() : iterator.Advance(bb + 1);
				Assert.AreEqual(aa == - 1?DocIdSetIterator.NO_MORE_DOCS:aa, bb);
			}
			while (aa >= 0);
		}
		
		internal virtual void  DoIterate2(System.Collections.BitArray a, OpenBitSet b)
		{
			int aa = - 1, bb = - 1;
			OpenBitSetIterator iterator = new OpenBitSetIterator(b);
			do 
			{
				aa = SupportClass.BitSetSupport.NextSetBit(a, aa + 1);
				bb = rand.NextDouble() > 0.5 ? iterator.NextDoc() : iterator.Advance(bb + 1);
				Assert.AreEqual(aa == - 1?DocIdSetIterator.NO_MORE_DOCS:aa, bb);
			}
			while (aa >= 0);
		}
		
		internal virtual void  DoRandomSets(int maxSize, int iter, int mode)
		{
			System.Collections.BitArray a0 = null;
			OpenBitSet b0 = null;
			
			for (int i = 0; i < iter; i++)
			{
				int sz = rand.Next(maxSize);
				System.Collections.BitArray a = new System.Collections.BitArray(sz);
				OpenBitSet b = new OpenBitSet(sz);
				
				// test the various ways of setting bits
				if (sz > 0)
				{
					int nOper = rand.Next(sz);
					for (int j = 0; j < nOper; j++)
					{
						int idx;
						
						idx = rand.Next(sz);
						a.Set(idx, true);
						b.FastSet(idx);
						idx = rand.Next(sz);
						a.Set(idx, false);
						b.FastClear(idx);
						idx = rand.Next(sz);
						a.Set(idx, !a.Get(idx));
						b.FastFlip(idx);
						
						bool val = b.FlipAndGet(idx);
						bool val2 = b.FlipAndGet(idx);
						Assert.IsTrue(val != val2);
						
						val = b.GetAndSet(idx);
						Assert.IsTrue(val2 == val);
						Assert.IsTrue(b.Get(idx));
						
						if (!val)
							b.FastClear(idx);
						Assert.IsTrue(b.Get(idx) == val);
					}
				}
				
				// test that the various ways of accessing the bits are equivalent
				DoGet(a, b);
				
                // {{dougsale-2.4.0}}
                //
                // Java's java.util.BitSet automatically grows as needed - i.e., when a bit is referenced beyond
                // the size of the BitSet, an exception isn't thrown - rather, the set grows to the size of the 
                // referenced bit.
                //
                // System.Collections.BitArray does not have this feature, and thus I've faked it here by
                // "growing" the array explicitly when necessary (creating a new instance of the appropriate size
                // and setting the appropriate bits).
                //

                // test ranges, including possible extension
                int fromIndex, toIndex;
                fromIndex = rand.Next(sz + 80);
                toIndex = fromIndex + rand.Next((sz >> 1) + 1);

                // {{dougsale-2.4.0}}:
                // The following commented-out, compound statement's 'for loop' implicitly grows the Java BitSets 'a'
                // and 'aa' to the same cardinality as 'j+1' when 'a.Count < j+1' and 'fromIndex < toIndex':
                //BitArray aa = (BitArray)a.Clone(); for (int j = fromIndex; j < toIndex; j++) aa.Set(j, !a.Get(j));
                // So, if necessary, lets explicitly grow 'a' now; then 'a' and its clone, 'aa', will be of the required size.
                if (a.Count < toIndex && fromIndex < toIndex)
                {
                    System.Collections.BitArray tmp = new System.Collections.BitArray(toIndex, false);
                    for (int k = 0; k < a.Count; k++)
                        tmp.Set(k, a.Get(k));
                    a = tmp;
                }
                // {{dougsale-2.4.0}}: now we can invoke this statement without going 'out-of-bounds'
                System.Collections.BitArray aa = (System.Collections.BitArray)a.Clone(); for (int j = fromIndex; j < toIndex; j++) aa.Set(j, !a.Get(j));
                OpenBitSet bb = (OpenBitSet)b.Clone(); bb.Flip(fromIndex, toIndex);

                DoIterate(aa, bb, mode); // a problem here is from flip or doIterate

                fromIndex = rand.Next(sz + 80);
                toIndex = fromIndex + rand.Next((sz >> 1) + 1);
                // {{dougsale-2.4.0}}:
                // The following commented-out, compound statement's 'for loop' implicitly grows the Java BitSet 'aa'
                // when 'a.Count < j+1' and 'fromIndex < toIndex'
                //aa = (BitArray)a.Clone(); for (int j = fromIndex; j < toIndex; j++) aa.Set(j, false);
                // So, if necessary, lets explicitly grow 'aa' now
                if (a.Count < toIndex && fromIndex < toIndex)
                {
                    aa = new System.Collections.BitArray(toIndex);
                    for (int k = 0; k < a.Count; k++)
                        aa.Set(k, a.Get(k));
                }
                else
                {
                    aa = (System.Collections.BitArray)a.Clone();
                }
                for (int j = fromIndex; j < toIndex; j++) aa.Set(j, false);
                bb = (OpenBitSet)b.Clone(); bb.Clear(fromIndex, toIndex);

                DoNextSetBit(aa, bb); // a problem here is from clear() or nextSetBit

                fromIndex = rand.Next(sz + 80);
                toIndex = fromIndex + rand.Next((sz >> 1) + 1);
                // {{dougsale-2.4.0}}:
                // The following commented-out, compound statement's 'for loop' implicitly grows the Java BitSet 'aa'
                // when 'a.Count < j+1' and 'fromIndex < toIndex'
                //aa = (BitArray)a.Clone(); for (int j = fromIndex; j < toIndex; j++) aa.Set(j, false);
                // So, if necessary, lets explicitly grow 'aa' now
                if (a.Count < toIndex && fromIndex < toIndex)
                {
                    aa = new System.Collections.BitArray(toIndex);
                    for (int k = 0; k < a.Count; k++)
                        aa.Set(k, a.Get(k));
                }
                else
                {
                    aa = (System.Collections.BitArray)a.Clone();
                }
                for (int j = fromIndex; j < toIndex; j++) aa.Set(j, true);
                bb = (OpenBitSet)b.Clone(); bb.Set(fromIndex, toIndex);
				
				DoNextSetBit(aa, bb); // a problem here is from set() or nextSetBit     
				
				
				if (a0 != null)
				{
                    Assert.AreEqual(a.Equals(a0), b.Equals(b0));

                    Assert.AreEqual(SupportClass.BitSetSupport.Cardinality(a), b.Cardinality());

                    // {{dougsale-2.4.0}}
                    //
                    // The Java code used java.util.BitSet, which grows as needed.
                    // When a bit, outside the dimension of the set is referenced,
                    // the set automatically grows to the necessary size.  The
                    // new entries default to false.
                    //
                    // BitArray does not grow automatically and is not growable.
                    // Thus when BitArray instances of mismatched cardinality
                    // interact, we must first explicitly "grow" the smaller one.
                    //
                    // This growth is acheived by creating a new instance of the
                    // required size and copying the appropriate values.
                    //

                    //BitArray a_and = (BitArray)a.Clone(); a_and.And(a0);
                    //BitArray a_or = (BitArray)a.Clone(); a_or.Or(a0);
                    //BitArray a_xor = (BitArray)a.Clone(); a_xor.Xor(a0);
                    //BitArray a_andn = (BitArray)a.Clone(); for (int j = 0; j < a_andn.Count; j++) if (a0.Get(j)) a_andn.Set(j, false);

                    System.Collections.BitArray a_and;
                    System.Collections.BitArray a_or;
                    System.Collections.BitArray a_xor;
                    System.Collections.BitArray a_andn;

                    if (a.Count < a0.Count)
                    {
                        // the Java code would have implicitly resized 'a_and', 'a_or', 'a_xor', and 'a_andn'
                        // in this case, so we explicitly create a resized stand-in for 'a' here, allowing for
                        // a to keep its original size while 'a_and', 'a_or', 'a_xor', and 'a_andn' are resized
                        System.Collections.BitArray tmp = new System.Collections.BitArray(a0.Count, false);
                        for (int z = 0; z < a.Count; z++)
                            tmp.Set(z, a.Get(z));

                        a_and = (System.Collections.BitArray)tmp.Clone(); a_and.And(a0);
                        a_or = (System.Collections.BitArray)tmp.Clone(); a_or.Or(a0);
                        a_xor = (System.Collections.BitArray)tmp.Clone(); a_xor.Xor(a0);
                        a_andn = (System.Collections.BitArray)tmp.Clone(); for (int j = 0; j < a_andn.Count; j++) if (a0.Get(j)) a_andn.Set(j, false);
                    }
                    else if (a.Count > a0.Count)
                    {
                        // the Java code would have implicitly resized 'a0' in this case, so
                        // we explicitly do so here:
                        System.Collections.BitArray tmp = new System.Collections.BitArray(a.Count, false);
                        for (int z = 0; z < a0.Count; z++)
                            tmp.Set(z, a0.Get(z));
                        a0 = tmp;

                        a_and = (System.Collections.BitArray)a.Clone(); a_and.And(a0);
                        a_or = (System.Collections.BitArray)a.Clone(); a_or.Or(a0);
                        a_xor = (System.Collections.BitArray)a.Clone(); a_xor.Xor(a0);
                        a_andn = (System.Collections.BitArray)a.Clone(); for (int j = 0; j < a_andn.Count; j++) if (a0.Get(j)) a_andn.Set(j, false);
                    }
                    else
                    {
                        // 'a' and 'a0' are the same size, no explicit growing necessary
                        a_and = (System.Collections.BitArray)a.Clone(); a_and.And(a0);
                        a_or = (System.Collections.BitArray)a.Clone(); a_or.Or(a0);
                        a_xor = (System.Collections.BitArray)a.Clone(); a_xor.Xor(a0);
                        a_andn = (System.Collections.BitArray)a.Clone(); for (int j = 0; j < a_andn.Count; j++) if (a0.Get(j)) a_andn.Set(j, false);
                    }

                    OpenBitSet b_and = (OpenBitSet)b.Clone(); Assert.AreEqual(b, b_and); b_and.And(b0);
                    OpenBitSet b_or = (OpenBitSet)b.Clone(); b_or.Or(b0);
                    OpenBitSet b_xor = (OpenBitSet)b.Clone(); b_xor.Xor(b0);
                    OpenBitSet b_andn = (OpenBitSet)b.Clone(); b_andn.AndNot(b0);

                    DoIterate(a_and, b_and, mode);
                    DoIterate(a_or, b_or, mode);
                    DoIterate(a_xor, b_xor, mode);
                    DoIterate(a_andn, b_andn, mode);

                    Assert.AreEqual(SupportClass.BitSetSupport.Cardinality(a_and), b_and.Cardinality());
                    Assert.AreEqual(SupportClass.BitSetSupport.Cardinality(a_or), b_or.Cardinality());
                    Assert.AreEqual(SupportClass.BitSetSupport.Cardinality(a_xor), b_xor.Cardinality());
                    Assert.AreEqual(SupportClass.BitSetSupport.Cardinality(a_andn), b_andn.Cardinality());

                    // test non-mutating popcounts
                    Assert.AreEqual(b_and.Cardinality(), OpenBitSet.IntersectionCount(b, b0));
                    Assert.AreEqual(b_or.Cardinality(), OpenBitSet.UnionCount(b, b0));
                    Assert.AreEqual(b_xor.Cardinality(), OpenBitSet.XorCount(b, b0));
                    Assert.AreEqual(b_andn.Cardinality(), OpenBitSet.AndNotCount(b, b0));
                }
				
				a0 = a;
				b0 = b;
			}
		}
		
		// large enough to flush obvious bugs, small enough to run in <.5 sec as part of a
		// larger testsuite.
		[Test]
		public virtual void  TestSmall()
		{
			rand = NewRandom();
			DoRandomSets(1200, 1000, 1);
			DoRandomSets(1200, 1000, 2);
		}
		
		[Test]
		public virtual void  TestBig()
		{
			// uncomment to run a bigger test (~2 minutes).
			// rand = newRandom();
			// doRandomSets(2000,200000, 1);
			// doRandomSets(2000,200000, 2);
		}
		
		[Test]
		public virtual void  TestEquals()
		{
			rand = NewRandom();
			OpenBitSet b1 = new OpenBitSet(1111);
			OpenBitSet b2 = new OpenBitSet(2222);
			Assert.IsTrue(b1.Equals(b2));
			Assert.IsTrue(b2.Equals(b1));
			b1.Set(10);
			Assert.IsFalse(b1.Equals(b2));
			Assert.IsFalse(b2.Equals(b1));
			b2.Set(10);
			Assert.IsTrue(b1.Equals(b2));
			Assert.IsTrue(b2.Equals(b1));
			b2.Set(2221);
			Assert.IsFalse(b1.Equals(b2));
			Assert.IsFalse(b2.Equals(b1));
			b1.Set(2221);
			Assert.IsTrue(b1.Equals(b2));
			Assert.IsTrue(b2.Equals(b1));
			
			// try different type of object
			Assert.IsFalse(b1.Equals(new System.Object()));
		}
		
		[Test]
		public virtual void  TestBitUtils()
		{
			rand = NewRandom();
			long num = 100000;
			Assert.AreEqual(5, BitUtil.Ntz(num));
			Assert.AreEqual(5, BitUtil.Ntz2(num));
			Assert.AreEqual(5, BitUtil.Ntz3(num));
			
			num = 10;
			Assert.AreEqual(1, BitUtil.Ntz(num));
			Assert.AreEqual(1, BitUtil.Ntz2(num));
			Assert.AreEqual(1, BitUtil.Ntz3(num));
			
			for (int i = 0; i < 64; i++)
			{
				num = 1L << i;
				Assert.AreEqual(i, BitUtil.Ntz(num));
				Assert.AreEqual(i, BitUtil.Ntz2(num));
				Assert.AreEqual(i, BitUtil.Ntz3(num));
			}
		}
	}
}