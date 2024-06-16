/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;

namespace Lucene.Net.Spatial.Util
{
	/* BitSet of fixed length (numBits), backed by accessible
 *  ({@link #getBits}) long[], accessed with an int index,
 *  implementing Bits and DocIdSet.  Unlike {@link
 *  OpenBitSet} this bit set does not auto-expand, cannot
 *  handle long index, and does not have fastXX/XX variants
 *  (just X).
 *
 * @lucene.internal
 **/
	public class FixedBitSet : DocIdSet, IBits
	{
		private readonly BitArray bits;

		/// <summary>
		/// returns the number of 64 bit words it would take to hold numBits
		/// </summary>
		/// <param name="numBits"></param>
		/// <returns></returns>
		public static int bits2words(int numBits)
		{
			var numLong = (int)((uint)numBits >> 6);
			if ((numBits & 63) != 0)
			{
				numLong++;
			}
			return numLong;
		}

		public FixedBitSet(int numBits)
		{
            bits = new BitArray(++numBits);
		}

		/// <summary>
		/// Makes full copy.
		/// </summary>
		/// <param name="other"></param>
		public FixedBitSet(FixedBitSet other)
		{
			bits = new BitArray(other.bits);
		}

		public IBits Bits()
		{
			return this;
		}

		public int Length()
		{
			return bits.Length;
		}

		public override bool IsCacheable
		{
			get { return true; }
		}

		/// <summary>
		/// Returns number of set bits.  NOTE: this visits every
		/// long in the backing bits array, and the result is not
		/// internally cached!
		/// </summary>
		/// <returns></returns>
		public int Cardinality()
		{
			int ret = 0;
			for (var i = 0; i < bits.Length; i++)
			{
				if (bits[i]) ret++;
			}
			return ret;
		}

		public bool Get(int index)
		{
			return bits[index];
		}

		public void Set(int index)
		{
			bits.Set(index, true);
		}

		public bool GetAndSet(int index)
		{
			var ret = bits[index];
			bits.Set(index, true);
			return ret;
		}

		public void Clear(int index)
		{
			bits.Set(index, false);
		}

		public bool GetAndClear(int index)
		{
			var ret = bits[index];
			bits.Set(index, false);
			return ret;
		}

		/// <summary>
		/// Returns the index of the first set bit starting at the index specified.
		/// -1 is returned if there are no more set bits.
		/// </summary>
		/// <param name="index"></param>
		/// <returns></returns>
		public int NextSetBit(int index)
		{
			if (index >= bits.Length || index < 0)
				throw new ArgumentException("Invalid index", "index");

			for (var i = index; i < bits.Length; i++)
			{
				if (bits[i]) return i;
			}

			return -1;
		}

		/* Returns the index of the last set bit before or on the index specified.
		 *  -1 is returned if there are no more set bits.
		 */
		public int PrevSetBit(int index)
		{
			if (index >= bits.Length || index < 0)
				throw new ArgumentException("Invalid index", "index");

			for (var i = index; i >= 0; i--)
			{
				if (bits[i]) return i;
			}

			return -1;
		}

		/* Does in-place OR of the bits provided by the
		 *  iterator. */
		//public void Or(DocIdSetIterator iter)
		//{
		//    if (iter is OpenBitSetIterator && iter.DocID() == -1)
		//    {
		//        var obs = (OpenBitSetIterator)iter;
		//        Or(obs.arr, obs.words);
		//        // advance after last doc that would be accepted if standard
		//        // iteration is used (to exhaust it):
		//        obs.Advance(bits.Length);
		//    }
		//    else
		//    {
		//        int doc;
		//        while ((doc = iter.NextDoc()) < bits.Length)
		//        {
		//            Set(doc);
		//        }
		//    }
		//}

		/* this = this OR other */
		public void Or(FixedBitSet other)
		{
			Or(other.bits, other.bits.Length);
		}

		private void Or(BitArray otherArr, int otherLen)
		{
			var thisArr = this.bits;
			int pos = Math.Min(thisArr.Length, otherLen);
			while (--pos >= 0)
			{
				thisArr[pos] |= otherArr[pos];
			}
		}

		/* Does in-place AND of the bits provided by the
		 *  iterator. */
		//public void And(DocIdSetIterator iter)
		//{
		//    if (iter is OpenBitSetIterator && iter.DocID() == -1)
		//    {
		//        var obs = (OpenBitSetIterator)iter;
		//        And(obs.arr, obs.words);
		//        // advance after last doc that would be accepted if standard
		//        // iteration is used (to exhaust it):
		//        obs.Advance(bits.Length);
		//    }
		//    else
		//    {
		//        if (bits.Length == 0) return;
		//        int disiDoc, bitSetDoc = NextSetBit(0);
		//        while (bitSetDoc != -1 && (disiDoc = iter.Advance(bitSetDoc)) < bits.Length)
		//        {
		//            Clear(bitSetDoc, disiDoc);
		//            disiDoc++;
		//            bitSetDoc = (disiDoc < bits.Length) ? NextSetBit(disiDoc) : -1;
		//        }
		//        if (bitSetDoc != -1)
		//        {
		//            Clear(bitSetDoc, bits.Length);
		//        }
		//    }
		//}

		/* this = this AND other */
		public void And(FixedBitSet other)
		{
			And(other.bits, other.bits.Length);
		}

		private void And(BitArray otherArr, int otherLen)
		{
			var thisArr = this.bits;
			int pos = Math.Min(thisArr.Length, otherLen);
			while (--pos >= 0)
			{
				thisArr[pos] &= otherArr[pos];
			}
			if (thisArr.Length > otherLen)
			{
				for (var i = otherLen; i < thisArr.Length; i++)
				{
					thisArr[i] = false;
				}
			}
		}

		/* Does in-place AND NOT of the bits provided by the
		 *  iterator. */
		//public void AndNot(DocIdSetIterator iter)
		//{
		//    var obs = iter as OpenBitSetIterator;
		//    if (obs != null && iter.DocID() == -1)
		//    {
		//        AndNot(obs.arr, obs.words);
		//        // advance after last doc that would be accepted if standard
		//        // iteration is used (to exhaust it):
		//        obs.Advance(bits.Length);
		//    }
		//    else
		//    {
		//        int doc;
		//        while ((doc = iter.NextDoc()) < bits.Length)
		//        {
		//            Clear(doc);
		//        }
		//    }
		//}

		/* this = this AND NOT other */
		public void AndNot(FixedBitSet other)
		{
			AndNot(other.bits, other.bits.Length);
		}

		private void AndNot(BitArray otherArr, int otherLen)
		{
			var thisArr = this.bits;
			int pos = Math.Min(thisArr.Length, otherLen);
			while (--pos >= 0)
			{
				thisArr[pos] &= !otherArr[pos];
			}
		}

		// NOTE: no .isEmpty() here because that's trappy (ie,
		// typically isEmpty is low cost, but this one wouldn't
		// be)

		/* Flips a range of bits
		 *
		 * @param startIndex lower index
		 * @param endIndex one-past the last bit to flip
		 */
		//      public void Flip(int startIndex, int endIndex) {
		//  Debug.Assert(startIndex >= 0 && startIndex < numBits);
		//  Debug.Assert(endIndex >= 0 && endIndex <= numBits);
		//  if (endIndex <= startIndex) {
		//    return;
		//  }

		//  int startWord = startIndex >> 6;
		//  int endWord = (endIndex-1) >> 6;

		//  /* Grrr, java shifting wraps around so -1L>>>64 == -1
		//   * for that reason, make sure not to use endmask if the bits to flip will
		//   * be zero in the last word (redefine endWord to be the last changed...)
		//  long startmask = -1L << (startIndex & 0x3f);     // example: 11111...111000
		//  long endmask = -1L >>> (64-(endIndex & 0x3f));   // example: 00111...111111
		//  ***/

		//  long startmask = -1L << startIndex;
		//  long endmask =  -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

		//  if (startWord == endWord) {
		//    bits[startWord] ^= (startmask & endmask);
		//    return;
		//  }

		//  bits[startWord] ^= startmask;

		//  for (var i=startWord+1; i<endWord; i++) {
		//    bits[i] = ~bits[i];
		//  }

		//  bits[endWord] ^= endmask;
		//}

		/* Sets a range of bits
		 *
		 * @param startIndex lower index
		 * @param endIndex one-past the last bit to set
		 */
		public void Set(int startIndex, int endIndex)
		{
			// Naive implementation
			for (int i = startIndex; i < endIndex; i++)
			{
				Set(i);
			}
		}

		//      public void Set(int startIndex, int endIndex) {
		//  Debug.Assert(startIndex >= 0 && startIndex < numBits);
		//  Debug.Assert(endIndex >= 0 && endIndex <= numBits);
		//  if (endIndex <= startIndex) {
		//    return;
		//  }

		//  int startWord = startIndex >> 6;
		//  int endWord = (endIndex-1) >> 6;

		//  long startmask = -1L << startIndex;
		//  long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

		//  if (startWord == endWord) {
		//    bits[startWord] |= (startmask & endmask);
		//    return;
		//  }

		//  bits[startWord] |= startmask;
		//  Arrays.Fill(bits, startWord+1, endWord, -1L);
		//  bits[endWord] |= endmask;
		//}

		/* Clears a range of bits.
		 *
		 * @param startIndex lower index
		 * @param endIndex one-past the last bit to clear
		 */
		public void Clear(int startIndex, int endIndex)
		{
			for (int i = startIndex; i < endIndex; i++)
			{
				Clear(i);
			}
		}

		//@Override
		public FixedBitSet Clone()
		{
			return new FixedBitSet(this);
		}

		/* returns true if both sets have the same bits set */
		public override bool Equals(Object o)
		{
			if (this == o)
			{
				return true;
			}

			var other = o as FixedBitSet;
			if (other == null)
			{
				return false;
			}

			var thisLength = bits.Length;
			var otherLength = other.bits.Length;

			if (thisLength != otherLength)
			{
				return false;
			}

			for (var i = 0; i < thisLength; i++)
			{
				if (bits[i] != other.bits[i])
				{
					return false;
				}
			}

			return true;
		}

		public override int GetHashCode()
		{
			int hash = 17;
			foreach (var bit in bits)
			{
				hash = hash * 23 + bit.GetHashCode();
			}
			return hash;
		}

		public override DocIdSetIterator Iterator(IState state)
		{
			return new FixedBitSetIterator(this);
		}

		/// <summary>
		/// A FixedBitSet Iterator implementation
		/// </summary>
		public class FixedBitSetIterator : DocIdSetIterator
		{
			private int curDocId = -1;
			private readonly IEnumerator enumerator;

			public FixedBitSetIterator(FixedBitSet bitset)
			{
				enumerator = bitset.bits.GetEnumerator();
			}

			public override int DocID()
			{
				return curDocId;
			}

			public override int NextDoc(IState state)
			{
				while (enumerator.MoveNext())
				{
					++curDocId;
					if ((bool)enumerator.Current) return curDocId;
				}
				return curDocId = NO_MORE_DOCS;
			}

			public override int Advance(int target, IState state)
			{
				int doc;
				while ((doc = NextDoc(state)) < target)
				{
				}
				return doc;
			}
		}
	}
}
