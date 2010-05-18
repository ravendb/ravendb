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

namespace Lucene.Net.Util
{
	
	/// <version>  $Id$
	/// </version>
	[TestFixture]
	public class TestSmallFloat:LuceneTestCase
	{
		
		// original lucene byteToFloat
		internal static float Orig_byteToFloat(byte b)
		{
			if (b == 0)
			// zero is a special case
				return 0.0f;
			int mantissa = b & 7;
			int exponent = (b >> 3) & 31;
			int bits = ((exponent + (63 - 15)) << 24) | (mantissa << 21);
			return BitConverter.ToSingle(BitConverter.GetBytes(bits), 0);
		}
		
		// original lucene floatToByte
		internal static sbyte Orig_floatToByte(float f)
		{
			if (f < 0.0f)
			// round negatives up to zero
				f = 0.0f;
			
			if (f == 0.0f)
			// zero is a special case
				return 0;
			
			int bits = BitConverter.ToInt32(BitConverter.GetBytes(f), 0);
			int mantissa = (bits & 0xffffff) >> 21;
			int exponent = (((bits >> 24) & 0x7f) - 63) + 15;
			
			if (exponent > 31)
			{
				// overflow: use max value
				exponent = 31;
				mantissa = 7;
			}
			
			if (exponent < 0)
			{
				// underflow: use min value
				exponent = 0;
				mantissa = 1;
			}
			
			return (sbyte) ((exponent << 3) | mantissa); // pack into a byte
		}
		
		[Test]
		public virtual void  TestByteToFloat()
		{
			for (int i = 0; i < 256; i++)
			{
				float f1 = Orig_byteToFloat((byte) i);
				float f2 = SmallFloat.ByteToFloat((byte) i, 3, 15);
				float f3 = SmallFloat.Byte315ToFloat((byte) i);
				Assert.AreEqual(f1, f2, 0.0);
				Assert.AreEqual(f2, f3, 0.0);
				
				float f4 = SmallFloat.ByteToFloat((byte) i, 5, 2);
				float f5 = SmallFloat.Byte52ToFloat((byte) i);
				Assert.AreEqual(f4, f5, 0.0);
			}
		}
		
		[Test]
		public virtual void  TestFloatToByte()
		{
			System.Random rand = NewRandom();
			// up iterations for more exhaustive test after changing something
			for (int i = 0; i < 100000; i++)
			{
				float f = BitConverter.ToSingle(BitConverter.GetBytes(rand.Next()), 0);
				if (f != f)
					continue; // skip NaN
				sbyte b1 = Orig_floatToByte(f);
				sbyte b2 = SmallFloat.FloatToByte(f, 3, 15);
				sbyte b3 = SmallFloat.FloatToByte315(f);
				Assert.AreEqual(b1, b2);
				Assert.AreEqual(b2, b3);
				
				sbyte b4 = SmallFloat.FloatToByte(f, 5, 2);
				sbyte b5 = SmallFloat.FloatToByte52(f);
				Assert.AreEqual(b4, b5);
			}
		}
		
		/// <summary> 
		/// // Do an exhaustive test of all possible floating point values
		/// // for the 315 float against the original norm encoding in Similarity.
		/// // Takes 75 seconds on my Pentium4 3GHz, with Java5 -server
		/// public void testAllFloats() {
		/// for(int i = Integer.MIN_VALUE;;i++) {
		/// float f = Float.intBitsToFloat(i);
		/// if (f==f) { // skip non-numbers
		/// byte b1 = orig_floatToByte(f);
		/// byte b2 = SmallFloat.floatToByte315(f);
		/// if (b1!=b2) {
		/// TestCase.fail("Failed floatToByte315 for float " + f);
		/// }
		/// }
		/// if (i==Integer.MAX_VALUE) break;
		/// }
		/// }
		/// *
		/// </summary>
	}
}