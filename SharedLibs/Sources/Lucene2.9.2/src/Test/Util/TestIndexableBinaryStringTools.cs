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
using System.Collections.Generic;

using NUnit.Framework;

namespace Lucene.Net.Util
{
	
    [TestFixture]
	public class TestIndexableBinaryStringTools:LuceneTestCase
	{
		private const int NUM_RANDOM_TESTS = 20000;
		private const int MAX_RANDOM_BINARY_LENGTH = 300;
		
        [Test]
		public virtual void  TestSingleBinaryRoundTrip()
		{
            byte[] binary = new byte[] {(byte)0x23, (byte)0x98, (byte)0x13, (byte)0xE4, (byte)0x76, (byte)0x41, (byte)0xB2, (byte)0xC9, (byte)0x7F, (byte)0x0A, (byte)0xA6, (byte)0xD8 };

            List<byte> binaryBuf = new List<byte>(binary);
            List<char> encoded = IndexableBinaryStringTools.Encode(binaryBuf);
            List<byte> decoded = IndexableBinaryStringTools.Decode(encoded);
            Assert.AreEqual(binaryBuf, decoded, "Round trip decode/decode returned different results:" + System.Environment.NewLine + "original: " + BinaryDump(binaryBuf) + System.Environment.NewLine + " encoded: " + CharArrayDump(encoded) + System.Environment.NewLine + " decoded: " + BinaryDump(decoded));

		}
		
        [Test]
		public virtual void  TestEncodedSortability()
		{
            System.Random random = NewRandom();
            byte[] originalArray1 = new byte[MAX_RANDOM_BINARY_LENGTH];
            List<byte> originalBuf1 = new List<byte>(originalArray1);
            char[] originalString1 = new char[MAX_RANDOM_BINARY_LENGTH];
            List<char> originalStringBuf1 = new List<char>(originalString1);
            char[] encoded1 = new char[IndexableBinaryStringTools.GetEncodedLength(originalBuf1)];
            List<char> encodedBuf1 = new List<char>(encoded1);
            byte[] original2 = new byte[MAX_RANDOM_BINARY_LENGTH];
            List<byte> originalBuf2 = new List<byte>(original2);
            char[] originalString2 = new char[MAX_RANDOM_BINARY_LENGTH];
            List<char> originalStringBuf2 = new List<char>(originalString2);
            char[] encoded2 = new char[IndexableBinaryStringTools.GetEncodedLength(originalBuf2)];
            List<char> encodedBuf2 = new List<char>(encoded2);
            for (int testNum = 0; testNum < NUM_RANDOM_TESTS; ++testNum)
            {
                int numBytes1 = random.Next(MAX_RANDOM_BINARY_LENGTH - 1) + 1; // Min == 1
                
                for (int byteNum = 0; byteNum < numBytes1; ++byteNum)
                {
                    int randomInt = random.Next(0x100);
                    originalArray1[byteNum] = (byte) randomInt;
                    originalString1[byteNum] = (char) randomInt;
                }
                
                int numBytes2 = random.Next(MAX_RANDOM_BINARY_LENGTH - 1) + 1; // Min == 1
                for (int byteNum = 0; byteNum < numBytes2; ++byteNum)
                {
                    int randomInt = random.Next(0x100);
                    original2[byteNum] = (byte) randomInt;
                    originalString2[byteNum] = (char) randomInt;
                }
                // put in strings to compare ordinals
                string orgStrBuf1 = new string(originalStringBuf1.ToArray());
                string orgStrBuf2 = new string(originalStringBuf2.ToArray());

                int originalComparison = string.CompareOrdinal(orgStrBuf1, orgStrBuf2);
                originalComparison = originalComparison < 0 ? -1 : (originalComparison > 0 ? 1 : 0);
                
                IndexableBinaryStringTools.Encode(originalBuf1, encodedBuf1);
                IndexableBinaryStringTools.Encode(originalBuf2, encodedBuf2);

                // put in strings to compare ordinals
                string encBuf1 = new string(encodedBuf1.ToArray());
                string encBuf2 = new string(encodedBuf2.ToArray());

                int encodedComparison = string.CompareOrdinal(encBuf1, encBuf2);
                encodedComparison = encodedComparison < 0?- 1:(encodedComparison > 0?1:0);
                
                Assert.AreEqual(originalComparison, encodedComparison, "Test #" + (testNum + 1) + ": Original bytes and encoded chars compare differently:" + System.Environment.NewLine + " binary 1: " + BinaryDump(originalBuf1) + System.Environment.NewLine + " binary 2: " + BinaryDump(originalBuf2) + System.Environment.NewLine + "encoded 1: " + CharArrayDump(encodedBuf1) + System.Environment.NewLine + "encoded 2: " + CharArrayDump(encodedBuf2) + System.Environment.NewLine);
            }
		}
		
		[Test]
		public virtual void  TestEmptyInput()
		{
			byte[] binary = new byte[0];
            List<char> encoded = IndexableBinaryStringTools.Encode(new List<byte>(binary));
			List<byte> decoded = IndexableBinaryStringTools.Decode(encoded);
			Assert.IsNotNull(decoded, "decode() returned null");
			Assert.AreEqual(decoded.Capacity, 0, "decoded empty input was not empty");
		}
		
        [Test]
		public virtual void  TestAllNullInput()
		{
			byte[] binary = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
            List<byte> binaryBuf = new List<byte>(binary);
            List<char> encoded = IndexableBinaryStringTools.Encode(binaryBuf);
			Assert.IsNotNull(encoded, "encode() returned null");
            List<byte> decodedBuf = IndexableBinaryStringTools.Decode(encoded);
			Assert.IsNotNull(decodedBuf, "decode() returned null");
			Assert.AreEqual(binaryBuf, decodedBuf, "Round trip decode/decode returned different results:" + System.Environment.NewLine + "  original: " + BinaryDump(binaryBuf) + System.Environment.NewLine + "decodedBuf: " + BinaryDump(decodedBuf));
		}
		
        [Test]
		public virtual void  TestRandomBinaryRoundTrip()
		{
			System.Random random = NewRandom();
			byte[] binary = new byte[MAX_RANDOM_BINARY_LENGTH];
            List<byte> binaryBuf = new List<byte>(binary);
			char[] encoded = new char[IndexableBinaryStringTools.GetEncodedLength(binaryBuf)];
            List<char> encodedBuf = new List<char>(encoded);
			byte[] decoded = new byte[MAX_RANDOM_BINARY_LENGTH];
            List<byte> decodedBuf = new List<byte>(decoded);
			for (int testNum = 0; testNum < NUM_RANDOM_TESTS; ++testNum)
			{
				int numBytes = random.Next(MAX_RANDOM_BINARY_LENGTH - 1) + 1; // Min == 1
				for (int byteNum = 0; byteNum < numBytes; ++byteNum)
				{
					binary[byteNum] = (byte) random.Next(0x100);
				}
				IndexableBinaryStringTools.Encode(binaryBuf, encodedBuf);
				IndexableBinaryStringTools.Decode(encodedBuf, decodedBuf);
				Assert.AreEqual(binaryBuf, decodedBuf, "Test #" + (testNum + 1) + ": Round trip decode/decode returned different results:" + System.Environment.NewLine + "  original: " + BinaryDump(binaryBuf) + System.Environment.NewLine + "encodedBuf: " + CharArrayDump(encodedBuf) + System.Environment.NewLine + "decodedBuf: " + BinaryDump(decodedBuf));
			}
		}
		
		public virtual System.String BinaryDump(List<byte> binaryBuf)
		{
			System.Text.StringBuilder buf = new System.Text.StringBuilder();
			for (int byteNum = 0; byteNum < binaryBuf.Count; ++byteNum)
			{
				System.String hex = System.Convert.ToString((int) binaryBuf[byteNum] & 0xFF, 16);
				if (hex.Length == 1)
				{
					buf.Append('0');
				}
				buf.Append(hex.ToUpper());
				if (byteNum < binaryBuf.Count - 1)
				{
					buf.Append(' ');
				}
			}
			return buf.ToString();
		}
		
		public virtual System.String CharArrayDump(List<char> charBuf)
		{
			System.Text.StringBuilder buf = new System.Text.StringBuilder();
			for (int charNum = 0; charNum < charBuf.Count; ++charNum)
			{
				System.String hex = System.Convert.ToString((int) charBuf[charNum], 16);
				for (int digit = 0; digit < 4 - hex.Length; ++digit)
				{
					buf.Append('0');
				}
				buf.Append(hex.ToUpper());
				if (charNum < charBuf.Count - 1)
				{
					buf.Append(' ');
				}
			}
			return buf.ToString();
		}
	}
}