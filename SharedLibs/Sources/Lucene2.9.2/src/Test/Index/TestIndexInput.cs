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

using IndexInput = Lucene.Net.Store.IndexInput;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestIndexInput:LuceneTestCase
	{
		[Test]
		public virtual void  TestRead()
		{
			IndexInput is_Renamed = new MockIndexInput(new byte[]{(byte) (0x80), (byte) (0x01), (byte) (0xFF), (byte) (0x7F), (byte) (0x80), (byte) (0x80), (byte) (0x01), (byte) (0x81), (byte) (0x80), (byte) (0x01), (byte) (0x06), (byte) 'L', (byte) 'u', (byte) 'c', (byte) 'e', (byte) 'n', (byte) 'e', (byte) (0x02), (byte) (0xC2), (byte) (0xBF), (byte) (0x0A), (byte) 'L', (byte) 'u', (byte) (0xC2), (byte) (0xBF), (byte) 'c', (byte) 'e', (byte) (0xC2), (byte) (0xBF), (byte) 'n', (byte) 'e', (byte) (0x03), (byte) (0xE2), (byte) (0x98), (byte) (0xA0), (byte) (0x0C), (byte) 'L', (byte) 'u', (byte) (0xE2), (byte) (0x98), (byte) (0xA0), (byte) 'c', (byte) 'e', (byte) (0xE2), (byte) (0x98), (byte) (0xA0), (byte) 'n', (byte) 'e', (byte) (0x04), (byte) (0xF0), (byte) (0x9D), (byte) (0x84), (byte) (0x9E), (byte) (0x08), (byte) (0xF0), (byte) (0x9D), (byte) (0x84), (byte) (0x9E), (byte) (0xF0), (byte) (0x9D), (byte) (0x85), (byte) (0xA0), (byte) (0x0E), (byte) 'L', (byte) 'u', (byte) (0xF0), (byte) (0x9D), (byte) (0x84), (byte) (0x9E), (byte) 'c', (byte) 'e', (byte) (0xF0), (byte) (0x9D), (byte) (0x85), (byte) 
				(0xA0), (byte) 'n', (byte) 'e', (byte) (0x01), (byte) (0x00), (byte) (0x08), (byte) 'L', (byte) 'u', (byte) (0x00), (byte) 'c', (byte) 'e', (byte) (0x00), (byte) 'n', (byte) 'e'});
			
			Assert.AreEqual(128, is_Renamed.ReadVInt());
			Assert.AreEqual(16383, is_Renamed.ReadVInt());
			Assert.AreEqual(16384, is_Renamed.ReadVInt());
			Assert.AreEqual(16385, is_Renamed.ReadVInt());
			Assert.AreEqual("Lucene", is_Renamed.ReadString());
			
			Assert.AreEqual("\u00BF", is_Renamed.ReadString());
			Assert.AreEqual("Lu\u00BFce\u00BFne", is_Renamed.ReadString());
			
			Assert.AreEqual("\u2620", is_Renamed.ReadString());
			Assert.AreEqual("Lu\u2620ce\u2620ne", is_Renamed.ReadString());
			
			Assert.AreEqual("\uD834\uDD1E", is_Renamed.ReadString());
			Assert.AreEqual("\uD834\uDD1E\uD834\uDD60", is_Renamed.ReadString());
			Assert.AreEqual("Lu\uD834\uDD1Ece\uD834\uDD60ne", is_Renamed.ReadString());
			
			Assert.AreEqual("\u0000", is_Renamed.ReadString());
			Assert.AreEqual("Lu\u0000ce\u0000ne", is_Renamed.ReadString());
		}
		
		/// <summary> Expert
		/// 
		/// </summary>
		/// <throws>  IOException </throws>
		[Test]
		public virtual void  TestSkipChars()
		{
			byte[] bytes = new byte[]{(byte) (0x80), (byte) (0x01), (byte) (0xFF), (byte) (0x7F), (byte) (0x80), (byte) (0x80), (byte) (0x01), (byte) (0x81), (byte) (0x80), (byte) (0x01), (byte) (0x06), (byte) 'L', (byte) 'u', (byte) 'c', (byte) 'e', (byte) 'n', (byte) 'e'};
			System.String utf8Str = "\u0634\u1ea1";
			byte[] utf8Bytes = System.Text.Encoding.GetEncoding("UTF-8").GetBytes(utf8Str);
			byte[] theBytes = new byte[bytes.Length + 1 + utf8Bytes.Length];
			Array.Copy(bytes, 0, theBytes, 0, bytes.Length);
			theBytes[bytes.Length] = (byte) utf8Str.Length; //Add in the number of chars we are storing, which should fit in a byte for this test 
			Array.Copy(utf8Bytes, 0, theBytes, bytes.Length + 1, utf8Bytes.Length);
			IndexInput is_Renamed = new MockIndexInput(theBytes);
			Assert.AreEqual(128, is_Renamed.ReadVInt());
			Assert.AreEqual(16383, is_Renamed.ReadVInt());
			Assert.AreEqual(16384, is_Renamed.ReadVInt());
			Assert.AreEqual(16385, is_Renamed.ReadVInt());
			int charsToRead = is_Renamed.ReadVInt(); //number of chars in the Lucene string
			Assert.IsTrue(0x06 == charsToRead, 0x06 + " does not equal: " + charsToRead);
			is_Renamed.SkipChars(3);
			char[] chars = new char[3]; //there should be 6 chars remaining
			is_Renamed.ReadChars(chars, 0, 3);
			System.String tmpStr = new System.String(chars);
			Assert.IsTrue(tmpStr.Equals("ene") == true, tmpStr + " is not equal to " + "ene");
			//Now read the UTF8 stuff
			charsToRead = is_Renamed.ReadVInt() - 1; //since we are skipping one
			is_Renamed.SkipChars(1);
			Assert.IsTrue(utf8Str.Length - 1 == charsToRead, utf8Str.Length - 1 + " does not equal: " + charsToRead);
			chars = new char[charsToRead];
			is_Renamed.ReadChars(chars, 0, charsToRead);
			tmpStr = new System.String(chars);
			Assert.IsTrue(tmpStr.Equals(utf8Str.Substring(1)) == true, tmpStr + " is not equal to " + utf8Str.Substring(1));
		}
	}
}