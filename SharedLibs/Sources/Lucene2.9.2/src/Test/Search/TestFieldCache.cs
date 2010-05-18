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

using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class TestFieldCache:LuceneTestCase
	{
		protected internal IndexReader reader;
		private const int NUM_DOCS = 1000;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			RAMDirectory directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			long theLong = System.Int64.MaxValue;
			double theDouble = System.Double.MaxValue;
			sbyte theByte = (sbyte) System.SByte.MaxValue;
			short theShort = System.Int16.MaxValue;
			int theInt = System.Int32.MaxValue;
			float theFloat = System.Single.MaxValue;
			for (int i = 0; i < NUM_DOCS; i++)
			{
				Document doc = new Document();
				doc.Add(new Field("theLong", System.Convert.ToString(theLong--), Field.Store.NO, Field.Index.NOT_ANALYZED));
				doc.Add(new Field("theDouble", (theDouble--).ToString("E16"), Field.Store.NO, Field.Index.NOT_ANALYZED));
				doc.Add(new Field("theByte", System.Convert.ToString((sbyte) theByte--), Field.Store.NO, Field.Index.NOT_ANALYZED));
				doc.Add(new Field("theShort", System.Convert.ToString(theShort--), Field.Store.NO, Field.Index.NOT_ANALYZED));
				doc.Add(new Field("theInt", System.Convert.ToString(theInt--), Field.Store.NO, Field.Index.NOT_ANALYZED));
				doc.Add(new Field("theFloat", (theFloat--).ToString("E8"), Field.Store.NO, Field.Index.NOT_ANALYZED));
				writer.AddDocument(doc);
			}
			writer.Close();
			reader = IndexReader.Open(directory);
		}
		
		[Test]
		public virtual void  TestInfoStream()
		{
			try
			{
				FieldCache cache = Lucene.Net.Search.FieldCache_Fields.DEFAULT;
				System.IO.MemoryStream bos = new System.IO.MemoryStream(1024);
                System.IO.StreamWriter writer = new System.IO.StreamWriter(bos);
				cache.SetInfoStream(writer);
				double[] doubles = cache.GetDoubles(reader, "theDouble");
				float[] floats = cache.GetFloats(reader, "theDouble");
				char[] tmpChar;
				byte[] tmpByte;
                writer.Flush();
				tmpByte = bos.GetBuffer();
				tmpChar = new char[bos.Length];
				System.Array.Copy(tmpByte, 0, tmpChar, 0, tmpChar.Length);
				Assert.IsTrue(new System.String(tmpChar).IndexOf("WARNING") != - 1);
			}
			finally
			{
				Lucene.Net.Search.FieldCache_Fields.DEFAULT.PurgeAllCaches();
			}
		}
		
		[Test]
		public virtual void  Test()
		{
			FieldCache cache = Lucene.Net.Search.FieldCache_Fields.DEFAULT;
			double[] doubles = cache.GetDoubles(reader, "theDouble");
			Assert.AreSame(doubles, cache.GetDoubles(reader, "theDouble"), "Second request to cache return same array");
			Assert.AreSame(doubles, cache.GetDoubles(reader, "theDouble", Lucene.Net.Search.FieldCache_Fields.DEFAULT_DOUBLE_PARSER), "Second request with explicit parser return same array");
			Assert.IsTrue(doubles.Length == NUM_DOCS, "doubles Size: " + doubles.Length + " is not: " + NUM_DOCS);
			for (int i = 0; i < doubles.Length; i++)
			{
				Assert.IsTrue(doubles[i] == (System.Double.MaxValue - i), doubles[i] + " does not equal: " + (System.Double.MaxValue - i));
			}
			
			long[] longs = cache.GetLongs(reader, "theLong");
			Assert.AreSame(longs, cache.GetLongs(reader, "theLong"), "Second request to cache return same array");
			Assert.AreSame(longs, cache.GetLongs(reader, "theLong", Lucene.Net.Search.FieldCache_Fields.DEFAULT_LONG_PARSER), "Second request with explicit parser return same array");
			Assert.IsTrue(longs.Length == NUM_DOCS, "longs Size: " + longs.Length + " is not: " + NUM_DOCS);
			for (int i = 0; i < longs.Length; i++)
			{
				Assert.IsTrue(longs[i] == (System.Int64.MaxValue - i), longs[i] + " does not equal: " + (System.Int64.MaxValue - i));
			}
			
			sbyte[] bytes = cache.GetBytes(reader, "theByte");
			Assert.AreSame(bytes, cache.GetBytes(reader, "theByte"), "Second request to cache return same array");
			Assert.AreSame(bytes, cache.GetBytes(reader, "theByte", Lucene.Net.Search.FieldCache_Fields.DEFAULT_BYTE_PARSER), "Second request with explicit parser return same array");
			Assert.IsTrue(bytes.Length == NUM_DOCS, "bytes Size: " + bytes.Length + " is not: " + NUM_DOCS);
			for (int i = 0; i < bytes.Length; i++)
			{
				Assert.IsTrue(bytes[i] == (sbyte) ((byte) System.SByte.MaxValue - i), bytes[i] + " does not equal: " + ((byte) System.SByte.MaxValue - i));
			}
			
			short[] shorts = cache.GetShorts(reader, "theShort");
			Assert.AreSame(shorts, cache.GetShorts(reader, "theShort"), "Second request to cache return same array");
			Assert.AreSame(shorts, cache.GetShorts(reader, "theShort", Lucene.Net.Search.FieldCache_Fields.DEFAULT_SHORT_PARSER), "Second request with explicit parser return same array");
			Assert.IsTrue(shorts.Length == NUM_DOCS, "shorts Size: " + shorts.Length + " is not: " + NUM_DOCS);
			for (int i = 0; i < shorts.Length; i++)
			{
				Assert.IsTrue(shorts[i] == (short) (System.Int16.MaxValue - i), shorts[i] + " does not equal: " + (System.Int16.MaxValue - i));
			}
			
			int[] ints = cache.GetInts(reader, "theInt");
			Assert.AreSame(ints, cache.GetInts(reader, "theInt"), "Second request to cache return same array");
			Assert.AreSame(ints, cache.GetInts(reader, "theInt", Lucene.Net.Search.FieldCache_Fields.DEFAULT_INT_PARSER), "Second request with explicit parser return same array");
			Assert.IsTrue(ints.Length == NUM_DOCS, "ints Size: " + ints.Length + " is not: " + NUM_DOCS);
			for (int i = 0; i < ints.Length; i++)
			{
				Assert.IsTrue(ints[i] == (System.Int32.MaxValue - i), ints[i] + " does not equal: " + (System.Int32.MaxValue - i));
			}
			
			float[] floats = cache.GetFloats(reader, "theFloat");
			Assert.AreSame(floats, cache.GetFloats(reader, "theFloat"), "Second request to cache return same array");
			Assert.AreSame(floats, cache.GetFloats(reader, "theFloat", Lucene.Net.Search.FieldCache_Fields.DEFAULT_FLOAT_PARSER), "Second request with explicit parser return same array");
			Assert.IsTrue(floats.Length == NUM_DOCS, "floats Size: " + floats.Length + " is not: " + NUM_DOCS);
			for (int i = 0; i < floats.Length; i++)
			{
				Assert.IsTrue(floats[i] == (System.Single.MaxValue - i), floats[i] + " does not equal: " + (System.Single.MaxValue - i));
			}
		}
	}
}