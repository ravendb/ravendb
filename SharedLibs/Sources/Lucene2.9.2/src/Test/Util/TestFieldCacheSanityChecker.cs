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
using MultiReader = Lucene.Net.Index.MultiReader;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using Insanity = Lucene.Net.Util.FieldCacheSanityChecker.Insanity;
using InsanityType = Lucene.Net.Util.FieldCacheSanityChecker.InsanityType;
using FieldCache = Lucene.Net.Search.FieldCache;

namespace Lucene.Net.Util
{
	
    [TestFixture]
	public class TestFieldCacheSanityChecker:LuceneTestCase
	{
		
		protected internal IndexReader readerA;
		protected internal IndexReader readerB;
		protected internal IndexReader readerX;
		
		private const int NUM_DOCS = 1000;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			
			RAMDirectory dirA = new RAMDirectory();
			RAMDirectory dirB = new RAMDirectory();
			
			IndexWriter wA = new IndexWriter(dirA, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			IndexWriter wB = new IndexWriter(dirB, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			
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
				if (0 == i % 3)
				{
					wA.AddDocument(doc);
				}
				else
				{
					wB.AddDocument(doc);
				}
			}
			wA.Close();
			wB.Close();
			readerA = IndexReader.Open(dirA);
			readerB = IndexReader.Open(dirB);
			readerX = new MultiReader(new IndexReader[]{readerA, readerB});
		}
		
		[TearDown]
		public override void  TearDown()
		{
			base.TearDown();
			readerA.Close();
			readerB.Close();
			readerX.Close();
		}
		
		[Test]
		public virtual void  TestSanity()
		{
			FieldCache cache = Lucene.Net.Search.FieldCache_Fields.DEFAULT;
			cache.PurgeAllCaches();
			
			double[] doubles;
			int[] ints;
			
			doubles = cache.GetDoubles(readerA, "theDouble");
			doubles = cache.GetDoubles(readerA, "theDouble", Lucene.Net.Search.FieldCache_Fields.DEFAULT_DOUBLE_PARSER);
			doubles = cache.GetDoubles(readerB, "theDouble", Lucene.Net.Search.FieldCache_Fields.DEFAULT_DOUBLE_PARSER);
			
			ints = cache.GetInts(readerX, "theInt");
			ints = cache.GetInts(readerX, "theInt", Lucene.Net.Search.FieldCache_Fields.DEFAULT_INT_PARSER);
			
			// // // 
			
			Insanity[] insanity = FieldCacheSanityChecker.CheckSanity(cache.GetCacheEntries());
			
			if (0 < insanity.Length)
			{
				System.IO.StreamWriter temp_writer;
				temp_writer = new System.IO.StreamWriter(System.Console.OpenStandardError(), System.Console.Error.Encoding);
				temp_writer.AutoFlush = true;
				DumpArray(GetTestLabel() + " INSANITY", insanity, temp_writer);
			}
			
			Assert.AreEqual(0, insanity.Length, "shouldn't be any cache insanity");
			cache.PurgeAllCaches();
		}
		
		[Test]
		public virtual void  TestInsanity1()
		{
			FieldCache cache = Lucene.Net.Search.FieldCache_Fields.DEFAULT;
			cache.PurgeAllCaches();
			
			int[] ints;
			System.String[] strings;
			sbyte[] bytes;
			
			ints = cache.GetInts(readerX, "theInt", Lucene.Net.Search.FieldCache_Fields.DEFAULT_INT_PARSER);
			strings = cache.GetStrings(readerX, "theInt");
			
			// this one is ok
			bytes = cache.GetBytes(readerX, "theByte");
			
			// // // 
			
			Insanity[] insanity = FieldCacheSanityChecker.CheckSanity(cache.GetCacheEntries());
			
			Assert.AreEqual(1, insanity.Length, "wrong number of cache errors");
			Assert.AreEqual(InsanityType.VALUEMISMATCH, insanity[0].GetType(), "wrong type of cache error");
			Assert.AreEqual(2, insanity[0].GetCacheEntries().Length, "wrong number of entries in cache error");
			
			// we expect bad things, don't let tearDown complain about them
			cache.PurgeAllCaches();
		}
		
		[Test]
		public virtual void  TestInsanity2()
		{
			FieldCache cache = Lucene.Net.Search.FieldCache_Fields.DEFAULT;
			cache.PurgeAllCaches();
			
			System.String[] strings;
			sbyte[] bytes;
			
			strings = cache.GetStrings(readerA, "theString");
			strings = cache.GetStrings(readerB, "theString");
			strings = cache.GetStrings(readerX, "theString");
			
			// this one is ok
			bytes = cache.GetBytes(readerX, "theByte");
			
			
			// // // 
			
			Insanity[] insanity = FieldCacheSanityChecker.CheckSanity(cache.GetCacheEntries());
			
			Assert.AreEqual(1, insanity.Length, "wrong number of cache errors");
			Assert.AreEqual(InsanityType.SUBREADER, insanity[0].GetType(), "wrong type of cache error");
			Assert.AreEqual(3, insanity[0].GetCacheEntries().Length, "wrong number of entries in cache error");
			
			// we expect bad things, don't let tearDown complain about them
			cache.PurgeAllCaches();
		}
		
		[Test]
		public virtual void  TestInsanity3()
		{
			
			// :TODO: subreader tree walking is really hairy ... add more crazy tests.
		}
	}
}