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

using SimpleAnalyzer = Lucene.Net.Analysis.SimpleAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
    [TestFixture]
	public class BaseTestRangeFilter:LuceneTestCase
	{
		private void  InitBlock()
		{
			signedIndex = new TestIndex(this, System.Int32.MaxValue, System.Int32.MinValue, true);
			unsignedIndex = new TestIndex(this, System.Int32.MaxValue, 0, false);
		}
		
		public const bool F = false;
		public const bool T = true;
		
		protected internal System.Random rand;
		
		/// <summary> Collation interacts badly with hyphens -- collation produces different
		/// ordering than Unicode code-point ordering -- so two indexes are created:
		/// one which can't have negative random integers, for testing collated 
		/// ranges, and the other which can have negative random integers, for all
		/// other tests. 
		/// </summary>
		internal class TestIndex
		{
			private void  InitBlock(BaseTestRangeFilter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private BaseTestRangeFilter enclosingInstance;
			public BaseTestRangeFilter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal int maxR;
			internal int minR;
			internal bool allowNegativeRandomInts;
			internal RAMDirectory index = new RAMDirectory();
			
			internal TestIndex(BaseTestRangeFilter enclosingInstance, int minR, int maxR, bool allowNegativeRandomInts)
			{
				InitBlock(enclosingInstance);
				this.minR = minR;
				this.maxR = maxR;
				this.allowNegativeRandomInts = allowNegativeRandomInts;
			}
		}
		internal TestIndex signedIndex;
		internal TestIndex unsignedIndex;
		
		internal int minId = 0;
		internal int maxId = 10000;
		
		internal static readonly int intLength = System.Convert.ToString(System.Int32.MaxValue).Length;
		
		/// <summary> a simple padding function that should work with any int</summary>
		public static System.String Pad(int n)
		{
			System.Text.StringBuilder b = new System.Text.StringBuilder(40);
			System.String p = "0";
			if (n < 0)
			{
				p = "-";
				n = System.Int32.MaxValue + n + 1;
			}
			b.Append(p);
			System.String s = System.Convert.ToString(n);
			for (int i = s.Length; i <= intLength; i++)
			{
				b.Append("0");
			}
			b.Append(s);
			
			return b.ToString();
		}
		
		public BaseTestRangeFilter(System.String name):base(name)
		{
			InitBlock();
			rand = NewRandom();
			Build(signedIndex);
			Build(unsignedIndex);
		}
		public BaseTestRangeFilter()
		{
			InitBlock();
			rand = NewRandom();
			Build(signedIndex);
			Build(unsignedIndex);
		}
		
		private void  Build(TestIndex index)
		{
			try
			{
				
				/* build an index */
				IndexWriter writer = new IndexWriter(index.index, new SimpleAnalyzer(), T, IndexWriter.MaxFieldLength.LIMITED);
				
				for (int d = minId; d <= maxId; d++)
				{
					Document doc = new Document();
					doc.Add(new Field("id", Pad(d), Field.Store.YES, Field.Index.NOT_ANALYZED));
					int r = index.allowNegativeRandomInts ? rand.Next() : rand.Next(System.Int32.MaxValue);
					if (index.maxR < r)
					{
						index.maxR = r;
					}
					if (r < index.minR)
					{
						index.minR = r;
					}
					doc.Add(new Field("rand", Pad(r), Field.Store.YES, Field.Index.NOT_ANALYZED));
					doc.Add(new Field("body", "body", Field.Store.YES, Field.Index.NOT_ANALYZED));
					writer.AddDocument(doc);
				}
				
				writer.Optimize();
				writer.Close();
			}
			catch (System.Exception e)
			{
				throw new System.SystemException("can't build index", e);
			}
		}
		
		[Test]
		public virtual void  TestPad()
		{
			
			int[] tests = new int[]{- 9999999, - 99560, - 100, - 3, - 1, 0, 3, 9, 10, 1000, 999999999};
			for (int i = 0; i < tests.Length - 1; i++)
			{
				int a = tests[i];
				int b = tests[i + 1];
				System.String aa = Pad(a);
				System.String bb = Pad(b);
				System.String label = a + ":" + aa + " vs " + b + ":" + bb;
				Assert.AreEqual(aa.Length, bb.Length, "length of " + label);
				Assert.IsTrue(String.CompareOrdinal(aa, bb) < 0, "compare less than " + label);
			}
		}
	}
}