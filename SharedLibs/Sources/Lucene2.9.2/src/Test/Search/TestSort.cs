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
using CorruptIndexException = Lucene.Net.Index.CorruptIndexException;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using ParseException = Lucene.Net.QueryParsers.ParseException;
using LockObtainFailedException = Lucene.Net.Store.LockObtainFailedException;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using DocIdBitSet = Lucene.Net.Util.DocIdBitSet;
using Occur = Lucene.Net.Search.BooleanClause.Occur;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> Unit tests for sorting code.
	/// 
	/// <p/>Created: Feb 17, 2004 4:55:10 PM
	/// 
	/// </summary>
	/// <since>   lucene 1.4
	/// </since>
	/// <version>  $Id: TestSort.java 803676 2009-08-12 19:31:38Z hossman $
	/// </version>
	
	[Serializable]
    [TestFixture]
	public class TestSort:LuceneTestCase
	{
		[Serializable]
		private class AnonymousClassIntParser : Lucene.Net.Search.IntParser
		{
			public AnonymousClassIntParser(TestSort enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestSort enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestSort enclosingInstance;
			public TestSort Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public int ParseInt(System.String val)
			{
				return (val[0] - 'A') * 123456;
			}
		}
		[Serializable]
		private class AnonymousClassFloatParser : Lucene.Net.Search.FloatParser
		{
			public AnonymousClassFloatParser(TestSort enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestSort enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestSort enclosingInstance;
			public TestSort Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public float ParseFloat(System.String val)
			{
				return (float) System.Math.Sqrt(val[0]);
			}
		}
		[Serializable]
		private class AnonymousClassLongParser : Lucene.Net.Search.LongParser
		{
			public AnonymousClassLongParser(TestSort enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestSort enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestSort enclosingInstance;
			public TestSort Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public long ParseLong(System.String val)
			{
				return (val[0] - 'A') * 1234567890L;
			}
		}
		[Serializable]
		private class AnonymousClassDoubleParser : Lucene.Net.Search.DoubleParser
		{
			public AnonymousClassDoubleParser(TestSort enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestSort enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestSort enclosingInstance;
			public TestSort Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public double ParseDouble(System.String val)
			{
				return System.Math.Pow(val[0], (val[0] - 'A'));
			}
		}
		[Serializable]
		private class AnonymousClassByteParser : Lucene.Net.Search.ByteParser
		{
			public AnonymousClassByteParser(TestSort enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestSort enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestSort enclosingInstance;
			public TestSort Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public sbyte ParseByte(System.String val)
			{
				return (sbyte) (val[0] - 'A');
			}
		}
		[Serializable]
		private class AnonymousClassShortParser : Lucene.Net.Search.ShortParser
		{
			public AnonymousClassShortParser(TestSort enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestSort enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestSort enclosingInstance;
			public TestSort Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public short ParseShort(System.String val)
			{
				return (short) (val[0] - 'A');
			}
		}
		[Serializable]
		private class AnonymousClassFilter:Filter
		{
			public AnonymousClassFilter(Lucene.Net.Search.TopDocs docs1, TestSort enclosingInstance)
			{
				InitBlock(docs1, enclosingInstance);
			}
			private void  InitBlock(Lucene.Net.Search.TopDocs docs1, TestSort enclosingInstance)
			{
				this.docs1 = docs1;
				this.enclosingInstance = enclosingInstance;
			}
			private Lucene.Net.Search.TopDocs docs1;
			private TestSort enclosingInstance;
			public TestSort Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override DocIdSet GetDocIdSet(IndexReader reader)
			{
				System.Collections.BitArray bs = new System.Collections.BitArray((reader.MaxDoc() % 64 == 0?reader.MaxDoc() / 64:reader.MaxDoc() / 64 + 1) * 64);
                for (int i = 0; i < reader.MaxDoc(); i++) bs.Set(i, true);
				bs.Set(docs1.scoreDocs[0].doc, true);
				return new DocIdBitSet(bs);
			}
		}
		
		private const int NUM_STRINGS = 6000;
		private Searcher full;
		private Searcher searchX;
		private Searcher searchY;
		private Query queryX;
		private Query queryY;
		private Query queryA;
		private Query queryE;
		private Query queryF;
		private Query queryG;
		private Sort sort;
		
		
		/*public TestSort(System.String name):base(name)
		{
		}*/
		
		/*public static Test Suite()
		{
			return new TestSuite(typeof(TestSort));
		}*/
		
		
		// document data:
		// the tracer field is used to determine which document was hit
		// the contents field is used to search and sort by relevance
		// the int field to sort by int
		// the float field to sort by float
		// the string field to sort by string
		// the i18n field includes accented characters for testing locale-specific sorting
        private System.String[][] data = new System.String[][] { 
          //              tracer contents         int            float            string   custom   i18n               long                       double,                           'short',                    byte,                   'custom parser encoding'
          new string[]{   "A",   "x a",           "5",           "4f",            "c",     "A-3",   "p\u00EAche",      "10",                      "-4.0",                           "3",                        "126",                      "J"},//A, x
        //{{See: LUCENENET-364}} Intentional diversion from Java (3.4028235E38 changed to 3.402823E38)
          new string[]{   "B",   "y a",           "5",           "3.402823E38",   "i",     "B-10",  "HAT",             "1000000000",              "40.0",                           "24",                       "1",                        "I"},//B, y
        //new string[]{   "B",   "y a",           "5",           "3.4028235E38",  "i",     "B-10",  "HAT",             "1000000000",              "40.0",                           "24",                       "1",                        "I"},//B, y
          new string[]{   "C",   "x a b c",       "2147483647",  "1.0",           "j",     "A-2",   "p\u00E9ch\u00E9", "99999999",                "40.00002343",                    "125",                      "15",                       "H"},//C, x
          new string[]{   "D",   "y a b c",       "-1",          "0.0f",          "a",     "C-0",   "HUT",             long.MaxValue.ToString(),  double.MinValue.ToString("E16"),  short.MinValue.ToString(),  sbyte.MinValue.ToString(),   "G"},//D, y
          new string[]{   "E",   "x a b c d",     "5",           "2f",            "h",     "B-8",   "peach",           long.MinValue.ToString(),  double.MaxValue.ToString("E16"),  short.MaxValue.ToString(),  sbyte.MaxValue.ToString(),   "F"},//E,x
          new string[]{   "F",   "y a b c d",     "2",           "3.14159f",      "g",     "B-1",   "H\u00C5T",        "-44",                     "343.034435444",                  "-3",                       "0",                        "E"},//F,y
          new string[]{   "G",   "x a b c d",     "3",           "-1.0",          "f",     "C-100", "sin",             "323254543543",            "4.043544",                       "5",                        "100",                      "D"},//G,x
          new string[]{   "H",   "y a b c d",     "0",           "1.4E-45",       "e",     "C-88",  "H\u00D8T",        "1023423423005",           "4.043545",                       "10",                       "-50",                      "C"},//H,y
          new string[]{   "I",   "x a b c d e f", "-2147483648", "1.0e+0",        "d",     "A-10",  "s\u00EDn",        "332422459999",            "4.043546",                       "-340",                     "51",                       "B"},//I,x
          new string[]{   "J",   "y a b c d e f", "4",           ".5",            "b",     "C-7",   "HOT",             "34334543543",             "4.0000220343",                   "300",                      "2",                        "A"},//J,y
          new string[]{   "W",   "g",             "1",           null,            null,    null,    null,              null,                      null,                             null,                       null,                       null},
          new string[]{   "X",   "g",             "1",           "0.1",           null,    null,    null,              null,                      null,                             null,                       null,                       null},
          new string[]{   "Y",   "g",             "1",           "0.2",           null,    null,    null,              null,                      null,                             null,                       null,                       null},
          new string[]{   "Z",   "f g",           null,          null,            null,    null,    null,              null,                      null,                             null,                       null,                       null}
        };
		
		
		// create an index of all the documents, or just the x, or just the y documents
		private Searcher GetIndex(bool even, bool odd)
		{
			RAMDirectory indexStore = new RAMDirectory();
			IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			writer.SetMergeFactor(1000);
			for (int i = 0; i < data.Length; ++i)
			{
				if (((i % 2) == 0 && even) || ((i % 2) == 1 && odd))
				{
					Document doc = new Document();
					doc.Add(new Field("tracer", data[i][0], Field.Store.YES, Field.Index.NO));
					doc.Add(new Field("contents", data[i][1], Field.Store.NO, Field.Index.ANALYZED));
					if (data[i][2] != null)
						doc.Add(new Field("int", data[i][2], Field.Store.NO, Field.Index.NOT_ANALYZED));
					if (data[i][3] != null)
						doc.Add(new Field("float", data[i][3], Field.Store.NO, Field.Index.NOT_ANALYZED));
					if (data[i][4] != null)
						doc.Add(new Field("string", data[i][4], Field.Store.NO, Field.Index.NOT_ANALYZED));
					if (data[i][5] != null)
						doc.Add(new Field("custom", data[i][5], Field.Store.NO, Field.Index.NOT_ANALYZED));
					if (data[i][6] != null)
						doc.Add(new Field("i18n", data[i][6], Field.Store.NO, Field.Index.NOT_ANALYZED));
					if (data[i][7] != null)
						doc.Add(new Field("long", data[i][7], Field.Store.NO, Field.Index.NOT_ANALYZED));
					if (data[i][8] != null)
						doc.Add(new Field("double", data[i][8], Field.Store.NO, Field.Index.NOT_ANALYZED));
					if (data[i][9] != null)
						doc.Add(new Field("short", data[i][9], Field.Store.NO, Field.Index.NOT_ANALYZED));
					if (data[i][10] != null)
						doc.Add(new Field("byte", data[i][10], Field.Store.NO, Field.Index.NOT_ANALYZED));
					if (data[i][11] != null)
						doc.Add(new Field("parser", data[i][11], Field.Store.NO, Field.Index.NOT_ANALYZED));
					doc.SetBoost(2); // produce some scores above 1.0
					writer.AddDocument(doc);
				}
			}
			//writer.optimize ();
			writer.Close();
			IndexSearcher s = new IndexSearcher(indexStore);
			s.SetDefaultFieldSortScoring(true, true);
			return s;
		}
		
		private Searcher GetFullIndex()
		{
			return GetIndex(true, true);
		}
		
		private IndexSearcher GetFullStrings()
		{
			RAMDirectory indexStore = new RAMDirectory();
			IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(4);
			writer.SetMergeFactor(97);
			for (int i = 0; i < NUM_STRINGS; i++)
			{
				Document doc = new Document();
				System.String num = GetRandomCharString(GetRandomNumber(2, 8), 48, 52);
				doc.Add(new Field("tracer", num, Field.Store.YES, Field.Index.NO));
				//doc.add (new Field ("contents", Integer.toString(i), Field.Store.NO, Field.Index.ANALYZED));
				doc.Add(new Field("string", num, Field.Store.NO, Field.Index.NOT_ANALYZED));
				System.String num2 = GetRandomCharString(GetRandomNumber(1, 4), 48, 50);
				doc.Add(new Field("string2", num2, Field.Store.NO, Field.Index.NOT_ANALYZED));
				doc.Add(new Field("tracer2", num2, Field.Store.YES, Field.Index.NO));
				doc.SetBoost(2); // produce some scores above 1.0
				writer.SetMaxBufferedDocs(GetRandomNumber(2, 12));
				writer.AddDocument(doc);
			}
			//writer.optimize ();
			//System.out.println(writer.getSegmentCount());
			writer.Close();
			return new IndexSearcher(indexStore);
		}
		
		public virtual System.String GetRandomNumberString(int num, int low, int high)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			for (int i = 0; i < num; i++)
			{
				sb.Append(GetRandomNumber(low, high));
			}
			return sb.ToString();
		}
		
		public virtual System.String GetRandomCharString(int num)
		{
			return GetRandomCharString(num, 48, 122);
		}
		
		public virtual System.String GetRandomCharString(int num, int start, int end)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			for (int i = 0; i < num; i++)
			{
				sb.Append((char) GetRandomNumber(start, end));
			}
			return sb.ToString();
		}
		
		internal System.Random r;
		
		public virtual int GetRandomNumber(int low, int high)
		{
			
			int randInt = (System.Math.Abs(r.Next()) % (high - low)) + low;
			
			return randInt;
		}
		
		private Searcher GetXIndex()
		{
			return GetIndex(true, false);
		}
		
		private Searcher GetYIndex()
		{
			return GetIndex(false, true);
		}
		
		private Searcher GetEmptyIndex()
		{
			return GetIndex(false, false);
		}
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			full = GetFullIndex();
			searchX = GetXIndex();
			searchY = GetYIndex();
			queryX = new TermQuery(new Term("contents", "x"));
			queryY = new TermQuery(new Term("contents", "y"));
			queryA = new TermQuery(new Term("contents", "a"));
			queryE = new TermQuery(new Term("contents", "e"));
			queryF = new TermQuery(new Term("contents", "f"));
			queryG = new TermQuery(new Term("contents", "g"));
			sort = new Sort();
		}
		
		// test the sorts by score and document number
		[Test]
		public virtual void  TestBuiltInSorts()
		{
			sort = new Sort();
			AssertMatches(full, queryX, sort, "ACEGI");
			AssertMatches(full, queryY, sort, "BDFHJ");
			
			sort.SetSort(SortField.FIELD_DOC);
			AssertMatches(full, queryX, sort, "ACEGI");
			AssertMatches(full, queryY, sort, "BDFHJ");
		}
		
		// test sorts where the type of field is specified
		[Test]
		public virtual void  TestTypedSort()
		{
            sort.SetSort(new SortField[] { new SortField("int", SortField.INT), SortField.FIELD_DOC });
            AssertMatches(full, queryX, sort, "IGAEC");
            AssertMatches(full, queryY, sort, "DHFJB");

            sort.SetSort(new SortField[] { new SortField("float", SortField.FLOAT), SortField.FIELD_DOC });
            AssertMatches(full, queryX, sort, "GCIEA");
            AssertMatches(full, queryY, sort, "DHJFB");

            sort.SetSort(new SortField[] { new SortField("long", SortField.LONG), SortField.FIELD_DOC });
            AssertMatches(full, queryX, sort, "EACGI");
            AssertMatches(full, queryY, sort, "FBJHD");

            sort.SetSort(new SortField[] { new SortField("double", SortField.DOUBLE), SortField.FIELD_DOC });
            AssertMatches(full, queryX, sort, "AGICE");
            AssertMatches(full, queryY, sort, "DJHBF");
			
			sort.SetSort(new SortField[]{new SortField("byte", SortField.BYTE), SortField.FIELD_DOC});
			AssertMatches(full, queryX, sort, "CIGAE");
			AssertMatches(full, queryY, sort, "DHFBJ");
			
			sort.SetSort(new SortField[]{new SortField("short", SortField.SHORT), SortField.FIELD_DOC});
			AssertMatches(full, queryX, sort, "IAGCE");
			AssertMatches(full, queryY, sort, "DFHBJ");
			
			sort.SetSort(new SortField[]{new SortField("string", SortField.STRING), SortField.FIELD_DOC});
			AssertMatches(full, queryX, sort, "AIGEC");
			AssertMatches(full, queryY, sort, "DJHFB");
		}
		
		/// <summary> Test String sorting: small queue to many matches, multi field sort, reverse sort</summary>
		[Test]
		public virtual void  TestStringSort()
		{
			r = NewRandom();
			ScoreDoc[] result = null;
			IndexSearcher searcher = GetFullStrings();
			sort.SetSort(new SortField[]{new SortField("string", SortField.STRING), new SortField("string2", SortField.STRING, true), SortField.FIELD_DOC});
			
			result = searcher.Search(new MatchAllDocsQuery(), null, 500, sort).scoreDocs;
			
			System.Text.StringBuilder buff = new System.Text.StringBuilder();
			int n = result.Length;
			System.String last = null;
			System.String lastSub = null;
			int lastDocId = 0;
			bool fail = false;
			for (int x = 0; x < n; ++x)
			{
				Document doc2 = searcher.Doc(result[x].doc);
				System.String[] v = doc2.GetValues("tracer");
				System.String[] v2 = doc2.GetValues("tracer2");
				for (int j = 0; j < v.Length; ++j)
				{
					if (last != null)
					{
						int cmp = String.CompareOrdinal(v[j], last);
						if (!(cmp >= 0))
						{
							// ensure first field is in order
							fail = true;
							System.Console.Out.WriteLine("fail:" + v[j] + " < " + last);
						}
						if (cmp == 0)
						{
							// ensure second field is in reverse order
							cmp = String.CompareOrdinal(v2[j], lastSub);
							if (cmp > 0)
							{
								fail = true;
								System.Console.Out.WriteLine("rev field fail:" + v2[j] + " > " + lastSub);
							}
							else if (cmp == 0)
							{
								// ensure docid is in order
								if (result[x].doc < lastDocId)
								{
									fail = true;
									System.Console.Out.WriteLine("doc fail:" + result[x].doc + " > " + lastDocId);
								}
							}
						}
					}
					last = v[j];
					lastSub = v2[j];
					lastDocId = result[x].doc;
					buff.Append(v[j] + "(" + v2[j] + ")(" + result[x].doc + ") ");
				}
			}
			if (fail)
			{
				System.Console.Out.WriteLine("topn field1(field2)(docID):" + buff);
			}
			Assert.IsFalse(fail, "Found sort results out of order");
		}
		
		/// <summary> test sorts where the type of field is specified and a custom field parser 
		/// is used, that uses a simple char encoding. The sorted string contains a 
		/// character beginning from 'A' that is mapped to a numeric value using some 
		/// "funny" algorithm to be different for each data type.
		/// </summary>
		[Test]
		public virtual void  TestCustomFieldParserSort()
		{
			// since tests explicilty uses different parsers on the same fieldname
			// we explicitly check/purge the FieldCache between each assertMatch
			FieldCache fc = Lucene.Net.Search.FieldCache_Fields.DEFAULT;
			
			
			sort.SetSort(new SortField[]{new SortField("parser", new AnonymousClassIntParser(this)), SortField.FIELD_DOC});
			AssertMatches(full, queryA, sort, "JIHGFEDCBA");
            AssertSaneFieldCaches(Lucene.Net.TestCase.GetName() + " IntParser"); 
			fc.PurgeAllCaches();
			
			sort.SetSort(new SortField[]{new SortField("parser", new AnonymousClassFloatParser(this)), SortField.FIELD_DOC});
			AssertMatches(full, queryA, sort, "JIHGFEDCBA");
            AssertSaneFieldCaches(Lucene.Net.TestCase.GetName() + " FloatParser"); 
			fc.PurgeAllCaches();
			
			sort.SetSort(new SortField[]{new SortField("parser", new AnonymousClassLongParser(this)), SortField.FIELD_DOC});
			AssertMatches(full, queryA, sort, "JIHGFEDCBA");
            AssertSaneFieldCaches(Lucene.Net.TestCase.GetName() + " LongParser"); 
			fc.PurgeAllCaches();
			
			sort.SetSort(new SortField[]{new SortField("parser", new AnonymousClassDoubleParser(this)), SortField.FIELD_DOC});
			AssertMatches(full, queryA, sort, "JIHGFEDCBA");
            AssertSaneFieldCaches(Lucene.Net.TestCase.GetName() + " DoubleParser"); 
			fc.PurgeAllCaches();
			
			sort.SetSort(new SortField[]{new SortField("parser", new AnonymousClassByteParser(this)), SortField.FIELD_DOC});
			AssertMatches(full, queryA, sort, "JIHGFEDCBA");
            AssertSaneFieldCaches(Lucene.Net.TestCase.GetName() + " ByteParser"); 
			fc.PurgeAllCaches();
			
			sort.SetSort(new SortField[]{new SortField("parser", new AnonymousClassShortParser(this)), SortField.FIELD_DOC});
			AssertMatches(full, queryA, sort, "JIHGFEDCBA");
            AssertSaneFieldCaches(Lucene.Net.TestCase.GetName() + " ShortParser"); 
			fc.PurgeAllCaches();
		}
		
		// test sorts when there's nothing in the index
		[Test]
		public virtual void  TestEmptyIndex()
		{
			Searcher empty = GetEmptyIndex();
			
			sort = new Sort();
			AssertMatches(empty, queryX, sort, "");
			
			sort.SetSort(SortField.FIELD_DOC);
			AssertMatches(empty, queryX, sort, "");
			
			sort.SetSort(new SortField[]{new SortField("int", SortField.INT), SortField.FIELD_DOC});
			AssertMatches(empty, queryX, sort, "");
			
			sort.SetSort(new SortField[]{new SortField("string", SortField.STRING, true), SortField.FIELD_DOC});
			AssertMatches(empty, queryX, sort, "");
			
			sort.SetSort(new SortField[]{new SortField("float", SortField.FLOAT), new SortField("string", SortField.STRING)});
			AssertMatches(empty, queryX, sort, "");
		}
		
		internal class MyFieldComparator:FieldComparator
		{
			[Serializable]
			private class AnonymousClassIntParser1 : Lucene.Net.Search.IntParser
			{
				public AnonymousClassIntParser1(MyFieldComparator enclosingInstance)
				{
					InitBlock(enclosingInstance);
				}
				private void  InitBlock(MyFieldComparator enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
				}
				private MyFieldComparator enclosingInstance;
				public MyFieldComparator Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				public int ParseInt(System.String val)
				{
					return (val[0] - 'A') * 123456;
				}
			}
			internal int[] docValues;
			internal int[] slotValues;
			internal int bottomValue;
			
			internal MyFieldComparator(int numHits)
			{
				slotValues = new int[numHits];
			}
			
			public override void  Copy(int slot, int doc)
			{
				slotValues[slot] = docValues[doc];
			}
			
			public override int Compare(int slot1, int slot2)
			{
				return slotValues[slot1] - slotValues[slot2];
			}
			
			public override int CompareBottom(int doc)
			{
				return bottomValue - docValues[doc];
			}
			
			public override void  SetBottom(int bottom)
			{
				bottomValue = slotValues[bottom];
			}
			
			public override void  SetNextReader(IndexReader reader, int docBase)
			{
				docValues = Lucene.Net.Search.FieldCache_Fields.DEFAULT.GetInts(reader, "parser", new AnonymousClassIntParser1(this));
			}
			
			public override System.IComparable Value(int slot)
			{
				return (System.Int32) slotValues[slot];
			}
		}
		
		[Serializable]
		internal class MyFieldComparatorSource:FieldComparatorSource
		{
			public override FieldComparator NewComparator(System.String fieldname, int numHits, int sortPos, bool reversed)
			{
				return new MyFieldComparator(numHits);
			}
		}
		
		// Test sorting w/ custom FieldComparator
		[Test]
		public virtual void  TestNewCustomFieldParserSort()
		{
			sort.SetSort(new SortField[]{new SortField("parser", new MyFieldComparatorSource())});
			AssertMatches(full, queryA, sort, "JIHGFEDCBA");
		}
		
		// test sorts where the type of field is determined dynamically
		[Test]
		public virtual void  TestAutoSort()
		{
			sort.SetSort("int");
			AssertMatches(full, queryX, sort, "IGAEC");
			AssertMatches(full, queryY, sort, "DHFJB");
			
			sort.SetSort("float");
			AssertMatches(full, queryX, sort, "GCIEA");
			AssertMatches(full, queryY, sort, "DHJFB");
			
			sort.SetSort("string");
			AssertMatches(full, queryX, sort, "AIGEC");
			AssertMatches(full, queryY, sort, "DJHFB");
		}
		
		// test sorts in reverse
		[Test]
		public virtual void  TestReverseSort()
		{
			sort.SetSort(new SortField[]{new SortField(null, SortField.SCORE, true), SortField.FIELD_DOC});
			AssertMatches(full, queryX, sort, "IEGCA");
			AssertMatches(full, queryY, sort, "JFHDB");
			
			sort.SetSort(new SortField(null, SortField.DOC, true));
			AssertMatches(full, queryX, sort, "IGECA");
			AssertMatches(full, queryY, sort, "JHFDB");
			
			sort.SetSort("int", true);
			AssertMatches(full, queryX, sort, "CAEGI");
			AssertMatches(full, queryY, sort, "BJFHD");
			
			sort.SetSort("float", true);
			AssertMatches(full, queryX, sort, "AECIG");
			AssertMatches(full, queryY, sort, "BFJHD");
			
			sort.SetSort("string", true);
			AssertMatches(full, queryX, sort, "CEGIA");
			AssertMatches(full, queryY, sort, "BFHJD");
		}
		
		// test sorting when the sort field is empty (undefined) for some of the documents
		[Test]
		public virtual void  TestEmptyFieldSort()
		{
			sort.SetSort("string");
			AssertMatches(full, queryF, sort, "ZJI");
			
			sort.SetSort("string", true);
			AssertMatches(full, queryF, sort, "IJZ");
			
			sort.SetSort(new SortField("i18n", new System.Globalization.CultureInfo("en")));
			AssertMatches(full, queryF, sort, "ZJI");
			
			sort.SetSort(new SortField("i18n", new System.Globalization.CultureInfo("en"), true));
			AssertMatches(full, queryF, sort, "IJZ");
			
			sort.SetSort("int");
			AssertMatches(full, queryF, sort, "IZJ");
			
			sort.SetSort("int", true);
			AssertMatches(full, queryF, sort, "JZI");
			
			sort.SetSort("float");
			AssertMatches(full, queryF, sort, "ZJI");
			
			// using a nonexisting field as first sort key shouldn't make a difference:
			sort.SetSort(new SortField[]{new SortField("nosuchfield", SortField.STRING), new SortField("float")});
			AssertMatches(full, queryF, sort, "ZJI");
			
			sort.SetSort("float", true);
			AssertMatches(full, queryF, sort, "IJZ");
			
			// When a field is null for both documents, the next SortField should be used.
			// Works for
			sort.SetSort(new SortField[]{new SortField("int"), new SortField("string", SortField.STRING), new SortField("float")});
			AssertMatches(full, queryG, sort, "ZWXY");
			
			// Reverse the last criterium to make sure the test didn't pass by chance
			sort.SetSort(new SortField[]{new SortField("int"), new SortField("string", SortField.STRING), new SortField("float", true)});
			AssertMatches(full, queryG, sort, "ZYXW");
			
			// Do the same for a MultiSearcher
			Searcher multiSearcher = new MultiSearcher(new Searchable[]{full});
			
			sort.SetSort(new SortField[]{new SortField("int"), new SortField("string", SortField.STRING), new SortField("float")});
			AssertMatches(multiSearcher, queryG, sort, "ZWXY");
			
			sort.SetSort(new SortField[]{new SortField("int"), new SortField("string", SortField.STRING), new SortField("float", true)});
			AssertMatches(multiSearcher, queryG, sort, "ZYXW");
			// Don't close the multiSearcher. it would close the full searcher too!
			
			// Do the same for a ParallelMultiSearcher
			Searcher parallelSearcher = new ParallelMultiSearcher(new Searchable[]{full});
			
			sort.SetSort(new SortField[]{new SortField("int"), new SortField("string", SortField.STRING), new SortField("float")});
			AssertMatches(parallelSearcher, queryG, sort, "ZWXY");
			
			sort.SetSort(new SortField[]{new SortField("int"), new SortField("string", SortField.STRING), new SortField("float", true)});
			AssertMatches(parallelSearcher, queryG, sort, "ZYXW");
			// Don't close the parallelSearcher. it would close the full searcher too!
		}
		
		// test sorts using a series of fields
		[Test]
		public virtual void  TestSortCombos()
		{
			sort.SetSort(new System.String[]{"int", "float"});
			AssertMatches(full, queryX, sort, "IGEAC");
			
			sort.SetSort(new SortField[]{new SortField("int", true), new SortField(null, SortField.DOC, true)});
			AssertMatches(full, queryX, sort, "CEAGI");
			
			sort.SetSort(new System.String[]{"float", "string"});
			AssertMatches(full, queryX, sort, "GICEA");
		}
		
		// test using a Locale for sorting strings
		[Test]
		public virtual void  TestLocaleSort()
		{
			sort.SetSort(new SortField[]{new SortField("string", new System.Globalization.CultureInfo("en-US"))});
			AssertMatches(full, queryX, sort, "AIGEC");
			AssertMatches(full, queryY, sort, "DJHFB");
			
			sort.SetSort(new SortField[]{new SortField("string", new System.Globalization.CultureInfo("en-US"), true)});
			AssertMatches(full, queryX, sort, "CEGIA");
			AssertMatches(full, queryY, sort, "BFHJD");
		}
		
		// test using various international locales with accented characters
		// (which sort differently depending on locale)
		[Test]
		public virtual void  TestInternationalSort()
		{
			sort.SetSort(new SortField("i18n", new System.Globalization.CultureInfo("en-US")));
			AssertMatches(full, queryY, sort, "BFJHD");
			
			sort.SetSort(new SortField("i18n", new System.Globalization.CultureInfo("sv-se")));
			AssertMatches(full, queryY, sort, "BJDFH");
			
			sort.SetSort(new SortField("i18n", new System.Globalization.CultureInfo("da-dk")));
			AssertMatches(full, queryY, sort, "BJDHF");
			
			sort.SetSort(new SortField("i18n", new System.Globalization.CultureInfo("en-US")));
			AssertMatches(full, queryX, sort, "ECAGI");
			
			sort.SetSort(new SortField("i18n", new System.Globalization.CultureInfo("fr-FR")));
			AssertMatches(full, queryX, sort, "EACGI");
		}
		
		// Test the MultiSearcher's ability to preserve locale-sensitive ordering
		// by wrapping it around a single searcher
		[Test]
		public virtual void  TestInternationalMultiSearcherSort()
		{
			Searcher multiSearcher = new MultiSearcher(new Searchable[]{full});
			
			sort.SetSort(new SortField("i18n", new System.Globalization.CultureInfo("sv" + "-" + "se")));
			AssertMatches(multiSearcher, queryY, sort, "BJDFH");
			
			sort.SetSort(new SortField("i18n", new System.Globalization.CultureInfo("en-US")));
			AssertMatches(multiSearcher, queryY, sort, "BFJHD");
			
			sort.SetSort(new SortField("i18n", new System.Globalization.CultureInfo("da" + "-" + "dk")));
			AssertMatches(multiSearcher, queryY, sort, "BJDHF");
		}
		
		// test a custom sort function
		[Test]
		public virtual void  TestCustomSorts()
		{
			sort.SetSort(new SortField("custom", SampleComparable.GetComparatorSource()));
			AssertMatches(full, queryX, sort, "CAIEG");
			sort.SetSort(new SortField("custom", SampleComparable.GetComparatorSource(), true));
			AssertMatches(full, queryY, sort, "HJDBF");
			SortComparator custom = SampleComparable.GetComparator();
			sort.SetSort(new SortField("custom", custom));
			AssertMatches(full, queryX, sort, "CAIEG");
			sort.SetSort(new SortField("custom", custom, true));
			AssertMatches(full, queryY, sort, "HJDBF");
		}
		
		// test a variety of sorts using more than one searcher
		[Test]
		public virtual void  TestMultiSort()
		{
			MultiSearcher searcher = new MultiSearcher(new Searchable[]{searchX, searchY});
			RunMultiSorts(searcher, false);
		}
		
		// test a variety of sorts using a parallel multisearcher
		[Test]
		public virtual void  TestParallelMultiSort()
		{
			Searcher searcher = new ParallelMultiSearcher(new Searchable[]{searchX, searchY});
			RunMultiSorts(searcher, false);
		}
		
		// test that the relevancy scores are the same even if
		// hits are sorted
		[Test]
		public virtual void  TestNormalizedScores()
		{
			
			// capture relevancy scores
			System.Collections.Hashtable scoresX = GetScores(full.Search(queryX, null, 1000).scoreDocs, full);
			System.Collections.Hashtable scoresY = GetScores(full.Search(queryY, null, 1000).scoreDocs, full);
			System.Collections.Hashtable scoresA = GetScores(full.Search(queryA, null, 1000).scoreDocs, full);
			
			// we'll test searching locally, remote and multi
			
			MultiSearcher multi = new MultiSearcher(new Searchable[]{searchX, searchY});
			
			// change sorting and make sure relevancy stays the same
			
			sort = new Sort();
			AssertSameValues(scoresX, GetScores(full.Search(queryX, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresX, GetScores(multi.Search(queryX, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresY, GetScores(full.Search(queryY, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresY, GetScores(multi.Search(queryY, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresA, GetScores(full.Search(queryA, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresA, GetScores(multi.Search(queryA, null, 1000, sort).scoreDocs, multi));
			
			sort.SetSort(SortField.FIELD_DOC);
			AssertSameValues(scoresX, GetScores(full.Search(queryX, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresX, GetScores(multi.Search(queryX, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresY, GetScores(full.Search(queryY, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresY, GetScores(multi.Search(queryY, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresA, GetScores(full.Search(queryA, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresA, GetScores(multi.Search(queryA, null, 1000, sort).scoreDocs, multi));
			
			sort.SetSort("int");
			AssertSameValues(scoresX, GetScores(full.Search(queryX, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresX, GetScores(multi.Search(queryX, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresY, GetScores(full.Search(queryY, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresY, GetScores(multi.Search(queryY, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresA, GetScores(full.Search(queryA, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresA, GetScores(multi.Search(queryA, null, 1000, sort).scoreDocs, multi));
			
			sort.SetSort("float");
			AssertSameValues(scoresX, GetScores(full.Search(queryX, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresX, GetScores(multi.Search(queryX, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresY, GetScores(full.Search(queryY, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresY, GetScores(multi.Search(queryY, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresA, GetScores(full.Search(queryA, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresA, GetScores(multi.Search(queryA, null, 1000, sort).scoreDocs, multi));
			
			sort.SetSort("string");
			AssertSameValues(scoresX, GetScores(full.Search(queryX, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresX, GetScores(multi.Search(queryX, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresY, GetScores(full.Search(queryY, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresY, GetScores(multi.Search(queryY, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresA, GetScores(full.Search(queryA, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresA, GetScores(multi.Search(queryA, null, 1000, sort).scoreDocs, multi));
			
			sort.SetSort(new System.String[]{"int", "float"});
			AssertSameValues(scoresX, GetScores(full.Search(queryX, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresX, GetScores(multi.Search(queryX, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresY, GetScores(full.Search(queryY, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresY, GetScores(multi.Search(queryY, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresA, GetScores(full.Search(queryA, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresA, GetScores(multi.Search(queryA, null, 1000, sort).scoreDocs, multi));
			
			sort.SetSort(new SortField[]{new SortField("int", true), new SortField(null, SortField.DOC, true)});
			AssertSameValues(scoresX, GetScores(full.Search(queryX, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresX, GetScores(multi.Search(queryX, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresY, GetScores(full.Search(queryY, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresY, GetScores(multi.Search(queryY, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresA, GetScores(full.Search(queryA, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresA, GetScores(multi.Search(queryA, null, 1000, sort).scoreDocs, multi));
			
			sort.SetSort(new System.String[]{"float", "string"});
			AssertSameValues(scoresX, GetScores(full.Search(queryX, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresX, GetScores(multi.Search(queryX, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresY, GetScores(full.Search(queryY, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresY, GetScores(multi.Search(queryY, null, 1000, sort).scoreDocs, multi));
			AssertSameValues(scoresA, GetScores(full.Search(queryA, null, 1000, sort).scoreDocs, full));
			AssertSameValues(scoresA, GetScores(multi.Search(queryA, null, 1000, sort).scoreDocs, multi));
		}
		
		[Test]
		public virtual void  TestTopDocsScores()
		{
			
			// There was previously a bug in FieldSortedHitQueue.maxscore when only a single
			// doc was added.  That is what the following tests for.
			Sort sort = new Sort();
			int nDocs = 10;
			
			// try to pick a query that will result in an unnormalized
			// score greater than 1 to test for correct normalization
			TopDocs docs1 = full.Search(queryE, null, nDocs, sort);
			
			// a filter that only allows through the first hit
			Filter filt = new AnonymousClassFilter(docs1, this);
			
			TopDocs docs2 = full.Search(queryE, filt, nDocs, sort);
			
			Assert.AreEqual(docs1.scoreDocs[0].score, docs2.scoreDocs[0].score, 1e-6);
		}
		
		[Test]
		public virtual void  TestSortWithoutFillFields()
		{
			
			// There was previously a bug in TopFieldCollector when fillFields was set
			// to false - the same doc and score was set in ScoreDoc[] array. This test
			// asserts that if fillFields is false, the documents are set properly. It
			// does not use Searcher's default search methods (with Sort) since all set
			// fillFields to true.
			Sort[] sort = new Sort[]{new Sort(SortField.FIELD_DOC), new Sort()};
			for (int i = 0; i < sort.Length; i++)
			{
				Query q = new MatchAllDocsQuery();
				TopDocsCollector tdc = TopFieldCollector.create(sort[i], 10, false, false, false, true);
				
				full.Search(q, tdc);
				
				ScoreDoc[] sd = tdc.TopDocs().scoreDocs;
				for (int j = 1; j < sd.Length; j++)
				{
					Assert.IsTrue(sd[j].doc != sd[j - 1].doc);
				}
			}
		}
		
		[Test]
		public virtual void  TestSortWithoutScoreTracking()
		{
			
			// Two Sort criteria to instantiate the multi/single comparators.
			Sort[] sort = new Sort[]{new Sort(SortField.FIELD_DOC), new Sort()};
			for (int i = 0; i < sort.Length; i++)
			{
				Query q = new MatchAllDocsQuery();
				TopDocsCollector tdc = TopFieldCollector.create(sort[i], 10, true, false, false, true);
				
				full.Search(q, tdc);
				
				TopDocs td = tdc.TopDocs();
				ScoreDoc[] sd = td.scoreDocs;
				for (int j = 0; j < sd.Length; j++)
				{
					Assert.IsTrue(System.Single.IsNaN(sd[j].score));
				}
				Assert.IsTrue(System.Single.IsNaN(td.GetMaxScore()));
			}
		}
		
		[Test]
		public virtual void  TestSortWithScoreNoMaxScoreTracking()
		{
			
			// Two Sort criteria to instantiate the multi/single comparators.
			Sort[] sort = new Sort[]{new Sort(SortField.FIELD_DOC), new Sort()};
			for (int i = 0; i < sort.Length; i++)
			{
				Query q = new MatchAllDocsQuery();
				TopDocsCollector tdc = TopFieldCollector.create(sort[i], 10, true, true, false, true);
				
				full.Search(q, tdc);
				
				TopDocs td = tdc.TopDocs();
				ScoreDoc[] sd = td.scoreDocs;
				for (int j = 0; j < sd.Length; j++)
				{
					Assert.IsTrue(!System.Single.IsNaN(sd[j].score));
				}
				Assert.IsTrue(System.Single.IsNaN(td.GetMaxScore()));
			}
		}
		
		[Test]
		public virtual void  TestSortWithScoreAndMaxScoreTracking()
		{
			
			// Two Sort criteria to instantiate the multi/single comparators.
			Sort[] sort = new Sort[]{new Sort(SortField.FIELD_DOC), new Sort()};
			for (int i = 0; i < sort.Length; i++)
			{
				Query q = new MatchAllDocsQuery();
				TopDocsCollector tdc = TopFieldCollector.create(sort[i], 10, true, true, true, true);
				
				full.Search(q, tdc);
				
				TopDocs td = tdc.TopDocs();
				ScoreDoc[] sd = td.scoreDocs;
				for (int j = 0; j < sd.Length; j++)
				{
					Assert.IsTrue(!System.Single.IsNaN(sd[j].score));
				}
				Assert.IsTrue(!System.Single.IsNaN(td.GetMaxScore()));
			}
		}
		
		[Test]
		public virtual void  TestOutOfOrderDocsScoringSort()
		{
			
			// Two Sort criteria to instantiate the multi/single comparators.
			Sort[] sort = new Sort[]{new Sort(SortField.FIELD_DOC), new Sort()};
			bool[][] tfcOptions = new bool[][]{new bool[]{false, false, false}, new bool[]{false, false, true}, new bool[]{false, true, false}, new bool[]{false, true, true}, new bool[]{true, false, false}, new bool[]{true, false, true}, new bool[]{true, true, false}, new bool[]{true, true, true}};
			System.String[] actualTFCClasses = new System.String[]{"OutOfOrderOneComparatorNonScoringCollector", "OutOfOrderOneComparatorScoringMaxScoreCollector", "OutOfOrderOneComparatorScoringNoMaxScoreCollector", "OutOfOrderOneComparatorScoringMaxScoreCollector", "OutOfOrderOneComparatorNonScoringCollector", "OutOfOrderOneComparatorScoringMaxScoreCollector", "OutOfOrderOneComparatorScoringNoMaxScoreCollector", "OutOfOrderOneComparatorScoringMaxScoreCollector"};
			
			// Save the original value to set later.
			bool origVal = BooleanQuery.GetAllowDocsOutOfOrder();
			
			BooleanQuery.SetAllowDocsOutOfOrder(true);
			
			BooleanQuery bq = new BooleanQuery();
			// Add a Query with SHOULD, since bw.scorer() returns BooleanScorer2
			// which delegates to BS if there are no mandatory clauses.
			bq.Add(new MatchAllDocsQuery(), Occur.SHOULD);
			// Set minNrShouldMatch to 1 so that BQ will not optimize rewrite to return
			// the clause instead of BQ.
			bq.SetMinimumNumberShouldMatch(1);
			try
			{
				for (int i = 0; i < sort.Length; i++)
				{
					for (int j = 0; j < tfcOptions.Length; j++)
					{
						TopDocsCollector tdc = TopFieldCollector.create(sort[i], 10, tfcOptions[j][0], tfcOptions[j][1], tfcOptions[j][2], false);
						
						Assert.IsTrue(tdc.GetType().FullName.EndsWith("+" + actualTFCClasses[j]));
						
						full.Search(bq, tdc);
						
						TopDocs td = tdc.TopDocs();
						ScoreDoc[] sd = td.scoreDocs;
						Assert.AreEqual(10, sd.Length);
					}
				}
			}
			finally
			{
				// Whatever happens, reset BooleanQuery.allowDocsOutOfOrder to the
				// original value. Don't set it to false in case the implementation in BQ
				// will change some day.
				BooleanQuery.SetAllowDocsOutOfOrder(origVal);
			}
		}
		
		[Test]
		public virtual void  TestSortWithScoreAndMaxScoreTrackingNoResults()
		{
			
			// Two Sort criteria to instantiate the multi/single comparators.
			Sort[] sort = new Sort[]{new Sort(SortField.FIELD_DOC), new Sort()};
			for (int i = 0; i < sort.Length; i++)
			{
				TopDocsCollector tdc = TopFieldCollector.create(sort[i], 10, true, true, true, true);
				TopDocs td = tdc.TopDocs();
				Assert.AreEqual(0, td.totalHits);
				Assert.IsTrue(System.Single.IsNaN(td.GetMaxScore()));
			}
		}
		
		// runs a variety of sorts useful for multisearchers
		private void  RunMultiSorts(Searcher multi, bool isFull)
		{
			sort.SetSort(SortField.FIELD_DOC);
			System.String expected = isFull?"ABCDEFGHIJ":"ACEGIBDFHJ";
			AssertMatches(multi, queryA, sort, expected);
			
			sort.SetSort(new SortField("int", SortField.INT));
			expected = isFull?"IDHFGJABEC":"IDHFGJAEBC";
			AssertMatches(multi, queryA, sort, expected);
			
			sort.SetSort(new SortField[]{new SortField("int", SortField.INT), SortField.FIELD_DOC});
			expected = isFull?"IDHFGJABEC":"IDHFGJAEBC";
			AssertMatches(multi, queryA, sort, expected);
			
			sort.SetSort("int");
			expected = isFull?"IDHFGJABEC":"IDHFGJAEBC";
			AssertMatches(multi, queryA, sort, expected);
			
			sort.SetSort(new SortField[]{new SortField("float", SortField.FLOAT), SortField.FIELD_DOC});
			AssertMatches(multi, queryA, sort, "GDHJCIEFAB");
			
			sort.SetSort("float");
			AssertMatches(multi, queryA, sort, "GDHJCIEFAB");
			
			sort.SetSort("string");
			AssertMatches(multi, queryA, sort, "DJAIHGFEBC");
			
			sort.SetSort("int", true);
			expected = isFull?"CABEJGFHDI":"CAEBJGFHDI";
			AssertMatches(multi, queryA, sort, expected);
			
			sort.SetSort("float", true);
			AssertMatches(multi, queryA, sort, "BAFECIJHDG");
			
			sort.SetSort("string", true);
			AssertMatches(multi, queryA, sort, "CBEFGHIAJD");
			
			sort.SetSort(new System.String[]{"int", "float"});
			AssertMatches(multi, queryA, sort, "IDHFGJEABC");
			
			sort.SetSort(new System.String[]{"float", "string"});
			AssertMatches(multi, queryA, sort, "GDHJICEFAB");
			
			sort.SetSort("int");
			AssertMatches(multi, queryF, sort, "IZJ");
			
			sort.SetSort("int", true);
			AssertMatches(multi, queryF, sort, "JZI");
			
			sort.SetSort("float");
			AssertMatches(multi, queryF, sort, "ZJI");
			
			sort.SetSort("string");
			AssertMatches(multi, queryF, sort, "ZJI");
			
			sort.SetSort("string", true);
			AssertMatches(multi, queryF, sort, "IJZ");
			
			// up to this point, all of the searches should have "sane" 
			// FieldCache behavior, and should have reused hte cache in several cases
            AssertSaneFieldCaches(Lucene.Net.TestCase.GetName() + " various"); 
			// next we'll check Locale based (String[]) for 'string', so purge first
			Lucene.Net.Search.FieldCache_Fields.DEFAULT.PurgeAllCaches();
			
			sort.SetSort(new SortField[]{new SortField("string", new System.Globalization.CultureInfo("en-US"))});
			AssertMatches(multi, queryA, sort, "DJAIHGFEBC");
			
			sort.SetSort(new SortField[]{new SortField("string", new System.Globalization.CultureInfo("en-US"), true)});
			AssertMatches(multi, queryA, sort, "CBEFGHIAJD");
			
			sort.SetSort(new SortField[]{new SortField("string", new System.Globalization.CultureInfo("en-GB"))});
			AssertMatches(multi, queryA, sort, "DJAIHGFEBC");

            AssertSaneFieldCaches(Lucene.Net.TestCase.GetName() + " Locale.US + Locale.UK"); 
			Lucene.Net.Search.FieldCache_Fields.DEFAULT.PurgeAllCaches();
		}
		
		// make sure the documents returned by the search match the expected list
		private void  AssertMatches(Searcher searcher, Query query, Sort sort, System.String expectedResult)
		{
			//ScoreDoc[] result = searcher.search (query, null, 1000, sort).scoreDocs;
			TopDocs hits = searcher.Search(query, null, expectedResult.Length, sort);
			ScoreDoc[] result = hits.scoreDocs;
			Assert.AreEqual(hits.totalHits, expectedResult.Length);
			System.Text.StringBuilder buff = new System.Text.StringBuilder(10);
			int n = result.Length;
			for (int i = 0; i < n; ++i)
			{
				Document doc = searcher.Doc(result[i].doc);
				System.String[] v = doc.GetValues("tracer");
				for (int j = 0; j < v.Length; ++j)
				{
					buff.Append(v[j]);
				}
			}
			Assert.AreEqual(expectedResult, buff.ToString());
		}
		
		private System.Collections.Hashtable GetScores(ScoreDoc[] hits, Searcher searcher)
		{
			System.Collections.Hashtable scoreMap = new System.Collections.Hashtable();
			int n = hits.Length;
			for (int i = 0; i < n; ++i)
			{
				Document doc = searcher.Doc(hits[i].doc);
				System.String[] v = doc.GetValues("tracer");
				Assert.AreEqual(v.Length, 1);
				scoreMap[v[0]] = (float) hits[i].score;
			}
			return scoreMap;
		}
		
		// make sure all the values in the maps match
		private void  AssertSameValues(System.Collections.Hashtable m1, System.Collections.Hashtable m2)
		{
			int n = m1.Count;
			int m = m2.Count;
			Assert.AreEqual(n, m);
			System.Collections.IEnumerator iter = m1.Keys.GetEnumerator();
			while (iter.MoveNext())
			{
				System.Object key = iter.Current;
				System.Object o1 = m1[key];
				System.Object o2 = m2[key];
				if (o1 is System.Single)
				{
					Assert.AreEqual((float) ((System.Single) o1), (float) ((System.Single) o2), 1e-6);
				}
				else
				{
					Assert.AreEqual(m1[key], m2[key]);
				}
			}
		}
	}
}