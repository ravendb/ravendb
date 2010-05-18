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

using Analyzer = Lucene.Net.Analysis.Analyzer;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using OffsetAttribute = Lucene.Net.Analysis.Tokenattributes.OffsetAttribute;
using PositionIncrementAttribute = Lucene.Net.Analysis.Tokenattributes.PositionIncrementAttribute;
using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestTermVectorsReader:LuceneTestCase
	{
		private void  InitBlock()
		{
			positions = new int[testTerms.Length][];
			offsets = new TermVectorOffsetInfo[testTerms.Length][];
			tokens = new TestToken[testTerms.Length * TERM_FREQ];
		}
		//Must be lexicographically sorted, will do in setup, versus trying to maintain here
		private System.String[] testFields = new System.String[]{"f1", "f2", "f3", "f4"};
		private bool[] testFieldsStorePos = new bool[]{true, false, true, false};
		private bool[] testFieldsStoreOff = new bool[]{true, false, false, true};
		private System.String[] testTerms = new System.String[]{"this", "is", "a", "test"};
		private int[][] positions;
		private TermVectorOffsetInfo[][] offsets;
		private MockRAMDirectory dir = new MockRAMDirectory();
		private System.String seg;
		private FieldInfos fieldInfos = new FieldInfos();
		private static int TERM_FREQ = 3;
		
		public TestTermVectorsReader(System.String s):base(s)
		{
			InitBlock();
		}
        public TestTermVectorsReader()
            : base()
        {
            InitBlock();
        }
		
		internal class TestToken : System.IComparable
		{
			public TestToken(TestTermVectorsReader enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestTermVectorsReader enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTermVectorsReader enclosingInstance;
			public TestTermVectorsReader Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal System.String text;
			internal int pos;
			internal int startOffset;
			internal int endOffset;
			public virtual int CompareTo(System.Object other)
			{
				return pos - ((TestToken) other).pos;
			}
		}
		
		internal TestToken[] tokens;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			/*
			for (int i = 0; i < testFields.length; i++) {
			fieldInfos.add(testFields[i], true, true, testFieldsStorePos[i], testFieldsStoreOff[i]);
			}
			*/
			
			System.Array.Sort(testTerms);
			int tokenUpto = 0;
			for (int i = 0; i < testTerms.Length; i++)
			{
				positions[i] = new int[TERM_FREQ];
				offsets[i] = new TermVectorOffsetInfo[TERM_FREQ];
				// first position must be 0
				for (int j = 0; j < TERM_FREQ; j++)
				{
					// positions are always sorted in increasing order
					positions[i][j] = (int) (j * 10 + (new System.Random().NextDouble()) * 10);
					// offsets are always sorted in increasing order
					offsets[i][j] = new TermVectorOffsetInfo(j * 10, j * 10 + testTerms[i].Length);
					TestToken token = tokens[tokenUpto++] = new TestToken(this);
					token.text = testTerms[i];
					token.pos = positions[i][j];
					token.startOffset = offsets[i][j].GetStartOffset();
					token.endOffset = offsets[i][j].GetEndOffset();
				}
			}
			System.Array.Sort(tokens);
			
			IndexWriter writer = new IndexWriter(dir, new MyAnalyzer(this), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetUseCompoundFile(false);
			Document doc = new Document();
			for (int i = 0; i < testFields.Length; i++)
			{
				Field.TermVector tv;
				if (testFieldsStorePos[i] && testFieldsStoreOff[i])
					tv = Field.TermVector.WITH_POSITIONS_OFFSETS;
				else if (testFieldsStorePos[i] && !testFieldsStoreOff[i])
					tv = Field.TermVector.WITH_POSITIONS;
				else if (!testFieldsStorePos[i] && testFieldsStoreOff[i])
					tv = Field.TermVector.WITH_OFFSETS;
				else
					tv = Field.TermVector.YES;
				doc.Add(new Field(testFields[i], "", Field.Store.NO, Field.Index.ANALYZED, tv));
			}
			
			//Create 5 documents for testing, they all have the same
			//terms
			for (int j = 0; j < 5; j++)
				writer.AddDocument(doc);
			writer.Flush();
			seg = writer.NewestSegment().name;
			writer.Close();
			
			fieldInfos = new FieldInfos(dir, seg + "." + IndexFileNames.FIELD_INFOS_EXTENSION);
		}
		
		private class MyTokenStream:TokenStream
		{
			private void  InitBlock(TestTermVectorsReader enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTermVectorsReader enclosingInstance;
			public TestTermVectorsReader Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal int tokenUpto;
			
			internal TermAttribute termAtt;
			internal PositionIncrementAttribute posIncrAtt;
			internal OffsetAttribute offsetAtt;
			
			public MyTokenStream(TestTermVectorsReader enclosingInstance)
			{
				InitBlock(enclosingInstance);
				termAtt = (TermAttribute) AddAttribute(typeof(TermAttribute));
				posIncrAtt = (PositionIncrementAttribute) AddAttribute(typeof(PositionIncrementAttribute));
				offsetAtt = (OffsetAttribute) AddAttribute(typeof(OffsetAttribute));
			}
			
			public override bool IncrementToken()
			{
				if (tokenUpto >= Enclosing_Instance.tokens.Length)
					return false;
				else
				{
					TestToken testToken = Enclosing_Instance.tokens[tokenUpto++];
                    ClearAttributes();
					termAtt.SetTermBuffer(testToken.text);
					offsetAtt.SetOffset(testToken.startOffset, testToken.endOffset);
					if (tokenUpto > 1)
					{
						posIncrAtt.SetPositionIncrement(testToken.pos - Enclosing_Instance.tokens[tokenUpto - 2].pos);
					}
					else
					{
						posIncrAtt.SetPositionIncrement(testToken.pos + 1);
					}
					return true;
				}
			}
		}
		
		private class MyAnalyzer:Analyzer
		{
			public MyAnalyzer(TestTermVectorsReader enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestTermVectorsReader enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTermVectorsReader enclosingInstance;
			public TestTermVectorsReader Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				return new MyTokenStream(enclosingInstance);
			}
		}
		
		[Test]
		public virtual void  Test()
		{
			//Check to see the files were created properly in setup
			Assert.IsTrue(dir.FileExists(seg + "." + IndexFileNames.VECTORS_DOCUMENTS_EXTENSION));
			Assert.IsTrue(dir.FileExists(seg + "." + IndexFileNames.VECTORS_INDEX_EXTENSION));
		}
		
		[Test]
		public virtual void  TestReader()
		{
			TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
			Assert.IsTrue(reader != null);
			for (int j = 0; j < 5; j++)
			{
				TermFreqVector vector = reader.Get(j, testFields[0]);
				Assert.IsTrue(vector != null);
				System.String[] terms = vector.GetTerms();
				Assert.IsTrue(terms != null);
				Assert.IsTrue(terms.Length == testTerms.Length);
				for (int i = 0; i < terms.Length; i++)
				{
					System.String term = terms[i];
					//System.out.println("Term: " + term);
					Assert.IsTrue(term.Equals(testTerms[i]));
				}
			}
		}
		
		[Test]
		public virtual void  TestPositionReader()
		{
			TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
			Assert.IsTrue(reader != null);
			TermPositionVector vector;
			System.String[] terms;
			vector = (TermPositionVector) reader.Get(0, testFields[0]);
			Assert.IsTrue(vector != null);
			terms = vector.GetTerms();
			Assert.IsTrue(terms != null);
			Assert.IsTrue(terms.Length == testTerms.Length);
			for (int i = 0; i < terms.Length; i++)
			{
				System.String term = terms[i];
				//System.out.println("Term: " + term);
				Assert.IsTrue(term.Equals(testTerms[i]));
				int[] positions = vector.GetTermPositions(i);
				Assert.IsTrue(positions != null);
				Assert.IsTrue(positions.Length == this.positions[i].Length);
				for (int j = 0; j < positions.Length; j++)
				{
					int position = positions[j];
					Assert.IsTrue(position == this.positions[i][j]);
				}
				TermVectorOffsetInfo[] offset = vector.GetOffsets(i);
				Assert.IsTrue(offset != null);
				Assert.IsTrue(offset.Length == this.offsets[i].Length);
				for (int j = 0; j < offset.Length; j++)
				{
					TermVectorOffsetInfo termVectorOffsetInfo = offset[j];
					Assert.IsTrue(termVectorOffsetInfo.Equals(offsets[i][j]));
				}
			}
			
			TermFreqVector freqVector = reader.Get(0, testFields[1]); //no pos, no offset
			Assert.IsTrue(freqVector != null);
			Assert.IsTrue(freqVector is TermPositionVector == false);
			terms = freqVector.GetTerms();
			Assert.IsTrue(terms != null);
			Assert.IsTrue(terms.Length == testTerms.Length);
			for (int i = 0; i < terms.Length; i++)
			{
				System.String term = terms[i];
				//System.out.println("Term: " + term);
				Assert.IsTrue(term.Equals(testTerms[i]));
			}
		}
		
		[Test]
		public virtual void  TestOffsetReader()
		{
			TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
			Assert.IsTrue(reader != null);
			TermPositionVector vector = (TermPositionVector) reader.Get(0, testFields[0]);
			Assert.IsTrue(vector != null);
			System.String[] terms = vector.GetTerms();
			Assert.IsTrue(terms != null);
			Assert.IsTrue(terms.Length == testTerms.Length);
			for (int i = 0; i < terms.Length; i++)
			{
				System.String term = terms[i];
				//System.out.println("Term: " + term);
				Assert.IsTrue(term.Equals(testTerms[i]));
				int[] positions = vector.GetTermPositions(i);
				Assert.IsTrue(positions != null);
				Assert.IsTrue(positions.Length == this.positions[i].Length);
				for (int j = 0; j < positions.Length; j++)
				{
					int position = positions[j];
					Assert.IsTrue(position == this.positions[i][j]);
				}
				TermVectorOffsetInfo[] offset = vector.GetOffsets(i);
				Assert.IsTrue(offset != null);
				Assert.IsTrue(offset.Length == this.offsets[i].Length);
				for (int j = 0; j < offset.Length; j++)
				{
					TermVectorOffsetInfo termVectorOffsetInfo = offset[j];
					Assert.IsTrue(termVectorOffsetInfo.Equals(offsets[i][j]));
				}
			}
		}
		
		[Test]
		public virtual void  TestMapper()
		{
			TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
			Assert.IsTrue(reader != null);
			SortedTermVectorMapper mapper = new SortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
			reader.Get(0, mapper);
			System.Collections.Generic.SortedDictionary<Object,Object> set_Renamed = mapper.GetTermVectorEntrySet();
			Assert.IsTrue(set_Renamed != null, "set is null and it shouldn't be");
			//three fields, 4 terms, all terms are the same
			Assert.IsTrue(set_Renamed.Count == 4, "set Size: " + set_Renamed.Count + " is not: " + 4);
			//Check offsets and positions
			for (System.Collections.IEnumerator iterator = set_Renamed.Keys.GetEnumerator(); iterator.MoveNext(); )
			{
				TermVectorEntry tve = (TermVectorEntry) iterator.Current;
				Assert.IsTrue(tve != null, "tve is null and it shouldn't be");
				Assert.IsTrue(tve.GetOffsets() != null, "tve.getOffsets() is null and it shouldn't be");
				Assert.IsTrue(tve.GetPositions() != null, "tve.getPositions() is null and it shouldn't be");
			}
			
			mapper = new SortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
			reader.Get(1, mapper);
			set_Renamed = mapper.GetTermVectorEntrySet();
			Assert.IsTrue(set_Renamed != null, "set is null and it shouldn't be");
			//three fields, 4 terms, all terms are the same
			Assert.IsTrue(set_Renamed.Count == 4, "set Size: " + set_Renamed.Count + " is not: " + 4);
			//Should have offsets and positions b/c we are munging all the fields together
			for (System.Collections.IEnumerator iterator = set_Renamed.Keys.GetEnumerator(); iterator.MoveNext(); )
			{
				TermVectorEntry tve = (TermVectorEntry) iterator.Current;
				Assert.IsTrue(tve != null, "tve is null and it shouldn't be");
				Assert.IsTrue(tve.GetOffsets() != null, "tve.getOffsets() is null and it shouldn't be");
				Assert.IsTrue(tve.GetPositions() != null, "tve.getPositions() is null and it shouldn't be");
			}
			
			
			FieldSortedTermVectorMapper fsMapper = new FieldSortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
			reader.Get(0, fsMapper);
			System.Collections.IDictionary map = fsMapper.GetFieldToTerms();
			Assert.IsTrue(map.Count == testFields.Length, "map Size: " + map.Count + " is not: " + testFields.Length);
			for (System.Collections.IEnumerator iterator = new System.Collections.Hashtable(map).GetEnumerator(); iterator.MoveNext(); )
			{
				System.Collections.DictionaryEntry entry = (System.Collections.DictionaryEntry) iterator.Current;
				System.Collections.Generic.SortedDictionary<Object,Object> sortedSet = (System.Collections.Generic.SortedDictionary<Object,Object>)entry.Value;
				Assert.IsTrue(sortedSet.Count == 4, "sortedSet Size: " + sortedSet.Count + " is not: " + 4);
				for (System.Collections.IEnumerator inner = sortedSet.Keys.GetEnumerator(); inner.MoveNext(); )
				{
					TermVectorEntry tve = (TermVectorEntry) inner.Current;
					Assert.IsTrue(tve != null, "tve is null and it shouldn't be");
					//Check offsets and positions.
					Assert.IsTrue(tve != null, "tve is null and it shouldn't be");
					System.String field = tve.GetField();
					if (field.Equals(testFields[0]))
					{
						//should have offsets
						
						Assert.IsTrue(tve.GetOffsets() != null, "tve.getOffsets() is null and it shouldn't be");
						Assert.IsTrue(tve.GetPositions() != null, "tve.getPositions() is null and it shouldn't be");
					}
					else if (field.Equals(testFields[1]))
					{
						//should not have offsets
						
						Assert.IsTrue(tve.GetOffsets() == null, "tve.getOffsets() is not null and it shouldn't be");
						Assert.IsTrue(tve.GetPositions() == null, "tve.getPositions() is not null and it shouldn't be");
					}
				}
			}
			//Try mapper that ignores offs and positions
			fsMapper = new FieldSortedTermVectorMapper(true, true, new TermVectorEntryFreqSortedComparator());
			reader.Get(0, fsMapper);
			map = fsMapper.GetFieldToTerms();
			Assert.IsTrue(map.Count == testFields.Length, "map Size: " + map.Count + " is not: " + testFields.Length);
			for (System.Collections.IEnumerator iterator = new System.Collections.Hashtable(map).GetEnumerator(); iterator.MoveNext(); )
			{
				System.Collections.DictionaryEntry entry = (System.Collections.DictionaryEntry) iterator.Current;
				System.Collections.Generic.SortedDictionary<Object,Object> sortedSet = (System.Collections.Generic.SortedDictionary<Object,Object>)entry.Value;
				Assert.IsTrue(sortedSet.Count == 4, "sortedSet Size: " + sortedSet.Count + " is not: " + 4);
				for (System.Collections.IEnumerator inner = sortedSet.Keys.GetEnumerator(); inner.MoveNext(); )
				{
					TermVectorEntry tve = (TermVectorEntry) inner.Current;
					Assert.IsTrue(tve != null, "tve is null and it shouldn't be");
					//Check offsets and positions.
					Assert.IsTrue(tve != null, "tve is null and it shouldn't be");
					System.String field = tve.GetField();
					if (field.Equals(testFields[0]))
					{
						//should have offsets
						
						Assert.IsTrue(tve.GetOffsets() == null, "tve.getOffsets() is null and it shouldn't be");
						Assert.IsTrue(tve.GetPositions() == null, "tve.getPositions() is null and it shouldn't be");
					}
					else if (field.Equals(testFields[1]))
					{
						//should not have offsets
						
						Assert.IsTrue(tve.GetOffsets() == null, "tve.getOffsets() is not null and it shouldn't be");
						Assert.IsTrue(tve.GetPositions() == null, "tve.getPositions() is not null and it shouldn't be");
					}
				}
			}
			
			// test setDocumentNumber()
			IndexReader ir = IndexReader.Open(dir);
			DocNumAwareMapper docNumAwareMapper = new DocNumAwareMapper();
			Assert.AreEqual(- 1, docNumAwareMapper.GetDocumentNumber());
			
			ir.GetTermFreqVector(0, docNumAwareMapper);
			Assert.AreEqual(0, docNumAwareMapper.GetDocumentNumber());
			docNumAwareMapper.SetDocumentNumber(- 1);
			
			ir.GetTermFreqVector(1, docNumAwareMapper);
			Assert.AreEqual(1, docNumAwareMapper.GetDocumentNumber());
			docNumAwareMapper.SetDocumentNumber(- 1);
			
			ir.GetTermFreqVector(0, "f1", docNumAwareMapper);
			Assert.AreEqual(0, docNumAwareMapper.GetDocumentNumber());
			docNumAwareMapper.SetDocumentNumber(- 1);
			
			ir.GetTermFreqVector(1, "f2", docNumAwareMapper);
			Assert.AreEqual(1, docNumAwareMapper.GetDocumentNumber());
			docNumAwareMapper.SetDocumentNumber(- 1);
			
			ir.GetTermFreqVector(0, "f1", docNumAwareMapper);
			Assert.AreEqual(0, docNumAwareMapper.GetDocumentNumber());
			
			ir.Close();
		}
		
		
		/// <summary> Make sure exceptions and bad params are handled appropriately</summary>
		[Test]
		public virtual void  TestBadParams()
		{
			try
			{
				TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
				Assert.IsTrue(reader != null);
				//Bad document number, good field number
				reader.Get(50, testFields[0]);
				Assert.Fail();
			}
			catch (System.IO.IOException e)
			{
				// expected exception
			}
			try
			{
				TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
				Assert.IsTrue(reader != null);
				//Bad document number, no field
				reader.Get(50);
				Assert.Fail();
			}
			catch (System.IO.IOException e)
			{
				// expected exception
			}
			try
			{
				TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
				Assert.IsTrue(reader != null);
				//good document number, bad field number
				TermFreqVector vector = reader.Get(0, "f50");
				Assert.IsTrue(vector == null);
			}
			catch (System.IO.IOException e)
			{
				Assert.Fail();
			}
		}
		
		
		public class DocNumAwareMapper:TermVectorMapper
		{
			
			public DocNumAwareMapper()
			{
			}
			
			private int documentNumber = - 1;
			
			public override void  SetExpectations(System.String field, int numTerms, bool storeOffsets, bool storePositions)
			{
				if (documentNumber == - 1)
				{
					throw new System.SystemException("Documentnumber should be set at this point!");
				}
			}
			
			public override void  Map(System.String term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions)
			{
				if (documentNumber == - 1)
				{
					throw new System.SystemException("Documentnumber should be set at this point!");
				}
			}
			
			public virtual int GetDocumentNumber()
			{
				return documentNumber;
			}
			
			public override void  SetDocumentNumber(int documentNumber)
			{
				this.documentNumber = documentNumber;
			}
		}
	}
}