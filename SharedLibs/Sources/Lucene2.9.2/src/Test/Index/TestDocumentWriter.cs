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
using SimpleAnalyzer = Lucene.Net.Analysis.SimpleAnalyzer;
using TokenFilter = Lucene.Net.Analysis.TokenFilter;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using WhitespaceTokenizer = Lucene.Net.Analysis.WhitespaceTokenizer;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using PayloadAttribute = Lucene.Net.Analysis.Tokenattributes.PayloadAttribute;
using PositionIncrementAttribute = Lucene.Net.Analysis.Tokenattributes.PositionIncrementAttribute;
using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Fieldable = Lucene.Net.Documents.Fieldable;
using Index = Lucene.Net.Documents.Field.Index;
using Store = Lucene.Net.Documents.Field.Store;
using TermVector = Lucene.Net.Documents.Field.TermVector;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using AttributeSource = Lucene.Net.Util.AttributeSource;
using BaseTokenStreamTestCase = Lucene.Net.Analysis.BaseTokenStreamTestCase;
using _TestUtil = Lucene.Net.Util._TestUtil;

namespace Lucene.Net.Index
{
	
	[TestFixture]
	public class TestDocumentWriter:BaseTokenStreamTestCase
	{
		private class AnonymousClassAnalyzer:Analyzer
		{
			public AnonymousClassAnalyzer(TestDocumentWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestDocumentWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestDocumentWriter enclosingInstance;
			public TestDocumentWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				return new WhitespaceTokenizer(reader);
			}
			
			public override int GetPositionIncrementGap(System.String fieldName)
			{
				return 500;
			}
		}
		private class AnonymousClassAnalyzer1:Analyzer
		{
			public AnonymousClassAnalyzer1(TestDocumentWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private class AnonymousClassTokenFilter:TokenFilter
			{
				private void  InitBlock(AnonymousClassAnalyzer1 enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
					termAtt = (TermAttribute) AddAttribute(typeof(TermAttribute));
					payloadAtt = (PayloadAttribute) AddAttribute(typeof(PayloadAttribute));
					posIncrAtt = (PositionIncrementAttribute) AddAttribute(typeof(PositionIncrementAttribute));
				}
				private AnonymousClassAnalyzer1 enclosingInstance;
				public AnonymousClassAnalyzer1 Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				internal AnonymousClassTokenFilter(AnonymousClassAnalyzer1 enclosingInstance, Lucene.Net.Analysis.TokenStream Param1):base(Param1)
				{
					InitBlock(enclosingInstance);
				}
				internal bool first = true;
				internal AttributeSource.State state;
				
				public override bool IncrementToken()
				{
					if (state != null)
					{
						RestoreState(state);
						payloadAtt.SetPayload(null);
						posIncrAtt.SetPositionIncrement(0);
						termAtt.SetTermBuffer(new char[]{'b'}, 0, 1);
						state = null;
						return true;
					}
					
					bool hasNext = input.IncrementToken();
					if (!hasNext)
						return false;
					if (System.Char.IsDigit(termAtt.TermBuffer()[0]))
					{
						posIncrAtt.SetPositionIncrement(termAtt.TermBuffer()[0] - '0');
					}
					if (first)
					{
						// set payload on first position only
						payloadAtt.SetPayload(new Payload(new byte[]{100}));
						first = false;
					}
					
					// index a "synonym" for every token
					state = CaptureState();
					return true;
				}
				
				internal TermAttribute termAtt;
				internal PayloadAttribute payloadAtt;
				internal PositionIncrementAttribute posIncrAtt;
			}
			private void  InitBlock(TestDocumentWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestDocumentWriter enclosingInstance;
			public TestDocumentWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				return new AnonymousClassTokenFilter(this, new WhitespaceTokenizer(reader));
			}
		}
		private class AnonymousClassTokenStream:TokenStream
		{
			public AnonymousClassTokenStream(TestDocumentWriter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestDocumentWriter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
				termAtt = (TermAttribute) AddAttribute(typeof(TermAttribute));
			}
			private TestDocumentWriter enclosingInstance;
			public TestDocumentWriter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private System.String[] tokens = new System.String[]{"term1", "term2", "term3", "term2"};
			private int index = 0;
			
			private TermAttribute termAtt;
			
			public override bool IncrementToken()
			{
				if (index == tokens.Length)
				{
					return false;
				}
				else
				{
                    ClearAttributes();
					termAtt.SetTermBuffer(tokens[index++]);
					return true;
				}
			}
		}
		private RAMDirectory dir;
		
		public TestDocumentWriter(System.String s):base(s)
		{
		}

        public TestDocumentWriter() : base("")
        {
        }
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			dir = new RAMDirectory();
		}
		
		[Test]
		public virtual void  Test()
		{
			Assert.IsTrue(dir != null);
		}
		
		[Test]
		public virtual void  TestAddDocument()
		{
			Document testDoc = new Document();
			DocHelper.SetupDoc(testDoc);
			Analyzer analyzer = new WhitespaceAnalyzer();
			IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			writer.AddDocument(testDoc);
			writer.Flush();
			SegmentInfo info = writer.NewestSegment();
			writer.Close();
			//After adding the document, we should be able to read it back in
			SegmentReader reader = SegmentReader.Get(info);
			Assert.IsTrue(reader != null);
			Document doc = reader.Document(0);
			Assert.IsTrue(doc != null);
			
			//System.out.println("Document: " + doc);
			Fieldable[] fields = doc.GetFields("textField2");
			Assert.IsTrue(fields != null && fields.Length == 1);
			Assert.IsTrue(fields[0].StringValue().Equals(DocHelper.FIELD_2_TEXT));
			Assert.IsTrue(fields[0].IsTermVectorStored());
			
			fields = doc.GetFields("textField1");
			Assert.IsTrue(fields != null && fields.Length == 1);
			Assert.IsTrue(fields[0].StringValue().Equals(DocHelper.FIELD_1_TEXT));
			Assert.IsFalse(fields[0].IsTermVectorStored());
			
			fields = doc.GetFields("keyField");
			Assert.IsTrue(fields != null && fields.Length == 1);
			Assert.IsTrue(fields[0].StringValue().Equals(DocHelper.KEYWORD_TEXT));
			
			fields = doc.GetFields(DocHelper.NO_NORMS_KEY);
			Assert.IsTrue(fields != null && fields.Length == 1);
			Assert.IsTrue(fields[0].StringValue().Equals(DocHelper.NO_NORMS_TEXT));
			
			fields = doc.GetFields(DocHelper.TEXT_FIELD_3_KEY);
			Assert.IsTrue(fields != null && fields.Length == 1);
			Assert.IsTrue(fields[0].StringValue().Equals(DocHelper.FIELD_3_TEXT));
			
			// test that the norms are not present in the segment if
			// omitNorms is true
			for (int i = 0; i < reader.core_ForNUnit.fieldInfos_ForNUnit.Size(); i++)
			{
				FieldInfo fi = reader.core_ForNUnit.fieldInfos_ForNUnit.FieldInfo(i);
				if (fi.isIndexed_ForNUnit)
				{
					Assert.IsTrue(fi.omitNorms_ForNUnit == !reader.HasNorms(fi.name_ForNUnit));
				}
			}
		}
		
		[Test]
		public virtual void  TestPositionIncrementGap()
		{
			Analyzer analyzer = new AnonymousClassAnalyzer(this);
			
			IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			
			Document doc = new Document();
			doc.Add(new Field("repeated", "repeated one", Field.Store.YES, Field.Index.ANALYZED));
			doc.Add(new Field("repeated", "repeated two", Field.Store.YES, Field.Index.ANALYZED));
			
			writer.AddDocument(doc);
			writer.Flush();
			SegmentInfo info = writer.NewestSegment();
			writer.Close();
			SegmentReader reader = SegmentReader.Get(info);
			
			TermPositions termPositions = reader.TermPositions(new Term("repeated", "repeated"));
			Assert.IsTrue(termPositions.Next());
			int freq = termPositions.Freq();
			Assert.AreEqual(2, freq);
			Assert.AreEqual(0, termPositions.NextPosition());
			Assert.AreEqual(502, termPositions.NextPosition());
		}
		
		[Test]
		public virtual void  TestTokenReuse()
		{
			Analyzer analyzer = new AnonymousClassAnalyzer1(this);
			
			IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			
			Document doc = new Document();
			doc.Add(new Field("f1", "a 5 a a", Field.Store.YES, Field.Index.ANALYZED));
			
			writer.AddDocument(doc);
			writer.Flush();
			SegmentInfo info = writer.NewestSegment();
			writer.Close();
			SegmentReader reader = SegmentReader.Get(info);
			
			TermPositions termPositions = reader.TermPositions(new Term("f1", "a"));
			Assert.IsTrue(termPositions.Next());
			int freq = termPositions.Freq();
			Assert.AreEqual(3, freq);
			Assert.AreEqual(0, termPositions.NextPosition());
			Assert.AreEqual(true, termPositions.IsPayloadAvailable());
			Assert.AreEqual(6, termPositions.NextPosition());
			Assert.AreEqual(false, termPositions.IsPayloadAvailable());
			Assert.AreEqual(7, termPositions.NextPosition());
			Assert.AreEqual(false, termPositions.IsPayloadAvailable());
		}
		
		
		[Test]
		public virtual void  TestPreAnalyzedField()
		{
			IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			
			doc.Add(new Field("preanalyzed", new AnonymousClassTokenStream(this), TermVector.NO));
			
			writer.AddDocument(doc);
			writer.Flush();
			SegmentInfo info = writer.NewestSegment();
			writer.Close();
			SegmentReader reader = SegmentReader.Get(info);
			
			TermPositions termPositions = reader.TermPositions(new Term("preanalyzed", "term1"));
			Assert.IsTrue(termPositions.Next());
			Assert.AreEqual(1, termPositions.Freq());
			Assert.AreEqual(0, termPositions.NextPosition());
			
			termPositions.Seek(new Term("preanalyzed", "term2"));
			Assert.IsTrue(termPositions.Next());
			Assert.AreEqual(2, termPositions.Freq());
			Assert.AreEqual(1, termPositions.NextPosition());
			Assert.AreEqual(3, termPositions.NextPosition());
			
			termPositions.Seek(new Term("preanalyzed", "term3"));
			Assert.IsTrue(termPositions.Next());
			Assert.AreEqual(1, termPositions.Freq());
			Assert.AreEqual(2, termPositions.NextPosition());
		}
		
		/// <summary> Test adding two fields with the same name, but 
		/// with different term vector setting (LUCENE-766).
		/// </summary>
		[Test]
		public virtual void  TestMixedTermVectorSettingsSameField()
		{
			Document doc = new Document();
			// f1 first without tv then with tv
			doc.Add(new Field("f1", "v1", Field.Store.YES, Field.Index.NOT_ANALYZED, TermVector.NO));
			doc.Add(new Field("f1", "v2", Field.Store.YES, Field.Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
			// f2 first with tv then without tv
			doc.Add(new Field("f2", "v1", Field.Store.YES, Field.Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
			doc.Add(new Field("f2", "v2", Field.Store.YES, Field.Index.NOT_ANALYZED, TermVector.NO));
			
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.AddDocument(doc);
			writer.Close();
			
			_TestUtil.CheckIndex(dir);
			
			IndexReader reader = IndexReader.Open(dir);
			// f1
			TermFreqVector tfv1 = reader.GetTermFreqVector(0, "f1");
			Assert.IsNotNull(tfv1);
			Assert.AreEqual(2, tfv1.GetTerms().Length, "the 'with_tv' setting should rule!");
			// f2
			TermFreqVector tfv2 = reader.GetTermFreqVector(0, "f2");
			Assert.IsNotNull(tfv2);
			Assert.AreEqual(2, tfv2.GetTerms().Length, "the 'with_tv' setting should rule!");
		}
		
		/// <summary> Test adding two fields with the same name, one indexed
		/// the other stored only. The omitNorms and omitTermFreqAndPositions setting
		/// of the stored field should not affect the indexed one (LUCENE-1590)
		/// </summary>
		[Test]
		public virtual void  TestLUCENE_1590()
		{
			Document doc = new Document();
			// f1 has no norms
			doc.Add(new Field("f1", "v1", Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));
			doc.Add(new Field("f1", "v2", Field.Store.YES, Field.Index.NO));
			// f2 has no TF
			Field f = new Field("f2", "v1", Field.Store.NO, Field.Index.ANALYZED);
			f.SetOmitTermFreqAndPositions(true);
			doc.Add(f);
			doc.Add(new Field("f2", "v2", Field.Store.YES, Field.Index.NO));
			
			IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.AddDocument(doc);
			writer.Optimize(); // be sure to have a single segment
			writer.Close();
			
			_TestUtil.CheckIndex(dir);
			
			SegmentReader reader = SegmentReader.GetOnlySegmentReader(dir);
			FieldInfos fi = reader.FieldInfos();
			// f1
			Assert.IsFalse(reader.HasNorms("f1"), "f1 should have no norms");
			Assert.IsFalse(fi.FieldInfo("f1").omitTermFreqAndPositions_ForNUnit, "omitTermFreqAndPositions field bit should not be set for f1");
			// f2
			Assert.IsTrue(reader.HasNorms("f2"), "f2 should have norms");
			Assert.IsTrue(fi.FieldInfo("f2").omitTermFreqAndPositions_ForNUnit, "omitTermFreqAndPositions field bit should be set for f2");
		}
	}
}