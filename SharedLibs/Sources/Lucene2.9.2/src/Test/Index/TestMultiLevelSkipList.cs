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
using LowerCaseTokenizer = Lucene.Net.Analysis.LowerCaseTokenizer;
using TokenFilter = Lucene.Net.Analysis.TokenFilter;
using TokenStream = Lucene.Net.Analysis.TokenStream;
using PayloadAttribute = Lucene.Net.Analysis.Tokenattributes.PayloadAttribute;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using Index = Lucene.Net.Documents.Field.Index;
using Store = Lucene.Net.Documents.Field.Store;
using IndexInput = Lucene.Net.Store.IndexInput;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	/// <summary> This testcase tests whether multi-level skipping is being used
	/// to reduce I/O while skipping through posting lists.
	/// 
	/// Skipping in general is already covered by several other
	/// testcases.
	/// 
	/// </summary>
    [TestFixture]
	public class TestMultiLevelSkipList:LuceneTestCase
	{
		[Test]
		public virtual void  TestSimpleSkip()
		{
			RAMDirectory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new PayloadAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			Term term = new Term("test", "a");
			for (int i = 0; i < 5000; i++)
			{
				Document d1 = new Document();
				d1.Add(new Field(term.Field(), term.Text(), Field.Store.NO, Field.Index.ANALYZED));
				writer.AddDocument(d1);
			}
			writer.Flush();
			writer.Optimize();
			writer.Close();
			
			IndexReader reader = SegmentReader.GetOnlySegmentReader(dir);
			SegmentTermPositions tp = (SegmentTermPositions) reader.TermPositions();
			tp.freqStream_ForNUnit = new CountingStream(this, tp.freqStream_ForNUnit);
			
			for (int i = 0; i < 2; i++)
			{
				counter = 0;
				tp.Seek(term);
				
				CheckSkipTo(tp, 14, 185); // no skips
				CheckSkipTo(tp, 17, 190); // one skip on level 0
				CheckSkipTo(tp, 287, 200); // one skip on level 1, two on level 0
				
				// this test would fail if we had only one skip level,
				// because than more bytes would be read from the freqStream
				CheckSkipTo(tp, 4800, 250); // one skip on level 2
			}
		}
		
		public virtual void  CheckSkipTo(TermPositions tp, int target, int maxCounter)
		{
			tp.SkipTo(target);
			if (maxCounter < counter)
			{
				Assert.Fail("Too many bytes read: " + counter);
			}
			
			Assert.AreEqual(target, tp.Doc(), "Wrong document " + tp.Doc() + " after skipTo target " + target);
			Assert.AreEqual(1, tp.Freq(), "Frequency is not 1: " + tp.Freq());
			tp.NextPosition();
			byte[] b = new byte[1];
			tp.GetPayload(b, 0);
			Assert.AreEqual((byte) target, b[0], "Wrong payload for the target " + target + ": " + b[0]);
		}
		
		private class PayloadAnalyzer:Analyzer
		{
			public override TokenStream TokenStream(System.String fieldName, System.IO.TextReader reader)
			{
				return new PayloadFilter(new LowerCaseTokenizer(reader));
			}
		}
		
		private class PayloadFilter:TokenFilter
		{
			internal static int count = 0;
			
			internal PayloadAttribute payloadAtt;
			
			protected internal PayloadFilter(TokenStream input):base(input)
			{
				payloadAtt = (PayloadAttribute) AddAttribute(typeof(PayloadAttribute));
			}
			
			public override bool IncrementToken()
			{
				bool hasNext = input.IncrementToken();
				if (hasNext)
				{
					payloadAtt.SetPayload(new Payload(new byte[]{(byte) count++}));
				}
				return hasNext;
			}
		}
		
		private int counter = 0;
		
		// Simply extends IndexInput in a way that we are able to count the number
		// of bytes read
		internal class CountingStream:IndexInput, System.ICloneable
		{
			private void  InitBlock(TestMultiLevelSkipList enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestMultiLevelSkipList enclosingInstance;
			public TestMultiLevelSkipList Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private IndexInput input;
			
			internal CountingStream(TestMultiLevelSkipList enclosingInstance, IndexInput input)
			{
				InitBlock(enclosingInstance);
				this.input = input;
			}
			
			public override byte ReadByte()
			{
				Enclosing_Instance.counter++;
				return this.input.ReadByte();
			}
			
			public override void  ReadBytes(byte[] b, int offset, int len)
			{
				Enclosing_Instance.counter += len;
				this.input.ReadBytes(b, offset, len);
			}
			
			public override void  Close()
			{
				this.input.Close();
			}
			
			public override long GetFilePointer()
			{
				return this.input.GetFilePointer();
			}
			
			public override void  Seek(long pos)
			{
				this.input.Seek(pos);
			}
			
			public override long Length()
			{
				return this.input.Length();
			}
			
			public override System.Object Clone()
			{
				return new CountingStream(enclosingInstance, (IndexInput) this.input.Clone());
			}
		}
	}
}