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

using OffsetAttribute = Lucene.Net.Analysis.Tokenattributes.OffsetAttribute;
using TermAttribute = Lucene.Net.Analysis.Tokenattributes.TermAttribute;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using TermVector = Lucene.Net.Documents.Field.TermVector;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using TermPositions = Lucene.Net.Index.TermPositions;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;

namespace Lucene.Net.Analysis
{
	
    [TestFixture]
	public class TestCachingTokenFilter:BaseTokenStreamTestCase
	{
		private class AnonymousClassTokenStream:TokenStream
		{
			public AnonymousClassTokenStream(TestCachingTokenFilter enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestCachingTokenFilter enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
				termAtt = (TermAttribute) AddAttribute(typeof(TermAttribute));
				offsetAtt = (OffsetAttribute) AddAttribute(typeof(OffsetAttribute));
			}
			private TestCachingTokenFilter enclosingInstance;
			public TestCachingTokenFilter Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private int index = 0;
			private TermAttribute termAtt;
			private OffsetAttribute offsetAtt;
			
			public override bool IncrementToken()
			{
				if (index == Enclosing_Instance.tokens.Length)
				{
					return false;
				}
				else
				{
                    ClearAttributes();
					termAtt.SetTermBuffer(Enclosing_Instance.tokens[index++]);
					offsetAtt.SetOffset(0, 0);
					return true;
				}
			}
		}
		private System.String[] tokens = new System.String[]{"term1", "term2", "term3", "term2"};
		
        [Test]
		public virtual void  TestCaching()
		{
			Directory dir = new RAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			Document doc = new Document();
			TokenStream stream = new AnonymousClassTokenStream(this);
			
			stream = new CachingTokenFilter(stream);
			
			doc.Add(new Field("preanalyzed", stream, TermVector.NO));
			
			// 1) we consume all tokens twice before we add the doc to the index
			checkTokens(stream);
			stream.Reset();
			checkTokens(stream);
			
			// 2) now add the document to the index and verify if all tokens are indexed
			//    don't reset the stream here, the DocumentWriter should do that implicitly
			writer.AddDocument(doc);
			writer.Close();
			
			IndexReader reader = IndexReader.Open(dir);
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
			reader.Close();
			
			// 3) reset stream and consume tokens again
			stream.Reset();
			checkTokens(stream);
		}
		
		private void  checkTokens(TokenStream stream)
		{
			int count = 0;
			
			TermAttribute termAtt = (TermAttribute) stream.GetAttribute(typeof(TermAttribute));
			Assert.IsNotNull(termAtt);
			while (stream.IncrementToken())
			{
				Assert.IsTrue(count < tokens.Length);
				Assert.AreEqual(tokens[count], termAtt.Term());
				count++;
			}
			
			Assert.AreEqual(tokens.Length, count);
		}
	}
}