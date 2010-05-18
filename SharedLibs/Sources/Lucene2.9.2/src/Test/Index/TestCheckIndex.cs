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
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using Constants = Lucene.Net.Util.Constants;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	[TestFixture]
	public class TestCheckIndex:LuceneTestCase
	{
		
		[Test]
		public virtual void  TestDeletedDocs()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			Document doc = new Document();
			doc.Add(new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
			for (int i = 0; i < 19; i++)
			{
				writer.AddDocument(doc);
			}
			writer.Close();
			IndexReader reader = IndexReader.Open(dir);
			reader.DeleteDocument(5);
			reader.Close();
			
			System.IO.MemoryStream bos = new System.IO.MemoryStream(1024);
			CheckIndex checker = new CheckIndex(dir);
			checker.SetInfoStream(new System.IO.StreamWriter(bos));
			//checker.setInfoStream(System.out);
			CheckIndex.Status indexStatus = checker.CheckIndex_Renamed_Method();
			if (indexStatus.clean == false)
			{
				System.Console.Out.WriteLine("CheckIndex failed");
				char[] tmpChar;
				byte[] tmpByte;
				tmpByte = bos.GetBuffer();
				tmpChar = new char[bos.Length];
				System.Array.Copy(tmpByte, 0, tmpChar, 0, tmpChar.Length);
				System.Console.Out.WriteLine(new System.String(tmpChar));
				Assert.Fail();
			}
			
			CheckIndex.Status.SegmentInfoStatus seg = (CheckIndex.Status.SegmentInfoStatus) indexStatus.segmentInfos[0];
			Assert.IsTrue(seg.openReaderPassed);
			
			Assert.IsNotNull(seg.diagnostics);
			
			Assert.IsNotNull(seg.fieldNormStatus);
			Assert.IsNull(seg.fieldNormStatus.error);
			Assert.AreEqual(1, seg.fieldNormStatus.totFields);
			
			Assert.IsNotNull(seg.termIndexStatus);
			Assert.IsNull(seg.termIndexStatus.error);
			Assert.AreEqual(1, seg.termIndexStatus.termCount);
			Assert.AreEqual(19, seg.termIndexStatus.totFreq);
			Assert.AreEqual(18, seg.termIndexStatus.totPos);
			
			Assert.IsNotNull(seg.storedFieldStatus);
			Assert.IsNull(seg.storedFieldStatus.error);
			Assert.AreEqual(18, seg.storedFieldStatus.docCount);
			Assert.AreEqual(18, seg.storedFieldStatus.totFields);
			
			Assert.IsNotNull(seg.termVectorStatus);
			Assert.IsNull(seg.termVectorStatus.error);
			Assert.AreEqual(18, seg.termVectorStatus.docCount);
			Assert.AreEqual(18, seg.termVectorStatus.totVectors);
			
			Assert.IsTrue(seg.diagnostics.Count > 0);
			System.Collections.IList onlySegments = new System.Collections.ArrayList();
			onlySegments.Add("_0");
			
			Assert.IsTrue(checker.CheckIndex_Renamed_Method(onlySegments).clean == true);
		}
		
		[Test]
		public virtual void  TestLuceneConstantVersion()
		{
			System.String version = null;

            AppDomain MyDomain = AppDomain.CurrentDomain;
            System.Reflection.Assembly[] AssembliesLoaded = MyDomain.GetAssemblies();

            foreach (System.Reflection.Assembly assembly in AssembliesLoaded)
            {
                if(assembly.FullName.StartsWith("Lucene.Net")){
                    version =assembly.GetName().Version.ToString(3);
                    break;
                }
            }
            Assert.IsNotNull(version);
            Assert.IsTrue(version.Equals(Constants.LUCENE_MAIN_VERSION + "-dev") || version.Equals(Constants.LUCENE_MAIN_VERSION));
            Assert.IsTrue(Constants.LUCENE_VERSION.StartsWith(version));

		}
	}
}