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
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using TestIndexWriterReader = Lucene.Net.Index.TestIndexWriterReader;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Store
{
	
    [TestFixture]
	public class TestFileSwitchDirectory:LuceneTestCase
	{
		/// <summary> Test if writing doc stores to disk and everything else to ram works.</summary>
		/// <throws>  IOException </throws>
        [Test]
		public virtual void  TestBasic()
		{
			System.Collections.Hashtable fileExtensions = new System.Collections.Hashtable();
			SupportClass.CollectionsHelper.AddIfNotContains(fileExtensions, "fdt");
			SupportClass.CollectionsHelper.AddIfNotContains(fileExtensions, "fdx");
			
			Directory primaryDir = new MockRAMDirectory();
			RAMDirectory secondaryDir = new MockRAMDirectory();
			
			FileSwitchDirectory fsd = new FileSwitchDirectory(fileExtensions, primaryDir, secondaryDir, true);
			IndexWriter writer = new IndexWriter(fsd, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
			writer.SetUseCompoundFile(false);
			TestIndexWriterReader.CreateIndexNoClose(true, "ram", writer);
			IndexReader reader = writer.GetReader();
			Assert.AreEqual(100, reader.MaxDoc());
			writer.Commit();
			// we should see only fdx,fdt files here
			System.String[] files = primaryDir.ListAll();
			Assert.IsTrue(files.Length > 0);
			for (int x = 0; x < files.Length; x++)
			{
				System.String ext = FileSwitchDirectory.GetExtension(files[x]);
				Assert.IsTrue(fileExtensions.Contains(ext));
			}
			files = secondaryDir.ListAll();
			Assert.IsTrue(files.Length > 0);
			// we should not see fdx,fdt files here
			for (int x = 0; x < files.Length; x++)
			{
				System.String ext = FileSwitchDirectory.GetExtension(files[x]);
				Assert.IsFalse(fileExtensions.Contains(ext));
			}
			reader.Close();
			writer.Close();
			
			files = fsd.ListAll();
			for (int i = 0; i < files.Length; i++)
			{
				Assert.IsNotNull(files[i]);
			}
			fsd.Close();
		}
	}
}