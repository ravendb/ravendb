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

using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Store
{
	
	[TestFixture]
	public class TestWindowsMMap:LuceneTestCase
	{
		
		private const System.String alphabet = "abcdefghijklmnopqrstuvwzyz";
		private System.Random random;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			random = NewRandom();
		}
		
		private System.String RandomToken()
		{
			int tl = 1 + random.Next(7);
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			for (int cx = 0; cx < tl; cx++)
			{
				int c = random.Next(25);
				sb.Append(alphabet.Substring(c, (c + 1) - (c)));
			}
			return sb.ToString();
		}
		
		private System.String RandomField()
		{
			int fl = 1 + random.Next(3);
			System.Text.StringBuilder fb = new System.Text.StringBuilder();
			for (int fx = 0; fx < fl; fx++)
			{
				fb.Append(RandomToken());
				fb.Append(" ");
			}
			return fb.ToString();
		}
		
		private static readonly System.String storePathname = new System.IO.FileInfo(System.IO.Path.Combine(SupportClass.AppSettings.Get("tempDir", ""), "testLuceneMmap")).FullName;
		
		[Test]
		public virtual void  TestMmapIndex()
		{
			FSDirectory storeDirectory;
			storeDirectory = new MMapDirectory(new System.IO.FileInfo(storePathname), null);
			
			// plan to add a set of useful stopwords, consider changing some of the
			// interior filters.
			StandardAnalyzer analyzer = new StandardAnalyzer(new System.Collections.Hashtable());
			// TODO: something about lock timeouts and leftover locks.
			IndexWriter writer = new IndexWriter(storeDirectory, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
			IndexSearcher searcher = new IndexSearcher(storePathname);
			
			for (int dx = 0; dx < 1000; dx++)
			{
				System.String f = RandomField();
				Document doc = new Document();
				doc.Add(new Field("data", f, Field.Store.YES, Field.Index.ANALYZED));
				writer.AddDocument(doc);
			}
			
			searcher.Close();
			writer.Close();
			RmDir(new System.IO.FileInfo(storePathname));
		}
		
		private void  RmDir(System.IO.FileInfo dir)
		{
			System.IO.FileInfo[] files = SupportClass.FileSupport.GetFiles(dir);
			for (int i = 0; i < files.Length; i++)
			{
				bool tmpBool;
				if (System.IO.File.Exists(files[i].FullName))
				{
					System.IO.File.Delete(files[i].FullName);
					tmpBool = true;
				}
				else if (System.IO.Directory.Exists(files[i].FullName))
				{
					System.IO.Directory.Delete(files[i].FullName);
					tmpBool = true;
				}
				else
					tmpBool = false;
				bool generatedAux = tmpBool;
			}
			bool tmpBool2;
			if (System.IO.File.Exists(dir.FullName))
			{
				System.IO.File.Delete(dir.FullName);
				tmpBool2 = true;
			}
			else if (System.IO.Directory.Exists(dir.FullName))
			{
				System.IO.Directory.Delete(dir.FullName);
				tmpBool2 = true;
			}
			else
				tmpBool2 = false;
			bool generatedAux2 = tmpBool2;
		}
	}
}