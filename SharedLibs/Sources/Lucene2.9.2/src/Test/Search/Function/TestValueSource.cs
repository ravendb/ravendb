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

using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Lucene.Net.Search;
using Lucene.Net.Search.Function;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search.Function
{
	
    [TestFixture]
	public class TestValueSource:LuceneTestCase
	{
		
        [Test]
		public virtual void  TestMultiValueSource()
		{
			Directory dir = new MockRAMDirectory();
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
			Document doc = new Document();
			Field f = new Field("field", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
			doc.Add(f);
			
			for (int i = 0; i < 17; i++)
			{
				f.SetValue("" + i);
				w.AddDocument(doc);
				w.Commit();
			}
			
			IndexReader r = w.GetReader();
			w.Close();
			
			Assert.IsTrue(r.GetSequentialSubReaders().Length > 1);
			
			ValueSource s1 = new IntFieldSource("field");
			DocValues v1 = s1.GetValues(r);
			DocValues v2 = new MultiValueSource(s1).GetValues(r);
			
			for (int i = 0; i < r.MaxDoc(); i++)
			{
				Assert.AreEqual(v1.IntVal(i), i);
				Assert.AreEqual(v2.IntVal(i), i);
			}
			
			Lucene.Net.Search.FieldCache_Fields.DEFAULT.PurgeAllCaches();
			
			r.Close();
			dir.Close();
		}
	}
}