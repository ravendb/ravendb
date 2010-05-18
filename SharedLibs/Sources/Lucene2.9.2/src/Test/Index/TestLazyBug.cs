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
using Lucene.Net.Documents;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	
	/// <summary> Test demonstrating EOF bug on the last field of the last doc 
	/// if other docs have allready been accessed.
	/// </summary>
    [TestFixture]
	public class TestLazyBug:LuceneTestCase
	{
		[Serializable]
		public class AnonymousClassFieldSelector : FieldSelector
		{
			public virtual FieldSelectorResult Accept(System.String f)
			{
				if (f.Equals(Lucene.Net.Index.TestLazyBug.MAGIC_FIELD))
				{
					return FieldSelectorResult.LOAD;
				}
				return FieldSelectorResult.LAZY_LOAD;
			}
		}
		
		public static int NUM_DOCS = 500;
		public static int NUM_FIELDS = 100;
		
		private static System.String[] data = new System.String[]{"now", "is the time", "for all good men", "to come to the aid", "of their country!", "this string contains big chars:{\u0111 \u0222 \u0333 \u1111 \u2222 \u3333}", "this string is a bigger string, mary had a little lamb, little lamb, little lamb!"};
		
		private static System.Collections.Hashtable dataset = new System.Collections.Hashtable();
		
		private static System.String MAGIC_FIELD = "f" + (NUM_FIELDS / 3);
		
		private static FieldSelector SELECTOR;
		
		private Directory MakeIndex()
		{
			Directory dir = new RAMDirectory();
			try
			{
				System.Random r = NewRandom();
				Analyzer analyzer = new SimpleAnalyzer();
				IndexWriter writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
				
				writer.SetUseCompoundFile(false);
				
				for (int d = 1; d <= NUM_DOCS; d++)
				{
					Document doc = new Document();
					for (int f = 1; f <= NUM_FIELDS; f++)
					{
						doc.Add(new Field("f" + f, data[f % data.Length] + '#' + data[r.Next(data.Length)], Field.Store.YES, Field.Index.ANALYZED));
					}
					writer.AddDocument(doc);
				}
				writer.Close();
			}
			catch (System.Exception e)
			{
				throw new System.SystemException("", e);
			}
			return dir;
		}
		
		public virtual void  DoTest(int[] docs)
		{
            if (dataset.Count == 0)
                for (int i = 0; i < data.Length; i++)
                    dataset.Add(data[i], data[i]);

			Directory dir = MakeIndex();
			IndexReader reader = IndexReader.Open(dir);
			for (int i = 0; i < docs.Length; i++)
			{
				Document d = reader.Document(docs[i], SELECTOR);
				d.Get(MAGIC_FIELD);
				
				System.Collections.IList fields = d.GetFields();
				for (System.Collections.IEnumerator fi = fields.GetEnumerator(); fi.MoveNext(); )
				{
					Fieldable f = null;
					try
					{
						f = (Fieldable) fi.Current;
						System.String fname = f.Name();
						System.String fval = f.StringValue();
						Assert.IsNotNull(docs[i] + " FIELD: " + fname, fval);
						System.String[] vals = fval.Split('#');
						if (!dataset.Contains(vals[0]) || !dataset.Contains(vals[1]))
						{
							Assert.Fail("FIELD:" + fname + ",VAL:" + fval);
						}
					}
					catch (System.Exception e)
					{
						throw new Exception(docs[i] + " WTF: " + f.Name(), e);
					}
				}
			}
			reader.Close();
		}
		
		[Test]
		public virtual void  TestLazyWorks()
		{
			DoTest(new int[]{399});
		}
		
		[Test]
		public virtual void  TestLazyAlsoWorks()
		{
			DoTest(new int[]{399, 150});
		}
		
		[Test]
		public virtual void  TestLazyBroken()
		{
			DoTest(new int[]{150, 399});
		}
		static TestLazyBug()
		{
			SELECTOR = new AnonymousClassFieldSelector();
		}
	}
}