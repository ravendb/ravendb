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
using Lucene.Net.Documents;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Directory = Lucene.Net.Store.Directory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> </summary>
	/// <version>  $Id: TestThreadSafe.java 741311 2009-02-05 21:53:40Z mikemccand $
	/// </version>
    [TestFixture]
	public class TestThreadSafe:LuceneTestCase
	{
		internal System.Random r;
		internal Directory dir1;
		internal Directory dir2;
		
		internal IndexReader ir1;
		internal IndexReader ir2;
		
		internal System.String failure = null;
		
		
		internal class Thr:SupportClass.ThreadClass
		{
			[Serializable]
			private class AnonymousClassFieldSelector : FieldSelector
			{
				public AnonymousClassFieldSelector(Thr enclosingInstance)
				{
					InitBlock(enclosingInstance);
				}
				private void  InitBlock(Thr enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
				}
				private Thr enclosingInstance;
				public Thr Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				public virtual FieldSelectorResult Accept(System.String fieldName)
				{
					switch (Enclosing_Instance.rand.Next(2))
					{
						
						case 0:  return FieldSelectorResult.LAZY_LOAD;
						
						case 1:  return FieldSelectorResult.LOAD;
							// TODO: add other options
						
						default:  return FieldSelectorResult.LOAD;
						
					}
				}
			}
			private void  InitBlock(TestThreadSafe enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestThreadSafe enclosingInstance;
			public TestThreadSafe Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal int iter;
			internal System.Random rand;
			// pass in random in case we want to make things reproducable
			public Thr(TestThreadSafe enclosingInstance, int iter, System.Random rand)
			{
				InitBlock(enclosingInstance);
				this.iter = iter;
				this.rand = rand;
			}
			
			override public void  Run()
			{
				try
				{
					for (int i = 0; i < iter; i++)
					{
						/*** future
						// pick a random index reader... a shared one, or create your own
						IndexReader ir;
						***/
						
						switch (rand.Next(1))
						{
							
							case 0:  loadDoc(Enclosing_Instance.ir1); break;
							}
					}
				}
				catch (System.Exception th)
				{
					Enclosing_Instance.failure = th.ToString();
					Assert.Fail(Enclosing_Instance.failure); // TestCase.fail(Enclosing_Instance.failure);
				}
			}
			
			
			internal virtual void  loadDoc(IndexReader ir)
			{
				// beware of deleted docs in the future
				Document doc = ir.Document(rand.Next(ir.MaxDoc()), new AnonymousClassFieldSelector(this));
				
				System.Collections.IList fields = doc.GetFields();
				for (int i = 0; i < fields.Count; i++)
				{
					Fieldable f = (Fieldable) fields[i];
					Enclosing_Instance.ValidateField(f);
				}
			}
		}
		
		
		internal virtual void  ValidateField(Fieldable f)
		{
			System.String val = f.StringValue();
			if (!val.StartsWith("^") || !val.EndsWith("$"))
			{
				throw new System.SystemException("Invalid field:" + f.ToString() + " val=" + val);
			}
		}
		
		internal System.String[] words = "now is the time for all good men to come to the aid of their country".Split(' ');
		
		internal virtual void  BuildDir(Directory dir, int nDocs, int maxFields, int maxFieldLen)
		{
			IndexWriter iw = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			iw.SetMaxBufferedDocs(10);
			for (int j = 0; j < nDocs; j++)
			{
				Document d = new Document();
				int nFields = r.Next(maxFields);
				for (int i = 0; i < nFields; i++)
				{
					int flen = r.Next(maxFieldLen);
					System.Text.StringBuilder sb = new System.Text.StringBuilder("^ ");
					while (sb.Length < flen)
						sb.Append(' ').Append(words[r.Next(words.Length)]);
					sb.Append(" $");
					Field.Store store = Field.Store.YES; // make random later
					Field.Index index = Field.Index.ANALYZED; // make random later
					d.Add(new Field("f" + i, sb.ToString(), store, index));
				}
				iw.AddDocument(d);
			}
			iw.Close();
		}
		
		
		internal virtual void  DoTest(int iter, int nThreads)
		{
			Thr[] tarr = new Thr[nThreads];
			for (int i = 0; i < nThreads; i++)
			{
				tarr[i] = new Thr(this, iter, new System.Random((System.Int32) r.Next(System.Int32.MaxValue)));
				tarr[i].Start();
			}
			for (int i = 0; i < nThreads; i++)
			{
				tarr[i].Join();
			}
			if (failure != null)
			{
                Assert.Fail(failure); // TestCase.fail(failure);
			}
		}
		
		[Test]
		public virtual void  TestLazyLoadThreadSafety()
		{
			r = NewRandom();
			dir1 = new RAMDirectory();
			// test w/ field sizes bigger than the buffer of an index input
			BuildDir(dir1, 15, 5, 2000);
			
			// do many small tests so the thread locals go away inbetween
			for (int i = 0; i < 100; i++)
			{
				ir1 = IndexReader.Open(dir1);
				DoTest(10, 100);
			}
		}
	}
}