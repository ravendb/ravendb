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

using SimpleAnalyzer = Lucene.Net.Analysis.SimpleAnalyzer;
using Lucene.Net.Documents;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using TermFreqVector = Lucene.Net.Index.TermFreqVector;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using English = Lucene.Net.Util.English;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Search
{
	
	/// <summary> </summary>
	/// <version>  $rcs = ' $Id: TestMultiThreadTermVectors.java 759556 2009-03-28 19:10:55Z mikemccand $ ' ;
	/// </version>
    [TestFixture]
	public class TestMultiThreadTermVectors:LuceneTestCase
	{
		private RAMDirectory directory = new RAMDirectory();
		public int numDocs = 100;
		public int numThreads = 3;
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			IndexWriter writer = new IndexWriter(directory, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			//writer.setUseCompoundFile(false);
			//writer.infoStream = System.out;
			for (int i = 0; i < numDocs; i++)
			{
				Document doc = new Document();
				Fieldable fld = new Field("field", English.IntToEnglish(i), Field.Store.YES, Field.Index.NOT_ANALYZED, Field.TermVector.YES);
				doc.Add(fld);
				writer.AddDocument(doc);
			}
			writer.Close();
		}
		
		[Test]
		public virtual void  Test()
		{
			
			IndexReader reader = null;
			
			try
			{
				reader = IndexReader.Open(directory);
				for (int i = 1; i <= numThreads; i++)
					TestTermPositionVectors(reader, i);
			}
			catch (System.IO.IOException ioe)
			{
				Assert.Fail(ioe.Message);
			}
			finally
			{
				if (reader != null)
				{
					try
					{
						/** close the opened reader */
						reader.Close();
					}
					catch (System.IO.IOException ioe)
					{
						System.Console.Error.WriteLine(ioe.StackTrace);
					}
				}
			}
		}
		

		public virtual void  TestTermPositionVectors(IndexReader reader, int threadCount)
		{
			MultiThreadTermVectorsReader[] mtr = new MultiThreadTermVectorsReader[threadCount];
			for (int i = 0; i < threadCount; i++)
			{
				mtr[i] = new MultiThreadTermVectorsReader();
				mtr[i].Init(reader);
			}
			
			
			/** run until all threads finished */
			int threadsAlive = mtr.Length;
			while (threadsAlive > 0)
			{
				//System.out.println("Threads alive");
				System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * 10));
				threadsAlive = mtr.Length;
				for (int i = 0; i < mtr.Length; i++)
				{
					if (mtr[i].IsAlive() == true)
					{
						break;
					}
					
					threadsAlive--;
				}
			}
			
			long totalTime = 0L;
			for (int i = 0; i < mtr.Length; i++)
			{
				totalTime += mtr[i].timeElapsed;
				mtr[i] = null;
			}
			
			//System.out.println("threadcount: " + mtr.length + " average term vector time: " + totalTime/mtr.length);
		}
	}
	
	class MultiThreadTermVectorsReader : IThreadRunnable
	{
		
		private IndexReader reader = null;
		private SupportClass.ThreadClass t = null;
		
		private int runsToDo = 100;
		internal long timeElapsed = 0;
		
		
		public virtual void  Init(IndexReader reader)
		{
			this.reader = reader;
			timeElapsed = 0;
			t = new SupportClass.ThreadClass(new System.Threading.ThreadStart(this.Run));
			t.Start();
		}
		
		public virtual bool IsAlive()
		{
			if (t == null)
				return false;
			
			return t.IsAlive;
		}
		
		public virtual void  Run()
		{
			try
			{
				// run the test 100 times
				for (int i = 0; i < runsToDo; i++)
					TestTermVectors();
			}
			catch (System.Exception e)
			{
				System.Console.Error.WriteLine(e.StackTrace);
			}
			return ;
		}
		
		private void  TestTermVectors()
		{
			// check:
			int numDocs = reader.NumDocs();
			long start = 0L;
			for (int docId = 0; docId < numDocs; docId++)
			{
				start = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
				TermFreqVector[] vectors = reader.GetTermFreqVectors(docId);
				timeElapsed += (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) - start;
				
				// verify vectors result
				VerifyVectors(vectors, docId);
				
				start = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
				TermFreqVector vector = reader.GetTermFreqVector(docId, "field");
				timeElapsed += (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) - start;
				
				vectors = new TermFreqVector[1];
				vectors[0] = vector;
				
				VerifyVectors(vectors, docId);
			}
		}
		
		private void  VerifyVectors(TermFreqVector[] vectors, int num)
		{
			System.Text.StringBuilder temp = new System.Text.StringBuilder();
			System.String[] terms = null;
			for (int i = 0; i < vectors.Length; i++)
			{
				terms = vectors[i].GetTerms();
				for (int z = 0; z < terms.Length; z++)
				{
					temp.Append(terms[z]);
				}
			}
			
			if (!English.IntToEnglish(num).Trim().Equals(temp.ToString().Trim()))
				System.Console.Out.WriteLine("wrong term result");
		}
	}
}