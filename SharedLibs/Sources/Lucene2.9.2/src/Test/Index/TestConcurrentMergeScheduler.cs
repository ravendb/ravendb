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
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	[TestFixture]
	public class TestConcurrentMergeScheduler:LuceneTestCase
	{
		
		private static readonly Analyzer ANALYZER = new SimpleAnalyzer();
		
		private class FailOnlyOnFlush:MockRAMDirectory.Failure
		{
			internal bool doFail = false;
			
			public virtual void  SetDoFail()
			{
				this.doFail = true;
			}
			public virtual void  ClearDoFail()
			{
				this.doFail = false;
			}
			
			public override void  Eval(MockRAMDirectory dir)
			{
				if (doFail)
				{
					System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace();
					for (int i = 0; i < trace.FrameCount; i++)
					{
						System.Diagnostics.StackFrame sf = trace.GetFrame(i);
						if ("DoFlush".Equals(sf.GetMethod().Name))
						{
							//new RuntimeException().printStackTrace(System.out);
							throw new System.IO.IOException("now failing during flush");
						}
					}
				}
			}
		}
		
		// Make sure running BG merges still work fine even when
		// we are hitting exceptions during flushing.
		[Test]
		public virtual void  TestFlushExceptions()
		{
			
			MockRAMDirectory directory = new MockRAMDirectory();
			FailOnlyOnFlush failure = new FailOnlyOnFlush();
			directory.FailOn(failure);
			
			IndexWriter writer = new IndexWriter(directory, true, ANALYZER, true);
			ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
			writer.SetMergeScheduler(cms);
			writer.SetMaxBufferedDocs(2);
			Document doc = new Document();
			Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
			doc.Add(idField);
			for (int i = 0; i < 10; i++)
			{
				for (int j = 0; j < 20; j++)
				{
					idField.SetValue(System.Convert.ToString(i * 20 + j));
					writer.AddDocument(doc);
				}
				
				writer.AddDocument(doc);
				
				failure.SetDoFail();
				try
				{
					writer.Flush();
					Assert.Fail("failed to hit IOException");
				}
				catch (System.IO.IOException ioe)
				{
					failure.ClearDoFail();
				}
			}
			
			writer.Close();
			IndexReader reader = IndexReader.Open(directory);
			Assert.AreEqual(200, reader.NumDocs());
			reader.Close();
			directory.Close();
		}
		
		// Test that deletes committed after a merge started and
		// before it finishes, are correctly merged back:
		[Test]
		public virtual void  TestDeleteMerging()
		{
			
			RAMDirectory directory = new MockRAMDirectory();
			
			IndexWriter writer = new IndexWriter(directory, true, ANALYZER, true);
			ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
			writer.SetMergeScheduler(cms);
			
			LogDocMergePolicy mp = new LogDocMergePolicy(writer);
			writer.SetMergePolicy(mp);
			
			// Force degenerate merging so we can get a mix of
			// merging of segments with and without deletes at the
			// start:
			mp.SetMinMergeDocs(1000);
			
			Document doc = new Document();
			Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
			doc.Add(idField);
			for (int i = 0; i < 10; i++)
			{
				for (int j = 0; j < 100; j++)
				{
					idField.SetValue(System.Convert.ToString(i * 100 + j));
					writer.AddDocument(doc);
				}
				
				int delID = i;
				while (delID < 100 * (1 + i))
				{
					writer.DeleteDocuments(new Term("id", "" + delID));
					delID += 10;
				}
				
				writer.Flush();
			}
			
			writer.Close();
			IndexReader reader = IndexReader.Open(directory);
			// Verify that we did not lose any deletes...
			Assert.AreEqual(450, reader.NumDocs());
			reader.Close();
			directory.Close();
		}
		
		[Test]
		public virtual void  TestNoExtraFiles()
		{
			
			RAMDirectory directory = new MockRAMDirectory();
			
			for (int pass = 0; pass < 2; pass++)
			{
				
				bool autoCommit = pass == 0;
				IndexWriter writer = new IndexWriter(directory, autoCommit, ANALYZER, true);
				
				for (int iter = 0; iter < 7; iter++)
				{
					ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
					writer.SetMergeScheduler(cms);
					writer.SetMaxBufferedDocs(2);
					
					for (int j = 0; j < 21; j++)
					{
						Document doc = new Document();
						doc.Add(new Field("content", "a b c", Field.Store.NO, Field.Index.ANALYZED));
						writer.AddDocument(doc);
					}
					
					writer.Close();
					TestIndexWriter.AssertNoUnreferencedFiles(directory, "testNoExtraFiles autoCommit=" + autoCommit);
					
					// Reopen
					writer = new IndexWriter(directory, autoCommit, ANALYZER, false);
				}
				
				writer.Close();
			}
			
			directory.Close();
		}
		
		[Test]
		public virtual void  TestNoWaitClose()
		{
			RAMDirectory directory = new MockRAMDirectory();
			
			Document doc = new Document();
			Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
			doc.Add(idField);
			
			for (int pass = 0; pass < 2; pass++)
			{
				bool autoCommit = pass == 0;
				IndexWriter writer = new IndexWriter(directory, autoCommit, ANALYZER, true);
				
				for (int iter = 0; iter < 10; iter++)
				{
					ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
					writer.SetMergeScheduler(cms);
					writer.SetMaxBufferedDocs(2);
					writer.SetMergeFactor(100);
					
					for (int j = 0; j < 201; j++)
					{
						idField.SetValue(System.Convert.ToString(iter * 201 + j));
						writer.AddDocument(doc);
					}
					
					int delID = iter * 201;
					for (int j = 0; j < 20; j++)
					{
						writer.DeleteDocuments(new Term("id", System.Convert.ToString(delID)));
						delID += 5;
					}
					
					// Force a bunch of merge threads to kick off so we
					// stress out aborting them on close:
					writer.SetMergeFactor(3);
					writer.AddDocument(doc);
					writer.Flush();
					
					writer.Close(false);
					
					IndexReader reader = IndexReader.Open(directory);
					Assert.AreEqual((1 + iter) * 182, reader.NumDocs());
					reader.Close();
					
					// Reopen
					writer = new IndexWriter(directory, autoCommit, ANALYZER, false);
				}
				writer.Close();
			}
			
			directory.Close();
		}
	}
}