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
using ConcurrentMergeScheduler = Lucene.Net.Index.ConcurrentMergeScheduler;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using MergePolicy = Lucene.Net.Index.MergePolicy;
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net
{
	
	
	/// <summary> Holds tests cases to verify external APIs are accessible
	/// while not being in Lucene.Net.Index package.
	/// </summary>
	[TestFixture]
	public class TestMergeSchedulerExternal:LuceneTestCase
	{
		
		internal volatile bool mergeCalled;
		internal volatile bool mergeThreadCreated;
		internal volatile bool excCalled;
		
		[Serializable]
		private class MyMergeException:System.SystemException
		{
			private void  InitBlock(TestMergeSchedulerExternal enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestMergeSchedulerExternal enclosingInstance;
			public TestMergeSchedulerExternal Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal Directory dir;
			public MyMergeException(TestMergeSchedulerExternal enclosingInstance, System.Exception exc, Directory dir):base("", exc)
			{
				InitBlock(enclosingInstance);
				this.dir = dir;
			}
		}
		
		private class MyMergeScheduler:ConcurrentMergeScheduler
		{
			public MyMergeScheduler(TestMergeSchedulerExternal enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestMergeSchedulerExternal enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestMergeSchedulerExternal enclosingInstance;
			public TestMergeSchedulerExternal Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			private class MyMergeThread:ConcurrentMergeScheduler.MergeThread
			{
				private void  InitBlock(MyMergeScheduler enclosingInstance)
				{
					this.enclosingInstance = enclosingInstance;
				}
				private MyMergeScheduler enclosingInstance;
				public new MyMergeScheduler Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				public MyMergeThread(MyMergeScheduler enclosingInstance, IndexWriter writer, MergePolicy.OneMerge merge):base(enclosingInstance, writer, merge)
				{
					InitBlock(enclosingInstance);
					Enclosing_Instance.Enclosing_Instance.mergeThreadCreated = true;
				}
			}
			
			protected /*internal*/ override MergeThread GetMergeThread(IndexWriter writer, MergePolicy.OneMerge merge)
			{
				MergeThread thread = new MyMergeThread(this, writer, merge);
				thread.SetThreadPriority(GetMergeThreadPriority());
				thread.IsBackground = true;
				thread.Name = "MyMergeThread";
				return thread;
			}
			
			protected /*internal*/ override void  HandleMergeException(System.Exception t)
			{
				Enclosing_Instance.excCalled = true;
			}
			
			protected /*internal*/ override void  DoMerge(MergePolicy.OneMerge merge)
			{
				Enclosing_Instance.mergeCalled = true;
				base.DoMerge(merge);
			}
		}
		
		private class FailOnlyOnMerge:MockRAMDirectory.Failure
		{
			public override /*virtual*/ void  Eval(MockRAMDirectory dir)
			{
                System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace();
				for (int i = 0; i < trace.FrameCount; i++)
				{
                    System.Diagnostics.StackFrame sf = trace.GetFrame(i);
					if ("DoMerge".Equals(sf.GetMethod().Name))
						throw new System.IO.IOException("now failing during merge");
				}
			}
		}
		
		[Test]
		public virtual void  TestSubclassConcurrentMergeScheduler()
		{
			MockRAMDirectory dir = new MockRAMDirectory();
			dir.FailOn(new FailOnlyOnMerge());
			
			Document doc = new Document();
			Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
			doc.Add(idField);
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
			MyMergeScheduler ms = new MyMergeScheduler(this);
			writer.SetMergeScheduler(ms);
			writer.SetMaxBufferedDocs(2);
			writer.SetRAMBufferSizeMB(Lucene.Net.Index.IndexWriter.DISABLE_AUTO_FLUSH);
			for (int i = 0; i < 20; i++)
				writer.AddDocument(doc);
			
			ms.Sync();
			writer.Close();
			
			Assert.IsTrue(mergeThreadCreated);
			Assert.IsTrue(mergeCalled);
			Assert.IsTrue(excCalled);
			dir.Close();
			Assert.IsTrue(ConcurrentMergeScheduler.AnyUnhandledExceptions());
		}
	}
}