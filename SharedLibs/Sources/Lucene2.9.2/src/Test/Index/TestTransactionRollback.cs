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
using MaxFieldLength = Lucene.Net.Index.IndexWriter.MaxFieldLength;
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	/// <summary> Test class to illustrate using IndexDeletionPolicy to provide multi-level rollback capability.
	/// This test case creates an index of records 1 to 100, introducing a commit point every 10 records.
	/// 
	/// A "keep all" deletion policy is used to ensure we keep all commit points for testing purposes
	/// </summary>
	
    [TestFixture]
	public class TestTransactionRollback:LuceneTestCase
	{
		
		private const System.String FIELD_RECORD_ID = "record_id";
		private Directory dir;
		
		
		//Rolls back index to a chosen ID
		private void  RollBackLast(int id)
		{
			
			// System.out.println("Attempting to rollback to "+id);
			System.String ids = "-" + id;
			IndexCommit last = null;
			System.Collections.ICollection commits = IndexReader.ListCommits(dir);
			for (System.Collections.IEnumerator iterator = commits.GetEnumerator(); iterator.MoveNext(); )
			{
				IndexCommit commit = (IndexCommit) iterator.Current;
                System.Collections.Generic.IDictionary<string, string> ud = commit.GetUserData();
				if (ud.Count > 0)
					if (((System.String) ud["index"]).EndsWith(ids))
						last = commit;
			}
			
			if (last == null)
				throw new System.SystemException("Couldn't find commit point " + id);
			
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), new RollbackDeletionPolicy(this, id), MaxFieldLength.UNLIMITED, last);
            System.Collections.Generic.IDictionary<string, string> data = new System.Collections.Generic.Dictionary<string, string>();
			data["index"] = "Rolled back to 1-" + id;
			w.Commit(data);
			w.Close();
		}
		
        [Test]
		public virtual void  TestRepeatedRollBacks()
		{
			
			int expectedLastRecordId = 100;
			while (expectedLastRecordId > 10)
			{
				expectedLastRecordId -= 10;
				RollBackLast(expectedLastRecordId);
				
				System.Collections.BitArray expecteds = new System.Collections.BitArray((100 % 64 == 0?100 / 64:100 / 64 + 1) * 64);
                for (int i = 1; i < (expectedLastRecordId + 1); i++) { expecteds.Set(i, true); }
				CheckExpecteds(expecteds);
			}
		}
		
		private void  CheckExpecteds(System.Collections.BitArray expecteds)
		{
			IndexReader r = IndexReader.Open(dir);
			
			//Perhaps not the most efficient approach but meets our needs here.
			for (int i = 0; i < r.MaxDoc(); i++)
			{
				if (!r.IsDeleted(i))
				{
					System.String sval = r.Document(i).Get(FIELD_RECORD_ID);
					if (sval != null)
                    {
                        int val = System.Int32.Parse(sval);
                        Assert.IsTrue(expecteds.Get(val), "Did not expect document #" + val);
                        expecteds.Set(val, false); 
                    }
				}
			}
			r.Close();
			Assert.AreEqual(0, SupportClass.BitSetSupport.Cardinality(expecteds), "Should have 0 docs remaining ");
		}
		
		/*
		private void showAvailableCommitPoints() throws Exception {
		Collection commits = IndexReader.listCommits(dir);
		for (Iterator iterator = commits.iterator(); iterator.hasNext();) {
		IndexCommit comm = (IndexCommit) iterator.next();
		System.out.print("\t Available commit point:["+comm.getUserData()+"] files=");
		Collection files = comm.getFileNames();
		for (Iterator iterator2 = files.iterator(); iterator2.hasNext();) {
		String filename = (String) iterator2.next();
		System.out.print(filename+", ");				
		}
		System.out.println();
		}
		}
		*/
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			dir = new MockRAMDirectory();
			
			//Build index, of records 1 to 100, committing after each batch of 10
			IndexDeletionPolicy sdp = new KeepAllDeletionPolicy(this);
			IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), sdp, MaxFieldLength.UNLIMITED);
			for (int currentRecordId = 1; currentRecordId <= 100; currentRecordId++)
			{
				Document doc = new Document();
				doc.Add(new Field(FIELD_RECORD_ID, "" + currentRecordId, Field.Store.YES, Field.Index.ANALYZED));
				w.AddDocument(doc);
				
				if (currentRecordId % 10 == 0)
				{
                    System.Collections.Generic.IDictionary<string, string> data = new System.Collections.Generic.Dictionary<string,string>();
					data["index"] = "records 1-" + currentRecordId;
					w.Commit(data);
				}
			}
			
			w.Close();
		}
		
		// Rolls back to previous commit point
		internal class RollbackDeletionPolicy : IndexDeletionPolicy
		{
			private void  InitBlock(TestTransactionRollback enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTransactionRollback enclosingInstance;
			public TestTransactionRollback Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			private int rollbackPoint;
			
			public RollbackDeletionPolicy(TestTransactionRollback enclosingInstance, int rollbackPoint)
			{
				InitBlock(enclosingInstance);
				this.rollbackPoint = rollbackPoint;
			}
			
			public virtual void  OnCommit(System.Collections.IList commits)
			{
			}
			
			public virtual void  OnInit(System.Collections.IList commits)
			{
				for (System.Collections.IEnumerator iterator = commits.GetEnumerator(); iterator.MoveNext(); )
				{
					IndexCommit commit = (IndexCommit) iterator.Current;
                    System.Collections.Generic.IDictionary<string, string> userData = commit.GetUserData();
					if (userData.Count > 0)
					{
						// Label for a commit point is "Records 1-30"
						// This code reads the last id ("30" in this example) and deletes it
						// if it is after the desired rollback point
						System.String x = (System.String) userData["index"];
						System.String lastVal = x.Substring(x.LastIndexOf("-") + 1);
						int last = System.Int32.Parse(lastVal);
						if (last > rollbackPoint)
						{
							/*
							System.out.print("\tRolling back commit point:" +
							" UserData="+commit.getUserData() +")  ("+(commits.size()-1)+" commit points left) files=");
							Collection files = commit.getFileNames();
							for (Iterator iterator2 = files.iterator(); iterator2.hasNext();) {
							System.out.print(" "+iterator2.next());				
							}
							System.out.println();
							*/
							
							commit.Delete();
						}
					}
				}
			}
		}
		
		internal class DeleteLastCommitPolicy : IndexDeletionPolicy
		{
			public DeleteLastCommitPolicy(TestTransactionRollback enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestTransactionRollback enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTransactionRollback enclosingInstance;
			public TestTransactionRollback Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			public virtual void  OnCommit(System.Collections.IList commits)
			{
			}
			
			public virtual void  OnInit(System.Collections.IList commits)
			{
				((IndexCommit) commits[commits.Count - 1]).Delete();
			}
		}
		
        [Test]
		public virtual void  TestRollbackDeletionPolicy()
		{
			for (int i = 0; i < 2; i++)
			{
				// Unless you specify a prior commit point, rollback
				// should not work:
				new IndexWriter(dir, new WhitespaceAnalyzer(), new DeleteLastCommitPolicy(this), MaxFieldLength.UNLIMITED).Close();
				IndexReader r = IndexReader.Open(dir);
				Assert.AreEqual(100, r.NumDocs());
				r.Close();
			}
		}
		
		// Keeps all commit points (used to build index)
		internal class KeepAllDeletionPolicy : IndexDeletionPolicy
		{
			public KeepAllDeletionPolicy(TestTransactionRollback enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestTransactionRollback enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestTransactionRollback enclosingInstance;
			public TestTransactionRollback Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public virtual void  OnCommit(System.Collections.IList commits)
			{
			}
			public virtual void  OnInit(System.Collections.IList commits)
			{
			}
		}
	}
}