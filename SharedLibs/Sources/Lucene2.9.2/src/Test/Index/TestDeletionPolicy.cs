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
using Directory = Lucene.Net.Store.Directory;
using MockRAMDirectory = Lucene.Net.Store.MockRAMDirectory;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using TermQuery = Lucene.Net.Search.TermQuery;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Index
{
	
	/*
	Verify we can read the pre-2.1 file format, do searches
	against it, and add documents to it.*/
	
	[TestFixture]
	public class TestDeletionPolicy:LuceneTestCase
	{
		private void  VerifyCommitOrder(System.Collections.IList commits)
		{
			IndexCommit firstCommit = ((IndexCommit) commits[0]);
			long last = SegmentInfos.GenerationFromSegmentsFileName(firstCommit.GetSegmentsFileName());
			Assert.AreEqual(last, firstCommit.GetGeneration());
			long lastVersion = firstCommit.GetVersion();
			long lastTimestamp = firstCommit.GetTimestamp();
			for (int i = 1; i < commits.Count; i++)
			{
				IndexCommit commit = ((IndexCommit) commits[i]);
				long now = SegmentInfos.GenerationFromSegmentsFileName(commit.GetSegmentsFileName());
				long nowVersion = commit.GetVersion();
				long nowTimestamp = commit.GetTimestamp();
				Assert.IsTrue(now > last, "SegmentInfos commits are out-of-order");
				Assert.IsTrue(nowVersion > lastVersion, "SegmentInfos versions are out-of-order");
				Assert.IsTrue(nowTimestamp >= lastTimestamp, "SegmentInfos timestamps are out-of-order: now=" + nowTimestamp + " vs last=" + lastTimestamp);
				Assert.AreEqual(now, commit.GetGeneration());
				last = now;
				lastVersion = nowVersion;
				lastTimestamp = nowTimestamp;
			}
		}
		
		internal class KeepAllDeletionPolicy : IndexDeletionPolicy
		{
			public KeepAllDeletionPolicy(TestDeletionPolicy enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestDeletionPolicy enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestDeletionPolicy enclosingInstance;
			public TestDeletionPolicy Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal int numOnInit;
			internal int numOnCommit;
			internal Directory dir;
			public virtual void  OnInit(System.Collections.IList commits)
			{
				Enclosing_Instance.VerifyCommitOrder(commits);
				numOnInit++;
			}
			public virtual void  OnCommit(System.Collections.IList commits)
			{
				IndexCommit lastCommit = (IndexCommit) commits[commits.Count - 1];
				IndexReader r = IndexReader.Open(dir);
				Assert.AreEqual(r.IsOptimized(), lastCommit.IsOptimized(), "lastCommit.isOptimized()=" + lastCommit.IsOptimized() + " vs IndexReader.isOptimized=" + r.IsOptimized());
				r.Close();
				Enclosing_Instance.VerifyCommitOrder(commits);
				numOnCommit++;
			}
		}
		
		/// <summary> This is useful for adding to a big index w/ autoCommit
		/// false when you know readers are not using it.
		/// </summary>
		internal class KeepNoneOnInitDeletionPolicy : IndexDeletionPolicy
		{
			public KeepNoneOnInitDeletionPolicy(TestDeletionPolicy enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestDeletionPolicy enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestDeletionPolicy enclosingInstance;
			public TestDeletionPolicy Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal int numOnInit;
			internal int numOnCommit;
			public virtual void  OnInit(System.Collections.IList commits)
			{
				Enclosing_Instance.VerifyCommitOrder(commits);
				numOnInit++;
				// On init, delete all commit points:
				System.Collections.IEnumerator it = commits.GetEnumerator();
				while (it.MoveNext())
				{
					IndexCommit commit = (IndexCommit) it.Current;
					commit.Delete();
					Assert.IsTrue(commit.IsDeleted());
				}
			}
			public virtual void  OnCommit(System.Collections.IList commits)
			{
				Enclosing_Instance.VerifyCommitOrder(commits);
				int size = commits.Count;
				// Delete all but last one:
				for (int i = 0; i < size - 1; i++)
				{
					((IndexCommit) commits[i]).Delete();
				}
				numOnCommit++;
			}
		}
		
		internal class KeepLastNDeletionPolicy : IndexDeletionPolicy
		{
			private void  InitBlock(TestDeletionPolicy enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestDeletionPolicy enclosingInstance;
			public TestDeletionPolicy Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			internal int numOnInit;
			internal int numOnCommit;
			internal int numToKeep;
			internal int numDelete;
			internal System.Collections.Hashtable seen = new System.Collections.Hashtable();
			
			public KeepLastNDeletionPolicy(TestDeletionPolicy enclosingInstance, int numToKeep)
			{
				InitBlock(enclosingInstance);
				this.numToKeep = numToKeep;
			}
			
			public virtual void  OnInit(System.Collections.IList commits)
			{
				Enclosing_Instance.VerifyCommitOrder(commits);
				numOnInit++;
				// do no deletions on init
				DoDeletes(commits, false);
			}
			
			public virtual void  OnCommit(System.Collections.IList commits)
			{
				Enclosing_Instance.VerifyCommitOrder(commits);
				DoDeletes(commits, true);
			}
			
			private void  DoDeletes(System.Collections.IList commits, bool isCommit)
			{
				
				// Assert that we really are only called for each new
				// commit:
				if (isCommit)
				{
					System.String fileName = ((IndexCommit) commits[commits.Count - 1]).GetSegmentsFileName();
					if (seen.Contains(fileName))
					{
						throw new System.SystemException("onCommit was called twice on the same commit point: " + fileName);
					}
					seen.Add(fileName, fileName);
					numOnCommit++;
				}
				int size = commits.Count;
				for (int i = 0; i < size - numToKeep; i++)
				{
					((IndexCommit) commits[i]).Delete();
					numDelete++;
				}
			}
		}
		
		/*
		* Delete a commit only when it has been obsoleted by N
		* seconds.
		*/
		internal class ExpirationTimeDeletionPolicy : IndexDeletionPolicy
		{
			private void  InitBlock(TestDeletionPolicy enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestDeletionPolicy enclosingInstance;
			public TestDeletionPolicy Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			
			internal Directory dir;
			internal double expirationTimeSeconds;
			internal int numDelete;
			
			public ExpirationTimeDeletionPolicy(TestDeletionPolicy enclosingInstance, Directory dir, double seconds)
			{
				InitBlock(enclosingInstance);
				this.dir = dir;
				this.expirationTimeSeconds = seconds;
			}
			
			public virtual void  OnInit(System.Collections.IList commits)
			{
				Enclosing_Instance.VerifyCommitOrder(commits);
				OnCommit(commits);
			}
			
			public virtual void  OnCommit(System.Collections.IList commits)
			{
				Enclosing_Instance.VerifyCommitOrder(commits);
				
				IndexCommit lastCommit = (IndexCommit) commits[commits.Count - 1];
				
				// Any commit older than expireTime should be deleted:
				double expireTime = dir.FileModified(lastCommit.GetSegmentsFileName()) / 1000.0 - expirationTimeSeconds;
				
				System.Collections.IEnumerator it = commits.GetEnumerator();
				
				while (it.MoveNext())
				{
					IndexCommit commit = (IndexCommit) it.Current;
					double modTime = dir.FileModified(commit.GetSegmentsFileName()) / 1000.0;
					if (commit != lastCommit && modTime < expireTime)
					{
						commit.Delete();
						numDelete += 1;
					}
				}
			}
		}
		
		/*
		* Test "by time expiration" deletion policy:
		*/
		[Test]
		public virtual void  TestExpirationTimeDeletionPolicy()
		{
			
			double SECONDS = 2.0;
			
			bool autoCommit = false;
			bool useCompoundFile = true;
			
			Directory dir = new RAMDirectory();
			ExpirationTimeDeletionPolicy policy = new ExpirationTimeDeletionPolicy(this, dir, SECONDS);
			IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
			writer.SetUseCompoundFile(useCompoundFile);
			writer.Close();
			
			long lastDeleteTime = 0;
			for (int i = 0; i < 7; i++)
			{
				// Record last time when writer performed deletes of
				// past commits
				lastDeleteTime = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond);
				writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
				writer.SetUseCompoundFile(useCompoundFile);
				for (int j = 0; j < 17; j++)
				{
					AddDoc(writer);
				}
				writer.Close();
				
				// Make sure to sleep long enough so that some commit
				// points will be deleted:
				System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64) 10000 * (int) (1000.0 * (SECONDS / 5.0))));
			}
			
			// First, make sure the policy in fact deleted something:
			Assert.IsTrue(policy.numDelete > 0, "no commits were deleted");
			
			// Then simplistic check: just verify that the
			// segments_N's that still exist are in fact within SECONDS
			// seconds of the last one's mod time, and, that I can
			// open a reader on each:
			long gen = SegmentInfos.GetCurrentSegmentGeneration(dir);
			
			System.String fileName = IndexFileNames.FileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen);
			dir.DeleteFile(IndexFileNames.SEGMENTS_GEN);
			while (gen > 0)
			{
				try
				{
					IndexReader reader = IndexReader.Open(dir);
					reader.Close();
					fileName = IndexFileNames.FileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen);
					long modTime = dir.FileModified(fileName);
					Assert.IsTrue(lastDeleteTime - modTime <= (SECONDS * 1000), "commit point was older than " + SECONDS + " seconds (" + (lastDeleteTime - modTime) + " msec) but did not get deleted");
				}
				catch (System.IO.IOException e)
				{
					// OK
					break;
				}
				
				dir.DeleteFile(IndexFileNames.FileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
				gen--;
			}
			
			dir.Close();
		}
		
		/*
		* Test a silly deletion policy that keeps all commits around.
		*/
		[Test]
		public virtual void  TestKeepAllDeletionPolicy()
		{
			
			for (int pass = 0; pass < 4; pass++)
			{
				
				bool autoCommit = pass < 2;
				bool useCompoundFile = (pass % 2) > 0;
				
				// Never deletes a commit
				KeepAllDeletionPolicy policy = new KeepAllDeletionPolicy(this);
				
				Directory dir = new RAMDirectory();
				policy.dir = dir;
				
				IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
				writer.SetMaxBufferedDocs(10);
				writer.SetUseCompoundFile(useCompoundFile);
				writer.SetMergeScheduler(new SerialMergeScheduler());
				for (int i = 0; i < 107; i++)
				{
					AddDoc(writer);
					if (autoCommit && i % 10 == 0)
						writer.Commit();
				}
				writer.Close();
				
				writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
				writer.SetUseCompoundFile(useCompoundFile);
				writer.Optimize();
				writer.Close();
				
				Assert.AreEqual(2, policy.numOnInit);
				if (!autoCommit)
				// If we are not auto committing then there should
				// be exactly 2 commits (one per close above):
					Assert.AreEqual(2, policy.numOnCommit);
				
				// Test listCommits
				System.Collections.ICollection commits = IndexReader.ListCommits(dir);
				if (!autoCommit)
				// 1 from opening writer + 2 from closing writer
					Assert.AreEqual(3, commits.Count);
				// 1 from opening writer + 2 from closing writer +
				// 11 from calling writer.commit() explicitly above
				else
					Assert.AreEqual(14, commits.Count);
				
				System.Collections.IEnumerator it = commits.GetEnumerator();
				// Make sure we can open a reader on each commit:
				while (it.MoveNext())
				{
					IndexCommit commit = (IndexCommit) it.Current;
					IndexReader r = IndexReader.Open(commit, null);
					r.Close();
				}
				
				// Simplistic check: just verify all segments_N's still
				// exist, and, I can open a reader on each:
				dir.DeleteFile(IndexFileNames.SEGMENTS_GEN);
				long gen = SegmentInfos.GetCurrentSegmentGeneration(dir);
				while (gen > 0)
				{
					IndexReader reader = IndexReader.Open(dir);
					reader.Close();
					dir.DeleteFile(IndexFileNames.FileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
					gen--;
					
					if (gen > 0)
					{
						// Now that we've removed a commit point, which
						// should have orphan'd at least one index file.
						// Open & close a writer and assert that it
						// actually removed something:
						int preCount = dir.ListAll().Length;
						writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, policy, IndexWriter.MaxFieldLength.LIMITED);
						writer.Close();
						int postCount = dir.ListAll().Length;
						Assert.IsTrue(postCount < preCount);
					}
				}
				
				dir.Close();
			}
		}
		
		/* Uses KeepAllDeletionPolicy to keep all commits around,
		* then, opens a new IndexWriter on a previous commit
		* point. */
		[Test]
		public virtual void  TestOpenPriorSnapshot()
		{
			
			// Never deletes a commit
			KeepAllDeletionPolicy policy = new KeepAllDeletionPolicy(this);
			
			Directory dir = new MockRAMDirectory();
			policy.dir = dir;
			
			IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), policy, IndexWriter.MaxFieldLength.LIMITED);
			writer.SetMaxBufferedDocs(2);
			for (int i = 0; i < 10; i++)
			{
				AddDoc(writer);
				if ((1 + i) % 2 == 0)
					writer.Commit();
			}
			writer.Close();
			
			System.Collections.ICollection commits = IndexReader.ListCommits(dir);
			Assert.AreEqual(6, commits.Count);
			IndexCommit lastCommit = null;
			System.Collections.IEnumerator it = commits.GetEnumerator();
			while (it.MoveNext())
			{
				IndexCommit commit = (IndexCommit) it.Current;
				if (lastCommit == null || commit.GetGeneration() > lastCommit.GetGeneration())
					lastCommit = commit;
			}
			Assert.IsTrue(lastCommit != null);
			
			// Now add 1 doc and optimize
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), policy, IndexWriter.MaxFieldLength.LIMITED);
			AddDoc(writer);
			Assert.AreEqual(11, writer.NumDocs());
			writer.Optimize();
			writer.Close();
			
			Assert.AreEqual(7, IndexReader.ListCommits(dir).Count);
			
			// Now open writer on the commit just before optimize:
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), policy, IndexWriter.MaxFieldLength.LIMITED, lastCommit);
			Assert.AreEqual(10, writer.NumDocs());
			
			// Should undo our rollback:
			writer.Rollback();
			
			IndexReader r = IndexReader.Open(dir);
			// Still optimized, still 11 docs
			Assert.IsTrue(r.IsOptimized());
			Assert.AreEqual(11, r.NumDocs());
			r.Close();
			
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), policy, IndexWriter.MaxFieldLength.LIMITED, lastCommit);
			Assert.AreEqual(10, writer.NumDocs());
			// Commits the rollback:
			writer.Close();
			
			// Now 8 because we made another commit
			Assert.AreEqual(8, IndexReader.ListCommits(dir).Count);
			
			r = IndexReader.Open(dir);
			// Not optimized because we rolled it back, and now only
			// 10 docs
			Assert.IsTrue(!r.IsOptimized());
			Assert.AreEqual(10, r.NumDocs());
			r.Close();
			
			// Reoptimize
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), policy, IndexWriter.MaxFieldLength.LIMITED);
			writer.Optimize();
			writer.Close();
			
			r = IndexReader.Open(dir);
			Assert.IsTrue(r.IsOptimized());
			Assert.AreEqual(10, r.NumDocs());
			r.Close();
			
			// Now open writer on the commit just before optimize,
			// but this time keeping only the last commit:
			writer = new IndexWriter(dir, new WhitespaceAnalyzer(), new KeepOnlyLastCommitDeletionPolicy(), IndexWriter.MaxFieldLength.LIMITED, lastCommit);
			Assert.AreEqual(10, writer.NumDocs());
			
			// Reader still sees optimized index, because writer
			// opened on the prior commit has not yet committed:
			r = IndexReader.Open(dir);
			Assert.IsTrue(r.IsOptimized());
			Assert.AreEqual(10, r.NumDocs());
			r.Close();
			
			writer.Close();
			
			// Now reader sees unoptimized index:
			r = IndexReader.Open(dir);
			Assert.IsTrue(!r.IsOptimized());
			Assert.AreEqual(10, r.NumDocs());
			r.Close();
			
			dir.Close();
		}
		
		
		/* Test keeping NO commit points.  This is a viable and
		* useful case eg where you want to build a big index with
		* autoCommit false and you know there are no readers.
		*/
		[Test]
		public virtual void  TestKeepNoneOnInitDeletionPolicy()
		{
			
			for (int pass = 0; pass < 4; pass++)
			{
				
				bool autoCommit = pass < 2;
				bool useCompoundFile = (pass % 2) > 0;
				
				KeepNoneOnInitDeletionPolicy policy = new KeepNoneOnInitDeletionPolicy(this);
				
				Directory dir = new RAMDirectory();
				
				IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
				writer.SetMaxBufferedDocs(10);
				writer.SetUseCompoundFile(useCompoundFile);
				for (int i = 0; i < 107; i++)
				{
					AddDoc(writer);
				}
				writer.Close();
				
				writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
				writer.SetUseCompoundFile(useCompoundFile);
				writer.Optimize();
				writer.Close();
				
				Assert.AreEqual(2, policy.numOnInit);
				if (!autoCommit)
				// If we are not auto committing then there should
				// be exactly 2 commits (one per close above):
					Assert.AreEqual(2, policy.numOnCommit);
				
				// Simplistic check: just verify the index is in fact
				// readable:
				IndexReader reader = IndexReader.Open(dir);
				reader.Close();
				
				dir.Close();
			}
		}
		
		/*
		* Test a deletion policy that keeps last N commits.
		*/
		[Test]
		public virtual void  TestKeepLastNDeletionPolicy()
		{
			
			int N = 5;
			
			for (int pass = 0; pass < 4; pass++)
			{
				
				bool autoCommit = pass < 2;
				bool useCompoundFile = (pass % 2) > 0;
				
				Directory dir = new RAMDirectory();
				
				KeepLastNDeletionPolicy policy = new KeepLastNDeletionPolicy(this, N);
				
				for (int j = 0; j < N + 1; j++)
				{
					IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
					writer.SetMaxBufferedDocs(10);
					writer.SetUseCompoundFile(useCompoundFile);
					for (int i = 0; i < 17; i++)
					{
						AddDoc(writer);
					}
					writer.Optimize();
					writer.Close();
				}
				
				Assert.IsTrue(policy.numDelete > 0);
				Assert.AreEqual(N + 1, policy.numOnInit);
				if (autoCommit)
				{
					Assert.IsTrue(policy.numOnCommit > 1);
				}
				else
				{
					Assert.AreEqual(N + 1, policy.numOnCommit);
				}
				
				// Simplistic check: just verify only the past N segments_N's still
				// exist, and, I can open a reader on each:
				dir.DeleteFile(IndexFileNames.SEGMENTS_GEN);
				long gen = SegmentInfos.GetCurrentSegmentGeneration(dir);
				for (int i = 0; i < N + 1; i++)
				{
					try
					{
						IndexReader reader = IndexReader.Open(dir);
						reader.Close();
						if (i == N)
						{
							Assert.Fail("should have failed on commits prior to last " + N);
						}
					}
					catch (System.IO.IOException e)
					{
						if (i != N)
						{
							throw e;
						}
					}
					if (i < N)
					{
						dir.DeleteFile(IndexFileNames.FileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
					}
					gen--;
				}
				
				dir.Close();
			}
		}
		
		/*
		* Test a deletion policy that keeps last N commits
		* around, with reader doing deletes.
		*/
		[Test]
		public virtual void  TestKeepLastNDeletionPolicyWithReader()
		{
			
			int N = 10;
			
			for (int pass = 0; pass < 4; pass++)
			{
				
				bool autoCommit = pass < 2;
				bool useCompoundFile = (pass % 2) > 0;
				
				KeepLastNDeletionPolicy policy = new KeepLastNDeletionPolicy(this, N);
				
				Directory dir = new RAMDirectory();
				IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
				writer.SetUseCompoundFile(useCompoundFile);
				writer.Close();
				Term searchTerm = new Term("content", "aaa");
				Query query = new TermQuery(searchTerm);
				
				for (int i = 0; i < N + 1; i++)
				{
					writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
					writer.SetUseCompoundFile(useCompoundFile);
					for (int j = 0; j < 17; j++)
					{
						AddDoc(writer);
					}
					// this is a commit when autoCommit=false:
					writer.Close();
					IndexReader reader = IndexReader.Open(dir, policy);
					reader.DeleteDocument(3 * i + 1);
					reader.SetNorm(4 * i + 1, "content", 2.0F);
					IndexSearcher searcher = new IndexSearcher(reader);
					ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
					Assert.AreEqual(16 * (1 + i), hits.Length);
					// this is a commit when autoCommit=false:
					reader.Close();
					searcher.Close();
				}
				writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
				writer.SetUseCompoundFile(useCompoundFile);
				writer.Optimize();
				// this is a commit when autoCommit=false:
				writer.Close();
				
				Assert.AreEqual(2 * (N + 2), policy.numOnInit);
				if (!autoCommit)
					Assert.AreEqual(2 * (N + 2) - 1, policy.numOnCommit);
				
				IndexSearcher searcher2 = new IndexSearcher(dir);
				ScoreDoc[] hits2 = searcher2.Search(query, null, 1000).scoreDocs;
				Assert.AreEqual(176, hits2.Length);
				
				// Simplistic check: just verify only the past N segments_N's still
				// exist, and, I can open a reader on each:
				long gen = SegmentInfos.GetCurrentSegmentGeneration(dir);
				
				dir.DeleteFile(IndexFileNames.SEGMENTS_GEN);
				int expectedCount = 176;
				
				for (int i = 0; i < N + 1; i++)
				{
					try
					{
						IndexReader reader = IndexReader.Open(dir);
						
						// Work backwards in commits on what the expected
						// count should be.  Only check this in the
						// autoCommit false case:
						if (!autoCommit)
						{
							searcher2 = new IndexSearcher(reader);
							hits2 = searcher2.Search(query, null, 1000).scoreDocs;
							if (i > 1)
							{
								if (i % 2 == 0)
								{
									expectedCount += 1;
								}
								else
								{
									expectedCount -= 17;
								}
							}
							Assert.AreEqual(expectedCount, hits2.Length);
							searcher2.Close();
						}
						reader.Close();
						if (i == N)
						{
							Assert.Fail("should have failed on commits before last 5");
						}
					}
					catch (System.IO.IOException e)
					{
						if (i != N)
						{
							throw e;
						}
					}
					if (i < N)
					{
						dir.DeleteFile(IndexFileNames.FileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
					}
					gen--;
				}
				
				dir.Close();
			}
		}
		
		/*
		* Test a deletion policy that keeps last N commits
		* around, through creates.
		*/
		[Test]
		public virtual void  TestKeepLastNDeletionPolicyWithCreates()
		{
			
			int N = 10;
			
			for (int pass = 0; pass < 4; pass++)
			{
				
				bool autoCommit = pass < 2;
				bool useCompoundFile = (pass % 2) > 0;
				
				KeepLastNDeletionPolicy policy = new KeepLastNDeletionPolicy(this, N);
				
				Directory dir = new RAMDirectory();
				IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
				writer.SetMaxBufferedDocs(10);
				writer.SetUseCompoundFile(useCompoundFile);
				writer.Close();
				Term searchTerm = new Term("content", "aaa");
				Query query = new TermQuery(searchTerm);
				
				for (int i = 0; i < N + 1; i++)
				{
					
					writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, policy);
					writer.SetMaxBufferedDocs(10);
					writer.SetUseCompoundFile(useCompoundFile);
					for (int j = 0; j < 17; j++)
					{
						AddDoc(writer);
					}
					// this is a commit when autoCommit=false:
					writer.Close();
					IndexReader reader = IndexReader.Open(dir, policy);
					reader.DeleteDocument(3);
					reader.SetNorm(5, "content", 2.0F);
					IndexSearcher searcher = new IndexSearcher(reader);
					ScoreDoc[] hits = searcher.Search(query, null, 1000).scoreDocs;
					Assert.AreEqual(16, hits.Length);
					// this is a commit when autoCommit=false:
					reader.Close();
					searcher.Close();
					
					writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, policy);
					// This will not commit: there are no changes
					// pending because we opened for "create":
					writer.Close();
				}
				
				Assert.AreEqual(1 + 3 * (N + 1), policy.numOnInit);
				if (!autoCommit)
					Assert.AreEqual(3 * (N + 1), policy.numOnCommit);
				
				IndexSearcher searcher2 = new IndexSearcher(dir);
				ScoreDoc[] hits2 = searcher2.Search(query, null, 1000).scoreDocs;
				Assert.AreEqual(0, hits2.Length);
				
				// Simplistic check: just verify only the past N segments_N's still
				// exist, and, I can open a reader on each:
				long gen = SegmentInfos.GetCurrentSegmentGeneration(dir);
				
				dir.DeleteFile(IndexFileNames.SEGMENTS_GEN);
				int expectedCount = 0;
				
				for (int i = 0; i < N + 1; i++)
				{
					try
					{
						IndexReader reader = IndexReader.Open(dir);
						
						// Work backwards in commits on what the expected
						// count should be.  Only check this in the
						// autoCommit false case:
						if (!autoCommit)
						{
							searcher2 = new IndexSearcher(reader);
							hits2 = searcher2.Search(query, null, 1000).scoreDocs;
							Assert.AreEqual(expectedCount, hits2.Length);
							searcher2.Close();
							if (expectedCount == 0)
							{
								expectedCount = 16;
							}
							else if (expectedCount == 16)
							{
								expectedCount = 17;
							}
							else if (expectedCount == 17)
							{
								expectedCount = 0;
							}
						}
						reader.Close();
						if (i == N)
						{
							Assert.Fail("should have failed on commits before last " + N);
						}
					}
					catch (System.IO.IOException e)
					{
						if (i != N)
						{
							throw e;
						}
					}
					if (i < N)
					{
						dir.DeleteFile(IndexFileNames.FileNameFromGeneration(IndexFileNames.SEGMENTS, "", gen));
					}
					gen--;
				}
				
				dir.Close();
			}
		}
		
		private void  AddDoc(IndexWriter writer)
		{
			Document doc = new Document();
			doc.Add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
			writer.AddDocument(doc);
		}
	}
}