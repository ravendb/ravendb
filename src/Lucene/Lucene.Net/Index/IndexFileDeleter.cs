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
using System.Collections.Generic;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Index
{

    /// <summary>
    /// <para>This class keeps track of each SegmentInfos instance that
    /// is still "live", either because it corresponds to a
    /// segments_N file in the Directory (a "commit", i.e. a
    /// committed SegmentInfos) or because it's an in-memory
    /// SegmentInfos that a writer is actively updating but has
    /// not yet committed.  This class uses simple reference
    /// counting to map the live SegmentInfos instances to
    /// individual files in the Directory.</para>
    /// 
    /// <para>The same directory file may be referenced by more than
    /// one IndexCommit, i.e. more than one SegmentInfos.
    /// Therefore we count how many commits reference each file.
    /// When all the commits referencing a certain file have been
    /// deleted, the refcount for that file becomes zero, and the
    /// file is deleted.</para>
    ///
    /// <para>A separate deletion policy interface
    /// (IndexDeletionPolicy) is consulted on creation (onInit)
    /// and once per commit (onCommit), to decide when a commit
    /// should be removed.</para>
    /// 
    /// <para>It is the business of the IndexDeletionPolicy to choose
    /// when to delete commit points.  The actual mechanics of
    /// file deletion, retrying, etc, derived from the deletion
    /// of commit points is the business of the IndexFileDeleter.</para>
    /// 
    /// <para>The current default deletion policy is
    /// <see cref="KeepOnlyLastCommitDeletionPolicy"/>, which removes all
    /// prior commits when a new commit has completed.  This
    /// matches the behavior before 2.2.</para>
    ///
    /// <para>Note that you must hold the write.lock before
    /// instantiating this class.  It opens segments_N file(s)
    /// directly with no retry logic.</para>
    /// </summary>
	
	public sealed class IndexFileDeleter : IDisposable
	{
		
		//// Files that we tried to delete but failed (likely
		/// because they are open and we are running on Windows),
		/// so we will retry them again later: ////
		private IList<string> deletable;
		
		//// Reference count for all files in the index.  
		/// Counts how many existing commits reference a file.
		/// Maps String to RefCount (class below) instances: ////
		private HashMap<string, RefCount> refCounts = new HashMap<string, RefCount>();
		
		//// Holds all commits (segments_N) currently in the index.
		/// This will have just 1 commit if you are using the
		/// default delete policy (KeepOnlyLastCommitDeletionPolicy).
		/// Other policies may leave commit points live for longer
		/// in which case this list would be longer than 1: ////
        private List<CommitPoint> commits = new List<CommitPoint>();
		
		//// Holds files we had incref'd from the previous
		/// non-commit checkpoint: ////
        private List<ICollection<string>> lastFiles = new List<ICollection<string>>();
		
		//// Commits that the IndexDeletionPolicy have decided to delete: ////
        private List<CommitPoint> commitsToDelete = new List<CommitPoint>();
		
		private System.IO.StreamWriter infoStream;
		private Directory directory;
		private IndexDeletionPolicy policy;
		private DocumentsWriter docWriter;
		
		internal bool startingCommitDeleted;
        private SegmentInfos lastSegmentInfos;

        private HashSet<string> synced;
		
		/// <summary>Change to true to see details of reference counts when
		/// infoStream != null 
		/// </summary>
		public static bool VERBOSE_REF_COUNTS = false;
		
		internal void  SetInfoStream(System.IO.StreamWriter infoStream)
		{
			this.infoStream = infoStream;
			if (infoStream != null)
			{
				Message("setInfoStream deletionPolicy=" + policy);
			}
		}
		
		private void  Message(System.String message)
		{
            infoStream.WriteLine("IFD [" + new DateTime().ToString() + "; " + ThreadClass.Current().Name + "]: " + message);
		}
		
		/// <summary> Initialize the deleter: find all previous commits in
		/// the Directory, incref the files they reference, call
		/// the policy to let it delete commits.  This will remove
		/// any files not referenced by any of the commits.
		/// </summary>
		/// <throws>  CorruptIndexException if the index is corrupt </throws>
		/// <throws>  IOException if there is a low-level IO error </throws>
        public IndexFileDeleter(Directory directory, IndexDeletionPolicy policy, SegmentInfos segmentInfos, System.IO.StreamWriter infoStream, DocumentsWriter docWriter, HashSet<string> synced, IState state)
		{
			
			this.docWriter = docWriter;
			this.infoStream = infoStream;
            this.synced = synced;
			
			if (infoStream != null)
			{
				Message("init: current segments file is \"" + segmentInfos.GetCurrentSegmentFileName() + "\"; deletionPolicy=" + policy);
			}
			
			this.policy = policy;
			this.directory = directory;
			
			// First pass: walk the files and initialize our ref
			// counts:
			long currentGen = segmentInfos.Generation;
			IndexFileNameFilter filter = IndexFileNameFilter.Filter;
			
			System.String[] files = directory.ListAll(state);
			
			CommitPoint currentCommitPoint = null;
			
			for (int i = 0; i < files.Length; i++)
			{
				
				System.String fileName = files[i];
				
				if (filter.Accept(null, fileName) && !fileName.Equals(IndexFileNames.SEGMENTS_GEN))
				{
					
					// Add this file to refCounts with initial count 0:
					GetRefCount(fileName);
					
					if (fileName.StartsWith(IndexFileNames.SEGMENTS))
					{
						
						// This is a commit (segments or segments_N), and
						// it's valid (<= the max gen).  Load it, then
						// incref all files it refers to:
                        if (infoStream != null)
                        {
                            Message("init: load commit \"" + fileName + "\"");
                        }
                        SegmentInfos sis = new SegmentInfos();
                        try
                        {
                            sis.Read(directory, fileName, state);
                        }
                        catch (System.IO.FileNotFoundException)
                        {
                            // LUCENE-948: on NFS (and maybe others), if
                            // you have writers switching back and forth
                            // between machines, it's very likely that the
                            // dir listing will be stale and will claim a
                            // file segments_X exists when in fact it
                            // doesn't.  So, we catch this and handle it
                            // as if the file does not exist
                            if (infoStream != null)
                            {
                                Message("init: hit FileNotFoundException when loading commit \"" + fileName + "\"; skipping this commit point");
                            }
                            sis = null;
                        }
                        catch (System.IO.IOException)
                        {
                            if (SegmentInfos.GenerationFromSegmentsFileName(fileName) <= currentGen)
                            {
                                throw;
                            }
                            else
                            {
                                // Most likely we are opening an index that
                                // has an aborted "future" commit, so suppress
                                // exc in this case
                                sis = null;
                            }
                        }
                        if (sis != null)
                        {
                            CommitPoint commitPoint = new CommitPoint(this, commitsToDelete, directory, sis, state);
                            if (sis.Generation == segmentInfos.Generation)
                            {
                                currentCommitPoint = commitPoint;
                            }
                            commits.Add(commitPoint);
                            IncRef(sis, true, state);

                            if (lastSegmentInfos == null || sis.Generation > lastSegmentInfos.Generation)
                            {
                                lastSegmentInfos = sis;
                            }
						}
					}
				}
			}
			
			if (currentCommitPoint == null)
			{
				// We did not in fact see the segments_N file
				// corresponding to the segmentInfos that was passed
				// in.  Yet, it must exist, because our caller holds
				// the write lock.  This can happen when the directory
				// listing was stale (eg when index accessed via NFS
				// client with stale directory listing cache).  So we
				// try now to explicitly open this commit point:
				SegmentInfos sis = new SegmentInfos();
				try
				{
					sis.Read(directory, segmentInfos.GetCurrentSegmentFileName(), state);
				}
				catch (System.IO.IOException)
				{
					throw new CorruptIndexException("failed to locate current segments_N file");
				}
				if (infoStream != null)
					Message("forced open of current segments file " + segmentInfos.GetCurrentSegmentFileName());
				currentCommitPoint = new CommitPoint(this, commitsToDelete, directory, sis, state);
				commits.Add(currentCommitPoint);
				IncRef(sis, true, state);
			}
			
			// We keep commits list in sorted order (oldest to newest):
			commits.Sort();
			
			// Now delete anything with ref count at 0.  These are
			// presumably abandoned files eg due to crash of
			// IndexWriter.
			foreach(KeyValuePair<string, RefCount> entry in refCounts)
			{
                string fileName = entry.Key;
				RefCount rc = refCounts[fileName];
				if (0 == rc.count)
				{
					if (infoStream != null)
					{
						Message("init: removing unreferenced file \"" + fileName + "\"");
					}
					DeleteFile(fileName, state);
				}
			}
			
			// Finally, give policy a chance to remove things on
			// startup:
			policy.OnInit(commits);
			
			// Always protect the incoming segmentInfos since
			// sometime it may not be the most recent commit
			Checkpoint(segmentInfos, false, state);
			
			startingCommitDeleted = currentCommitPoint.IsDeleted;
			
			DeleteCommits(state);
		}

        public SegmentInfos LastSegmentInfos
        {
            get { return lastSegmentInfos; }
        }

        /// <summary> Remove the CommitPoints in the commitsToDelete List by
		/// DecRef'ing all files from each SegmentInfos.
		/// </summary>
		private void  DeleteCommits(IState state)
		{
			
			int size = commitsToDelete.Count;
			
			if (size > 0)
			{
				
				// First decref all files that had been referred to by
				// the now-deleted commits:
				for (int i = 0; i < size; i++)
				{
					CommitPoint commit = commitsToDelete[i];
					if (infoStream != null)
					{
						Message("deleteCommits: now decRef commit \"" + commit.SegmentsFileName + "\"");
					}
					foreach(string file in commit.files)
					{
						DecRef(file, state);
					}
				}
				commitsToDelete.Clear();
				
				// Now compact commits to remove deleted ones (preserving the sort):
				size = commits.Count;
				int readFrom = 0;
				int writeTo = 0;
				while (readFrom < size)
				{
					CommitPoint commit = commits[readFrom];
					if (!commit.deleted)
					{
						if (writeTo != readFrom)
						{
							commits[writeTo] = commits[readFrom];
						}
						writeTo++;
					}
					readFrom++;
				}
				
				while (size > writeTo)
				{
					commits.RemoveAt(size - 1);
					size--;
				}
			}
		}
		
		/// <summary> Writer calls this when it has hit an error and had to
		/// roll back, to tell us that there may now be
		/// unreferenced files in the filesystem.  So we re-list
		/// the filesystem and delete such files.  If segmentName
		/// is non-null, we will only delete files corresponding to
		/// that segment.
		/// </summary>
		public void  Refresh(System.String segmentName, IState state)
		{
			System.String[] files = directory.ListAll(state);
			IndexFileNameFilter filter = IndexFileNameFilter.Filter;
			System.String segmentPrefix1;
			System.String segmentPrefix2;
			if (segmentName != null)
			{
				segmentPrefix1 = segmentName + ".";
				segmentPrefix2 = segmentName + "_";
			}
			else
			{
				segmentPrefix1 = null;
				segmentPrefix2 = null;
			}
			
			for (int i = 0; i < files.Length; i++)
			{
				System.String fileName = files[i];
				if (filter.Accept(null, fileName) && (segmentName == null || fileName.StartsWith(segmentPrefix1) || fileName.StartsWith(segmentPrefix2)) && !refCounts.ContainsKey(fileName) && !fileName.Equals(IndexFileNames.SEGMENTS_GEN))
				{
					// Unreferenced file, so remove it
					if (infoStream != null)
					{
						Message("refresh [prefix=" + segmentName + "]: removing newly created unreferenced file \"" + fileName + "\"");
					}
					DeleteFile(fileName, state);
				}
			}
		}
		
		public void  Refresh(IState state)
		{
			Refresh(null, state);
		}
		
		public void Dispose()
		{
		    var state = StateHolder.Current.Value;

            // Move to protected method if class becomes unsealed
			// DecRef old files from the last checkpoint, if any:
			int size = lastFiles.Count;
			if (size > 0)
			{
				for (int i = 0; i < size; i++)
					DecRef(lastFiles[i], state);
				lastFiles.Clear();
			}
			
			DeletePendingFiles(state);
		}
		
		private void  DeletePendingFiles(IState state)
		{
			if (deletable != null)
			{
				IList<string> oldDeletable = deletable;
				deletable = null;
				int size = oldDeletable.Count;
				for (int i = 0; i < size; i++)
				{
					if (infoStream != null)
					{
						Message("delete pending file " + oldDeletable[i]);
					}
					DeleteFile(oldDeletable[i], state);
				}
			}
		}
		
		/// <summary> For definition of "check point" see IndexWriter comments:
		/// "Clarification: Check Points (and commits)".
		/// 
		/// Writer calls this when it has made a "consistent
		/// change" to the index, meaning new files are written to
		/// the index and the in-memory SegmentInfos have been
		/// modified to point to those files.
		/// 
		/// This may or may not be a commit (segments_N may or may
		/// not have been written).
		/// 
		/// We simply incref the files referenced by the new
		/// SegmentInfos and decref the files we had previously
		/// seen (if any).
		/// 
		/// If this is a commit, we also call the policy to give it
		/// a chance to remove other commits.  If any commits are
		/// removed, we decref their files as well.
		/// </summary>
		public void  Checkpoint(SegmentInfos segmentInfos, bool isCommit, IState state)
		{
			
			if (infoStream != null)
			{
				Message("now checkpoint \"" + segmentInfos.GetCurrentSegmentFileName() + "\" [" + segmentInfos.Count + " segments " + "; isCommit = " + isCommit + "]");
			}
			
			// Try again now to delete any previously un-deletable
			// files (because they were in use, on Windows):
			DeletePendingFiles(state);
			
			// Incref the files:
			IncRef(segmentInfos, isCommit, state);
			
			if (isCommit)
			{
				// Append to our commits list:
				commits.Add(new CommitPoint(this, commitsToDelete, directory, segmentInfos, state));
				
				// Tell policy so it can remove commits:
				policy.OnCommit(commits);
				
				// Decref files for commits that were deleted by the policy:
				DeleteCommits(state);
			}
			else
			{
				
				IList<string> docWriterFiles;
				if (docWriter != null)
				{
					docWriterFiles = docWriter.OpenFiles();
					if (docWriterFiles != null)
					// We must incRef these files before decRef'ing
					// last files to make sure we don't accidentally
					// delete them:
						IncRef(docWriterFiles);
				}
				else
					docWriterFiles = null;
				
				// DecRef old files from the last checkpoint, if any:
				int size = lastFiles.Count;
				if (size > 0)
				{
					for (int i = 0; i < size; i++)
						DecRef(lastFiles[i], state);
					lastFiles.Clear();
				}
				
				// Save files so we can decr on next checkpoint/commit:
                lastFiles.Add(segmentInfos.Files(directory, false, state));
				
                if (docWriterFiles != null)
                {
                    lastFiles.Add(docWriterFiles);
                }
			}
		}
		
		internal void  IncRef(SegmentInfos segmentInfos, bool isCommit, IState state)
		{
			// If this is a commit point, also incRef the
			// segments_N file:
			foreach(string fileName in segmentInfos.Files(directory, isCommit, state))
			{
				IncRef(fileName);
			}
		}

        internal void IncRef(ICollection<string> files)
		{
            foreach(string file in files)
			{
                IncRef(file);
			}
		}
		
		internal void  IncRef(string fileName)
		{
			RefCount rc = GetRefCount(fileName);
			if (infoStream != null && VERBOSE_REF_COUNTS)
			{
				Message("  IncRef \"" + fileName + "\": pre-incr count is " + rc.count);
			}
			rc.IncRef();
		}
		
		internal void  DecRef(ICollection<string> files, IState state)
		{
            foreach(string file in files)
            {
                DecRef(file, state);
            }
		}
		
		internal void  DecRef(System.String fileName, IState state)
		{
			RefCount rc = GetRefCount(fileName);
			if (infoStream != null && VERBOSE_REF_COUNTS)
			{
				Message("  DecRef \"" + fileName + "\": pre-decr count is " + rc.count);
			}
			if (0 == rc.DecRef())
			{
				// This file is no longer referenced by any past
				// commit points nor by the in-memory SegmentInfos:
				DeleteFile(fileName, state);
				refCounts.Remove(fileName);

                if (synced != null) {
                    lock(synced) 
                    {
                      synced.Remove(fileName);
                    }
                }
			}
		}
		
		internal void  DecRef(SegmentInfos segmentInfos, IState state)
		{
			foreach(string file in segmentInfos.Files(directory, false, state))
			{
				DecRef(file, state);
			}
		}

        public bool Exists(String fileName)
        {
            if (!refCounts.ContainsKey(fileName))
            {
                return false;
            }
            else
            {
                return GetRefCount(fileName).count > 0;
            }
        }
		
		private RefCount GetRefCount(System.String fileName)
		{
			RefCount rc;
			if (!refCounts.ContainsKey(fileName))
			{
				rc = new RefCount(fileName);
				refCounts[fileName] = rc;
			}
			else
			{
				rc = refCounts[fileName];
			}
			return rc;
		}
		
		internal void  DeleteFiles(System.Collections.Generic.IList<string> files, IState state)
		{
			foreach(string file in files)
				DeleteFile(file, state);
		}
		
		/// <summary>Deletes the specified files, but only if they are new
		/// (have not yet been incref'd). 
		/// </summary>
        internal void DeleteNewFiles(System.Collections.Generic.ICollection<string> files, IState state)
		{
			foreach(string fileName in files)
			{
                if (!refCounts.ContainsKey(fileName))
                {
                    if (infoStream != null)
                    {
                        Message("delete new file \"" + fileName + "\"");
                    }
                    DeleteFile(fileName, state);
                }
			}
		}
		
		internal void  DeleteFile(System.String fileName, IState state)
		{
			try
			{
				if (infoStream != null)
				{
					Message("delete \"" + fileName + "\"");
				}
				directory.DeleteFile(fileName, state);
			}
			catch (System.IO.IOException e)
			{
				// if delete fails
				if (directory.FileExists(fileName, state))
				{
					
					// Some operating systems (e.g. Windows) don't
					// permit a file to be deleted while it is opened
					// for read (e.g. by another process or thread). So
					// we assume that when a delete fails it is because
					// the file is open in another process, and queue
					// the file for subsequent deletion.
					
					if (infoStream != null)
					{
						Message("IndexFileDeleter: unable to remove file \"" + fileName + "\": " + e.ToString() + "; Will re-try later.");
					}
					if (deletable == null)
					{
                        deletable = new List<string>();
					}
					deletable.Add(fileName); // add to deletable
				}
			}
		}
		
		/// <summary> Tracks the reference count for a single index file:</summary>
		sealed private class RefCount
		{
			
			// fileName used only for better assert error messages
			internal System.String fileName;
			internal bool initDone;
			internal RefCount(System.String fileName)
			{
				this.fileName = fileName;
			}
			
			internal int count;
			
			public int IncRef()
			{
				if (!initDone)
				{
					initDone = true;
				}
				else
				{
					System.Diagnostics.Debug.Assert(count > 0, "RefCount is 0 pre-increment for file " + fileName);
				}
				return ++count;
			}
			
			public int DecRef()
			{
				System.Diagnostics.Debug.Assert(count > 0, "RefCount is 0 pre-decrement for file " + fileName);
				return --count;
			}
		}
		
		/// <summary> Holds details for each commit point.  This class is
		/// also passed to the deletion policy.  Note: this class
		/// has a natural ordering that is inconsistent with
		/// equals.
		/// </summary>
		
		sealed private class CommitPoint:IndexCommit, System.IComparable<CommitPoint>
		{
            private void InitBlock(IndexFileDeleter enclosingInstance)
            {
                this.enclosingInstance = enclosingInstance;
            }
            private IndexFileDeleter enclosingInstance;
            public IndexFileDeleter Enclosing_Instance
            {
                get
                {
                    return enclosingInstance;
                }

            }
			
			internal long gen;
            internal ICollection<string> files;
			internal string segmentsFileName;
			internal bool deleted;
			internal Directory directory;
            internal ICollection<CommitPoint> commitsToDelete;
			internal long version;
			internal long generation;
			internal bool isOptimized;
            internal IDictionary<string, string> userData;
			
			public CommitPoint(IndexFileDeleter enclosingInstance, ICollection<CommitPoint> commitsToDelete, Directory directory, SegmentInfos segmentInfos, IState state)
			{
				InitBlock(enclosingInstance);
				this.directory = directory;
				this.commitsToDelete = commitsToDelete;
				userData = segmentInfos.UserData;
				segmentsFileName = segmentInfos.GetCurrentSegmentFileName();
				version = segmentInfos.Version;
				generation = segmentInfos.Generation;
                files = segmentInfos.Files(directory, true, state);
				gen = segmentInfos.Generation;
				isOptimized = segmentInfos.Count == 1 && !segmentInfos.Info(0).HasDeletions(state);
				
				System.Diagnostics.Debug.Assert(!segmentInfos.HasExternalSegments(directory));
			}

            public override string ToString()
            {
                return "IndexFileDeleter.CommitPoint(" + segmentsFileName + ")";
            }

		    public override bool IsOptimized
		    {
		        get { return isOptimized; }
		    }

		    public override string SegmentsFileName
		    {
		        get { return segmentsFileName; }
		    }

		    public override ICollection<string> FileNames
		    {
		        get { return files; }
		    }

		    public override Directory Directory
		    {
		        get { return directory; }
		    }

		    public override long Version
		    {
		        get { return version; }
		    }

		    public override long Generation
		    {
		        get { return generation; }
		    }

		    public override IDictionary<string, string> UserData
		    {
		        get { return userData; }
		    }

		    /// <summary> Called only be the deletion policy, to remove this
			/// commit point from the index.
			/// </summary>
			public override void  Delete()
			{
				if (!deleted)
				{
					deleted = true;
					Enclosing_Instance.commitsToDelete.Add(this);
				}
			}

		    public override bool IsDeleted
		    {
		        get { return deleted; }
		    }

		    public int CompareTo(CommitPoint commit)
			{
				if (gen < commit.gen)
				{
					return - 1;
				}
				else if (gen > commit.gen)
				{
					return 1;
				}
				else
				{
					return 0;
				}
			}
		}
	}
}