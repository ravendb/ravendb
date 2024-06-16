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
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Index
{
	
	/// <summary> <p/>Expert: represents a single commit into an index as seen by the
	/// <see cref="IndexDeletionPolicy" /> or <see cref="IndexReader" />.<p/>
	/// 
	/// <p/> Changes to the content of an index are made visible
	/// only after the writer who made that change commits by
	/// writing a new segments file
	/// (<c>segments_N</c>). This point in time, when the
	/// action of writing of a new segments file to the directory
	/// is completed, is an index commit.<p/>
	/// 
	/// <p/>Each index commit point has a unique segments file
	/// associated with it. The segments file associated with a
	/// later index commit point would have a larger N.<p/>
	/// 
	/// <p/><b>WARNING</b>: This API is a new and experimental and
	/// may suddenly change. <p/>
	/// </summary>
	
	public abstract class IndexCommit
	{
	    /// <summary> Get the segments file (<c>segments_N</c>) associated 
	    /// with this commit point.
	    /// </summary>
	    public abstract string SegmentsFileName { get; }

	    /// <summary> Returns all index files referenced by this commit point.</summary>
	    public abstract ICollection<string> FileNames { get; }

	    /// <summary> Returns the <see cref="Store.Directory" /> for the index.</summary>
	    public abstract Directory Directory { get; }

	    /// <summary> Delete this commit point.  This only applies when using
		/// the commit point in the context of IndexWriter's
		/// IndexDeletionPolicy.
		/// <p/>
		/// Upon calling this, the writer is notified that this commit 
		/// point should be deleted. 
		/// <p/>
		/// Decision that a commit-point should be deleted is taken by the <see cref="IndexDeletionPolicy" /> in effect
        /// and therefore this should only be called by its <see cref="IndexDeletionPolicy.OnInit{T}(IList{T})" /> or 
        /// <see cref="IndexDeletionPolicy.OnCommit{T}(IList{T})" /> methods.
		/// </summary>
        public abstract void Delete();

	    public abstract bool IsDeleted { get; }

	    /// <summary> Returns true if this commit is an optimized index.</summary>
	    public abstract bool IsOptimized { get; }

	    /// <summary> Two IndexCommits are equal if both their Directory and versions are equal.</summary>
		public  override bool Equals(System.Object other)
		{
			if (other is IndexCommit)
			{
				IndexCommit otherCommit = (IndexCommit) other;
				return otherCommit.Directory.Equals(Directory) && otherCommit.Version == Version;
			}
			else
				return false;
		}
		
		public override int GetHashCode()
		{
			return (int)(Directory.GetHashCode() + Version);
		}

	    /// <summary>Returns the version for this IndexCommit.  This is the
	    /// same value that <see cref="IndexReader.Version" /> would
	    /// return if it were opened on this commit. 
	    /// </summary>
	    public abstract long Version { get; }

	    /// <summary>Returns the generation (the _N in segments_N) for this
	    /// IndexCommit 
	    /// </summary>
	    public abstract long Generation { get; }

	    /// <summary>Convenience method that returns the last modified time
	    /// of the segments_N file corresponding to this index
	    /// commit, equivalent to
	    /// getDirectory().fileModified(getSegmentsFileName()). 
	    /// </summary>
	    public virtual long Timestamp(IState state)
	    {
	        return Directory.FileModified(SegmentsFileName, state);
	    }

	    /// <summary>Returns userData, previously passed to 
	    /// <see cref="IndexWriter.Commit(System.Collections.Generic.IDictionary{string, string})" />
	    /// for this commit.  IDictionary is String -> String. 
	    /// </summary>
	    public abstract IDictionary<string, string> UserData { get; }
	}
}