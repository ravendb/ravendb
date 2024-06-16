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
using Lucene.Net.Store;

namespace Lucene.Net.Index
{
	
	/// <summary><p/>Expert: <see cref="IndexWriter" /> uses an instance
	/// implementing this interface to execute the merges
	/// selected by a <see cref="MergePolicy" />.  The default
	/// MergeScheduler is <see cref="ConcurrentMergeScheduler" />.<p/>
	/// 
	/// <p/><b>NOTE:</b> This API is new and still experimental
	/// (subject to change suddenly in the next release)<p/>
	/// 
	/// <p/><b>NOTE</b>: This class typically requires access to
	/// package-private APIs (eg, SegmentInfos) to do its job;
	/// if you implement your own MergePolicy, you'll need to put
	/// it in package Lucene.Net.Index in order to use
	/// these APIs.
	/// </summary>
	
	public abstract class MergeScheduler : IDisposable
	{
		
		/// <summary>Run the merges provided by <see cref="IndexWriter.GetNextMerge()" />. </summary>
		public abstract void  Merge(IndexWriter writer, IState state);
		
        [Obsolete("Use Dispose() instead")]
		public void Close()
        {
            Dispose();
        }

        public void Dispose()
        {
            Dispose(true);
        }

	    /// <summary>Close this MergeScheduler. </summary>
	    protected abstract void Dispose(bool disposing);
	}
}