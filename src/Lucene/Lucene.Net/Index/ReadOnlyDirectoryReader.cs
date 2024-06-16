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
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Index
{
	
	public class ReadOnlyDirectoryReader:DirectoryReader
	{
		internal ReadOnlyDirectoryReader(Directory directory, SegmentInfos sis, IndexDeletionPolicy deletionPolicy, int termInfosIndexDivisor, IState state) :base(directory, sis, deletionPolicy, true, termInfosIndexDivisor, state)
		{
		}

        internal ReadOnlyDirectoryReader(Directory directory, SegmentInfos infos, SegmentReader[] oldReaders, int[] oldStarts, System.Collections.Generic.IDictionary<string, byte[]> oldNormsCache, bool doClone, int termInfosIndexDivisor, IState state)
            : base(directory, infos, oldReaders, oldStarts, oldNormsCache, true, doClone, termInfosIndexDivisor, state)
        {
        }

	    internal ReadOnlyDirectoryReader(IndexWriter writer, SegmentInfos infos, int termInfosIndexDivisor, IState state) :base(writer, infos, termInfosIndexDivisor, state)
		{
		}
		
		protected internal override void  AcquireWriteLock(IState state)
		{
			ReadOnlySegmentReader.NoWrite();
		}
	}
}