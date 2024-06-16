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
using Lucene.Net.Util;

namespace Lucene.Net.Index
{
	
	sealed class SegmentMergeQueue : PriorityQueue<SegmentMergeInfo>, IDisposable
	{
		internal SegmentMergeQueue(int size)
		{
			Initialize(size);
		}

        public override bool LessThan(SegmentMergeInfo stiA, SegmentMergeInfo stiB)
		{
			int comparison = stiA.term.CompareTo(stiB.term);
			if (comparison == 0)
				return stiA.base_Renamed < stiB.base_Renamed;
			else
				return comparison < 0;
		}

	    public void Dispose()
	    {
            // Move to protected method if class becomes unsealed
            while (Top() != null)
                Pop().Dispose();
	    }
	}
}