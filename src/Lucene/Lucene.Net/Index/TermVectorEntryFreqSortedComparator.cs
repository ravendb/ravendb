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

namespace Lucene.Net.Index
{
	
	/// <summary> Compares <see cref="Lucene.Net.Index.TermVectorEntry" />s first by frequency and then by
	/// the term (case-sensitive)
	/// 
	/// 
	/// </summary>
    public class TermVectorEntryFreqSortedComparator : System.Collections.Generic.IComparer<TermVectorEntry>
	{
        public virtual int Compare(TermVectorEntry entry, TermVectorEntry entry1)
		{
			int result = 0;
			result = entry1.Frequency - entry.Frequency;
			if (result == 0)
			{
				result = String.CompareOrdinal(entry.Term, entry1.Term);
				if (result == 0)
				{
					result = String.CompareOrdinal(entry.Field, entry1.Field);
				}
			}
			return result;
		}
	}
}