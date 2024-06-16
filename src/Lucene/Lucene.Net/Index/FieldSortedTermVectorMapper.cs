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

using System.Collections.Generic;
using Lucene.Net.Support;

namespace Lucene.Net.Index
{
	
	/// <summary> For each Field, store a sorted collection of <see cref="TermVectorEntry" />s
	/// <p/>
	/// This is not thread-safe.
	/// </summary>
	public class FieldSortedTermVectorMapper:TermVectorMapper
	{
        private readonly HashMap<string, SortedSet<TermVectorEntry>> fieldToTerms = new HashMap<string, SortedSet<TermVectorEntry>>();
		private SortedSet<TermVectorEntry> currentSet;
		private System.String currentField;
        private readonly IComparer<TermVectorEntry> comparator;
		
		/// <summary> </summary>
		/// <param name="comparator">A Comparator for sorting <see cref="TermVectorEntry" />s
		/// </param>
        public FieldSortedTermVectorMapper(IComparer<TermVectorEntry> comparator)
            : this(false, false, comparator)
		{
		}


        public FieldSortedTermVectorMapper(bool ignoringPositions, bool ignoringOffsets, IComparer<TermVectorEntry> comparator)
            : base(ignoringPositions, ignoringOffsets)
		{
			this.comparator = comparator;
		}
		
		public override void  Map(System.String term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions)
		{
			var entry = new TermVectorEntry(currentField, term, frequency, offsets, positions);
			currentSet.Add(entry);
		}
		
		public override void  SetExpectations(System.String field, int numTerms, bool storeOffsets, bool storePositions)
		{
			currentSet = new SortedSet<TermVectorEntry>(comparator);
			currentField = field;
			fieldToTerms[field] = currentSet;
		}

	    /// <summary> Get the mapping between fields and terms, sorted by the comparator
	    /// 
	    /// </summary>
	    /// <value> A map between field names and &lt;see cref=&quot;System.Collections.Generic.SortedDictionary{Object,Object}&quot; /&gt;s per field. SortedSet entries are &lt;see cref=&quot;TermVectorEntry&quot; /&gt; </value>
	    public virtual IDictionary<string, SortedSet<TermVectorEntry>> FieldToTerms
	    {
	        get { return fieldToTerms; }
	    }


	    public virtual IComparer<TermVectorEntry> Comparator
	    {
	        get { return comparator; }
	    }
	}
}