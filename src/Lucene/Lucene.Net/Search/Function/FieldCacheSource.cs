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
using IndexReader = Lucene.Net.Index.IndexReader;
using FieldCache = Lucene.Net.Search.FieldCache;

namespace Lucene.Net.Search.Function
{

    /// <summary> Expert: A base class for ValueSource implementations that retrieve values for
    /// a single field from the <see cref="Lucene.Net.Search.FieldCache">FieldCache</see>.
    /// <p/>
    /// Fields used herein nust be indexed (doesn't matter if these fields are stored or not).
    /// <p/> 
    /// It is assumed that each such indexed field is untokenized, or at least has a single token in a document.
    /// For documents with multiple tokens of the same field, behavior is undefined (It is likely that current 
    /// code would use the value of one of these tokens, but this is not guaranteed).
    /// <p/>
    /// Document with no tokens in this field are assigned the <c>Zero</c> value.    
    /// 
    /// <p/><font color="#FF0000">
    /// WARNING: The status of the <b>Search.Function</b> package is experimental. 
    /// The APIs introduced here might change in the future and will not be 
    /// supported anymore in such a case.</font>
    /// 
    /// <p/><b>NOTE</b>: with the switch in 2.9 to segment-based
    /// searching, if <see cref="GetValues" /> is invoked with a
    /// composite (multi-segment) reader, this can easily cause
    /// double RAM usage for the values in the FieldCache.  It's
    /// best to switch your application to pass only atomic
    /// (single segment) readers to this API.<p/>
    /// </summary>
    [Serializable]
    public abstract class FieldCacheSource:ValueSource
	{
		private System.String field;
		
		/// <summary> Create a cached field source for the input field.  </summary>
		protected FieldCacheSource(System.String field)
		{
			this.field = field;
		}
		
		/* (non-Javadoc) <see cref="Lucene.Net.Search.Function.ValueSource.getValues(Lucene.Net.Index.IndexReader) */
		public override DocValues GetValues(IndexReader reader, IState state)
		{
			return GetCachedFieldValues(Lucene.Net.Search.FieldCache_Fields.DEFAULT, field, reader, state);
		}
		
		/* (non-Javadoc) <see cref="Lucene.Net.Search.Function.ValueSource.description() */
		public override System.String Description()
		{
			return field;
		}
		
		/// <summary> Return cached DocValues for input field and reader.</summary>
		/// <param name="cache">FieldCache so that values of a field are loaded once per reader (RAM allowing)
		/// </param>
		/// <param name="field">Field for which values are required.
		/// </param>
		/// <seealso cref="ValueSource">
		/// </seealso>
		public abstract DocValues GetCachedFieldValues(FieldCache cache, System.String field, IndexReader reader, IState state);
		
		/*(non-Javadoc) <see cref="java.lang.Object.equals(java.lang.Object) */
		public  override bool Equals(System.Object o)
		{
			if (!(o is FieldCacheSource))
			{
				return false;
			}
			FieldCacheSource other = (FieldCacheSource) o;
			return this.field.Equals(other.field) && CachedFieldSourceEquals(other);
		}
		
		/*(non-Javadoc) <see cref="java.lang.Object.hashCode() */
		public override int GetHashCode()
		{
			return field.GetHashCode() + CachedFieldSourceHashCode();
		}
		
		/// <summary> Check if equals to another <see cref="FieldCacheSource" />, already knowing that cache and field are equal.  </summary>
		/// <seealso cref="Object.Equals(Object)">
		/// </seealso>
		public abstract bool CachedFieldSourceEquals(FieldCacheSource other);
		
		/// <summary> Return a hash code of a <see cref="FieldCacheSource" />, without the hash-codes of the field 
		/// and the cache (those are taken care of elsewhere).  
		/// </summary>
		/// <seealso cref="Object.GetHashCode()">
		/// </seealso>
		public abstract int CachedFieldSourceHashCode();
	}
}