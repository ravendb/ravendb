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
using Lucene.Net.Support;

namespace Lucene.Net.Search
{


    /// <summary> Encapsulates sort criteria for returned hits.
    /// 
    /// <p/>The fields used to determine sort order must be carefully chosen.
    /// Documents must contain a single term in such a field,
    /// and the value of the term should indicate the document's relative position in
    /// a given sort order.  The field must be indexed, but should not be tokenized,
    /// and does not need to be stored (unless you happen to want it back with the
    /// rest of your document data).  In other words:
    /// 
    /// <p/><c>document.add (new Field ("byNumber", Integer.toString(x), Field.Store.NO, Field.Index.NOT_ANALYZED));</c><p/>
    /// 
    /// 
    /// <p/><h3>Valid Types of Values</h3>
    /// 
    /// <p/>There are four possible kinds of term values which may be put into
    /// sorting fields: Integers, Longs, Floats, or Strings.  Unless
    /// <see cref="SortField">SortField</see> objects are specified, the type of value
    /// in the field is determined by parsing the first term in the field.
    /// 
    /// <p/>Integer term values should contain only digits and an optional
    /// preceding negative sign.  Values must be base 10 and in the range
    /// <c>Integer.MIN_VALUE</c> and <c>Integer.MAX_VALUE</c> inclusive.
    /// Documents which should appear first in the sort
    /// should have low value integers, later documents high values
    /// (i.e. the documents should be numbered <c>1..n</c> where
    /// <c>1</c> is the first and <c>n</c> the last).
    /// 
    /// <p/>Long term values should contain only digits and an optional
    /// preceding negative sign.  Values must be base 10 and in the range
    /// <c>Long.MIN_VALUE</c> and <c>Long.MAX_VALUE</c> inclusive.
    /// Documents which should appear first in the sort
    /// should have low value integers, later documents high values.
    /// 
    /// <p/>Float term values should conform to values accepted by
    /// <see cref="float.Parse(string)" /> (except that <c>NaN</c>
    /// and <c>Infinity</c> are not supported).
    /// Documents which should appear first in the sort
    /// should have low values, later documents high values.
    /// 
    /// <p/>String term values can contain any valid String, but should
    /// not be tokenized.  The values are sorted according to their
    /// <see cref="IComparable">natural order</see>.  Note that using this type
    /// of term value has higher memory requirements than the other
    /// two types.
    /// 
    /// <p/><h3>Object Reuse</h3>
    /// 
    /// <p/>One of these objects can be
    /// used multiple times and the sort order changed between usages.
    /// 
    /// <p/>This class is thread safe.
    /// 
    /// <p/><h3>Memory Usage</h3>
    /// 
    /// <p/>Sorting uses of caches of term values maintained by the
    /// internal HitQueue(s).  The cache is static and contains an integer
    /// or float array of length <c>IndexReader.MaxDoc</c> for each field
    /// name for which a sort is performed.  In other words, the size of the
    /// cache in bytes is:
    /// 
    /// <p/><c>4 * IndexReader.MaxDoc * (# of different fields actually used to sort)</c>
    /// 
    /// <p/>For String fields, the cache is larger: in addition to the
    /// above array, the value of every term in the field is kept in memory.
    /// If there are many unique terms in the field, this could
    /// be quite large.
    /// 
    /// <p/>Note that the size of the cache is not affected by how many
    /// fields are in the index and <i>might</i> be used to sort - only by
    /// the ones actually used to sort a result set.
    /// 
    /// <p/>Created: Feb 12, 2004 10:53:57 AM
    /// 
    /// </summary>

        [Serializable]
    public class Sort
	{
		
		/// <summary> Represents sorting by computed relevance. Using this sort criteria returns
		/// the same results as calling
		/// <see cref="Searcher.Search(Query,int)" />Searcher#search()without a sort criteria,
		/// only with slightly more overhead.
		/// </summary>
		public static readonly Sort RELEVANCE = new Sort();
		
		/// <summary>Represents sorting by index order. </summary>
		public static readonly Sort INDEXORDER;
		
		// internal representation of the sort criteria
		internal ArraySegment<SortField> fields;
		
		/// <summary> Sorts by computed relevance. This is the same sort criteria as calling
		/// <see cref="Searcher.Search(Query,int)" />without a sort criteria,
		/// only with slightly more overhead.
		/// </summary>
		public Sort():this(SortField.FIELD_SCORE)
		{
		}
		
		/// <summary>Sorts by the criteria in the given SortField. </summary>
		public Sort(SortField field)
		{
			SetSort(field);
		}
		
		/// <summary>Sorts in succession by the criteria in each SortField. </summary>
		public Sort(params SortField[] fields)
		{
			SetSort(fields);
		}

        /// <summary>Sorts in succession by the criteria in each SortField. </summary>
        public Sort(ArraySegment<SortField> fields)
        {
            SetSort(fields);
        }

        /// <summary>Sets the sort to the given criteria. </summary>
        public virtual void  SetSort(SortField field)
		{
			this.fields = new ArraySegment<SortField>(new SortField[]{field});
		}
		
		/// <summary>Sets the sort to the given criteria in succession. </summary>
		public virtual void  SetSort(params SortField[] fields)
		{
			this.fields = new ArraySegment<SortField>(fields);
		}

        /// <summary>Sets the sort to the given criteria in succession. </summary>
        public virtual void SetSort(ArraySegment<SortField> fields)
        {
            this.fields = fields;
        }

        /// <summary> Representation of the sort criteria.</summary>
        /// <returns> Array of SortField objects used in this sort criteria
        /// </returns>
        public virtual ArraySegment<SortField> GetSort()
		{
			return fields;
		}
		
		public override System.String ToString()
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			
            for (int i = 0; i < fields.Count; i++)
			{
				buffer.Append(fields.Array[i + fields.Offset].ToString());
				if ((i + 1) < fields.Count)
					buffer.Append(',');
			}
			
			return buffer.ToString();
		}
		
		/// <summary>Returns true if <c>o</c> is equal to this. </summary>
		public  override bool Equals(System.Object o)
		{
			if (this == o)
				return true;
			if (!(o is Sort))
				return false;
			Sort other = (Sort) o;

            bool result = false;
            if ((this.fields == null) && (other.fields == null))
                result = true;
            else if ((this.fields != null) && (other.fields != null))
            {
                if (this.fields.Count == other.fields.Count)
                {
                    int length = this.fields.Count;
                    result = true;
                    for (int i = 0; i < length; i++)
                    {
                        if (!(this.fields.Array[i + this.fields.Offset].Equals(other.fields.Array[i + other.fields.Offset])))
                        {
                            result = false;
                            break;
                        }
                    }
                }
            }
            return result;
		}
		
		/// <summary>Returns a hash code value for this object. </summary>
		public override int GetHashCode()
		{
			// TODO in Java 1.5: switch to Arrays.hashCode().  The 
			// Java 1.4 workaround below calculates the same hashCode
			// as Java 1.5's new Arrays.hashCode()
			return 0x45aaf665 + EquatableList<SortField>.GetHashCode(fields);
		}
		static Sort()
		{
			INDEXORDER = new Sort(SortField.FIELD_DOC);
		}
	}
}