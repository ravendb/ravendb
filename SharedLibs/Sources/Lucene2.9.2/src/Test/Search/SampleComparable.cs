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

using IndexReader = Lucene.Net.Index.IndexReader;
using Term = Lucene.Net.Index.Term;
using TermDocs = Lucene.Net.Index.TermDocs;
using TermEnum = Lucene.Net.Index.TermEnum;
using StringHelper = Lucene.Net.Util.StringHelper;

namespace Lucene.Net.Search
{
	
	/// <summary> An example Comparable for use with the custom sort tests.
	/// It implements a comparable for "id" sort of values which
	/// consist of an alphanumeric part and a numeric part, such as:
	/// <p/>
	/// ABC-123, A-1, A-7, A-100, B-99999
	/// <p/>
	/// Such values cannot be sorted as strings, since A-100 needs
	/// to come after A-7.
	/// <p/>
	/// <p/>It could be argued that the "ids" should be rewritten as
	/// A-0001, A-0100, etc. so they will sort as strings.  That is
	/// a valid alternate way to solve it - but
	/// this is only supposed to be a simple test case.
	/// <p/>
	/// Created: Apr 21, 2004 5:34:47 PM
	/// 
	/// 
	/// </summary>
	/// <version>  $Id: SampleComparable.java 801344 2009-08-05 18:05:06Z yonik $
	/// </version>
	/// <since> 1.4
	/// </since>
	[Serializable]
	public class SampleComparable : System.IComparable
	{
		[Serializable]
		private class AnonymousClassSortComparatorSource : SortComparatorSource
		{
			private class AnonymousClassScoreDocComparator : ScoreDocComparator
			{
				public AnonymousClassScoreDocComparator(Lucene.Net.Index.IndexReader reader, Lucene.Net.Index.TermEnum enumerator, System.String field, AnonymousClassSortComparatorSource enclosingInstance)
				{
					InitBlock(reader, enumerator, field, enclosingInstance);
				}
				private void  InitBlock(Lucene.Net.Index.IndexReader reader, Lucene.Net.Index.TermEnum enumerator, System.String field, AnonymousClassSortComparatorSource enclosingInstance)
				{
					this.reader = reader;
					this.enumerator = enumerator;
					this.field = field;
					this.enclosingInstance = enclosingInstance;
					cachedValues = Enclosing_Instance.fillCache(reader, enumerator, field);
				}
				private Lucene.Net.Index.IndexReader reader;
				private Lucene.Net.Index.TermEnum enumerator;
				private System.String field;
				private AnonymousClassSortComparatorSource enclosingInstance;
				public AnonymousClassSortComparatorSource Enclosing_Instance
				{
					get
					{
						return enclosingInstance;
					}
					
				}
				protected internal System.IComparable[] cachedValues;
				
				public virtual int Compare(ScoreDoc i, ScoreDoc j)
				{
					return cachedValues[i.doc].CompareTo(cachedValues[j.doc]);
				}
				
				public virtual System.IComparable SortValue(ScoreDoc i)
				{
					return cachedValues[i.doc];
				}
				
				public virtual int SortType()
				{
					return SortField.CUSTOM;
				}
			}
			public virtual ScoreDocComparator NewComparator(IndexReader reader, System.String fieldname)
			{
				System.String field = StringHelper.Intern(fieldname);
				TermEnum enumerator = reader.Terms(new Term(fieldname, ""));
				try
				{
					return new AnonymousClassScoreDocComparator(reader, enumerator, field, this);
				}
				finally
				{
					enumerator.Close();
				}
			}
			
			/// <summary> Returns an array of objects which represent that natural order
			/// of the term values in the given field.
			/// 
			/// </summary>
			/// <param name="reader">    Terms are in this index.
			/// </param>
			/// <param name="enumerator">Use this to get the term values and TermDocs.
			/// </param>
			/// <param name="fieldname"> Comparables should be for this field.
			/// </param>
			/// <returns> Array of objects representing natural order of terms in field.
			/// </returns>
			/// <throws>  IOException If an error occurs reading the index. </throws>
			protected internal virtual System.IComparable[] fillCache(IndexReader reader, TermEnum enumerator, System.String fieldname)
			{
				System.String field = StringHelper.Intern(fieldname);
				System.IComparable[] retArray = new System.IComparable[reader.MaxDoc()];
				if (retArray.Length > 0)
				{
					TermDocs termDocs = reader.TermDocs();
					try
					{
						if (enumerator.Term() == null)
						{
							throw new System.SystemException("no terms in field " + field);
						}
						do 
						{
							Term term = enumerator.Term();
							if ((System.Object) term.Field() != (System.Object) field)
								break;
							System.IComparable termval = GetComparable(term.Text());
							termDocs.Seek(enumerator);
							while (termDocs.Next())
							{
								retArray[termDocs.Doc()] = termval;
							}
						}
						while (enumerator.Next());
					}
					finally
					{
						termDocs.Close();
					}
				}
				return retArray;
			}
			
			internal virtual System.IComparable GetComparable(System.String termtext)
			{
				return new SampleComparable(termtext);
			}
		}
		
		internal System.String string_part;
		internal System.Int32 int_part;
		
		public SampleComparable(System.String s)
		{
			int i = s.IndexOf("-");
			string_part = s.Substring(0, (i) - (0));
			int_part = System.Int32.Parse(s.Substring(i + 1));
		}
		
		public virtual int CompareTo(System.Object o)
		{
			SampleComparable otherid = (SampleComparable) o;
			int i = String.CompareOrdinal(string_part, otherid.string_part);
			if (i == 0)
			{
				return int_part.CompareTo(otherid.int_part);
			}
			return i;
		}
		
		public static SortComparatorSource GetComparatorSource()
		{
			return new AnonymousClassSortComparatorSource();
		}
		
		[Serializable]
		private sealed class InnerSortComparator:SortComparator
		{
			public /*protected internal*/ override System.IComparable GetComparable(System.String termtext)
			{
				return new SampleComparable(termtext);
			}
			public override int GetHashCode()
			{
				return this.GetType().FullName.GetHashCode();
			}
			public  override bool Equals(System.Object that)
			{
				return this.GetType().Equals(that.GetType());
			}
		}
		
		
		public static SortComparator GetComparator()
		{
			return new InnerSortComparator();
		}
	}
}