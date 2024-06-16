/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
#if NET35
using Lucene.Net.Support.Compatibility;
#else
using System.Collections.Concurrent;
#endif
using System.Diagnostics;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace Lucene.Net.Spatial.Util
{
	public static class CompatibilityExtensions
	{
		public static void Append(this ITermAttribute termAtt, string str)
		{
			termAtt.SetTermBuffer(termAtt.Term + str); // TODO: Not optimal, but works
		}

		public static void Append(this ITermAttribute termAtt, char ch)
		{
			termAtt.SetTermBuffer(termAtt.Term + new string(new[] { ch })); // TODO: Not optimal, but works
		}

        private static readonly ConcurrentDictionary<Key<string, IndexReader>, IBits> _docsWithFieldCache = new ConcurrentDictionary<Key<string, IndexReader>, IBits>();

        //john diss: previously _docsWithFieldCache was keyed only by field name, when a cache request would come in it would always
        //return the entry associated with the first segment reader ever encountered for that field name.
        //adding the reader to the cache key fixes this issue, though I am not experienced enough with the inner workings of Lucene.net
        //to know if this is a bad idea for other reasons e.g transient readers or such like

        internal static IBits GetDocsWithField(this FieldCache fc, IndexReader reader, String field, IState state)
		{
			return _docsWithFieldCache.GetOrAdd(new Key<string,IndexReader>(field, reader), key => DocsWithFieldCacheEntry_CreateValue(key.Item2, new Entry(key.Item1, null), false, state));
		}

        /// <summary> <p/>
        /// EXPERT: Instructs the FieldCache to forcibly expunge all entries 
        /// from the underlying caches.  This is intended only to be used for 
        /// test methods as a way to ensure a known base state of the Cache 
        /// (with out needing to rely on GC to free WeakReferences).  
        /// It should not be relied on for "Cache maintenance" in general 
        /// application code.
        /// <p/>
        /// <p/>
        /// <b>EXPERIMENTAL API:</b> This API is considered extremely advanced 
        /// and experimental.  It may be removed or altered w/o warning in future 
        /// releases 
        /// of Lucene.
        /// <p/>
        /// </summary>
        public static void PurgeSpatialCaches(this FieldCache fc)
        {
            _docsWithFieldCache.Clear();
        }

		private static IBits DocsWithFieldCacheEntry_CreateValue(IndexReader reader, Entry entryKey, bool setDocsWithField /* ignored */, IState state)
		{
			var field = entryKey.field;
			FixedBitSet res = null;
			var terms = new TermsEnumCompatibility(reader, field, state);
			var maxDoc = reader.MaxDoc;

			var term = terms.Next(state);
			if (term != null)
			{
				int termsDocCount = terms.GetDocCount();
				Debug.Assert(termsDocCount <= maxDoc);
				if (termsDocCount == maxDoc)
				{
					// Fast case: all docs have this field:
					return new MatchAllBits(maxDoc);
				}

				while (true)
				{
					if (res == null)
					{
						// lazy init
						res = new FixedBitSet(maxDoc);
					}

					var termDocs = reader.TermDocs(term, state);
					while (termDocs.Next(state))
					{
						res.Set(termDocs.Doc);
					}
		
					term = terms.Next(state);
					if (term == null)
					{
						break;
					}
				}
			}
			if (res == null)
			{
				return new MatchNoBits(maxDoc);
			}
			int numSet = res.Cardinality();
			if (numSet >= maxDoc)
			{
				// The cardinality of the BitSet is maxDoc if all documents have a value.
				Debug.Assert(numSet == maxDoc);
				return new MatchAllBits(maxDoc);
			}
			return res;
		}

		/* table of number of leading zeros in a byte */
		public static readonly byte[] nlzTable = { 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

		/// <summary>
		/// Returns the number of leading zero bits.
		/// </summary>
		/// <param name="x"></param>
		/// <returns></returns>
		public static int BitUtilNlz(long x)
		{
			int n = 0;
			// do the first step as a long
			var y = (int)((ulong)x >> 32);
			if (y == 0) { n += 32; y = (int)(x); }
			if ((y & 0xFFFF0000) == 0) { n += 16; y <<= 16; }
			if ((y & 0xFF000000) == 0) { n += 8; y <<= 8; }
			return n + nlzTable[(uint)y >> 24];
			/* implementation without table:
			  if ((y & 0xF0000000) == 0) { n+=4; y<<=4; }
			  if ((y & 0xC0000000) == 0) { n+=2; y<<=2; }
			  if ((y & 0x80000000) == 0) { n+=1; y<<=1; }
			  if ((y & 0x80000000) == 0) { n+=1;}
			  return n;
			 */
		}
	}

    //john diss: this is to use in place of Tuple which does not exist in net35
    internal struct Key<T1, T2> : IEquatable<Key<T1, T2>>
    {
        public bool Equals(Key<T1, T2> other)
        {
            return EqualityComparer<T1>.Default.Equals(Item1, other.Item1) && EqualityComparer<T2>.Default.Equals(Item2, other.Item2);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            return obj is Key<T1, T2> && Equals((Key<T1, T2>)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (EqualityComparer<T1>.Default.GetHashCode(Item1) * 397) ^ EqualityComparer<T2>.Default.GetHashCode(Item2);
            }
        }

        public static bool operator ==(Key<T1, T2> left, Key<T1, T2> right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(Key<T1, T2> left, Key<T1, T2> right)
        {
            return !left.Equals(right);
        }

        public readonly T1 Item1;
        public readonly T2 Item2;

        public Key(T1 item1, T2 item2)
        {
            Item1 = item1;
            Item2 = item2;
        }
    }
	public static class Arrays
	{
		public static void Fill<T>(T[] array, int fromIndex, int toIndex, T value)
		{
			if (array == null)
			{
				throw new ArgumentNullException("array");
			}
			if (fromIndex < 0 || fromIndex > toIndex)
			{
				throw new ArgumentOutOfRangeException("fromIndex");
			}
			if (toIndex > array.Length)
			{
				throw new ArgumentOutOfRangeException("toIndex");
			}
			for (var i = fromIndex; i < toIndex; i++)
			{
				array[i] = value;
			}
		}
	}

	/// <summary>
	/// Expert: Every composite-key in the internal cache is of this type.
	/// </summary>
	internal class Entry
	{
		internal readonly String field;        // which Fieldable
		internal readonly Object custom;       // which custom comparator or parser

		/* Creates one of these objects for a custom comparator/parser. */
		public Entry(String field, Object custom)
		{
			this.field = field;
			this.custom = custom;
		}

		/* Two of these are equal iff they reference the same field and type. */
		public override bool Equals(Object o)
		{
			var other = o as Entry;
			if (other != null)
			{
				if (other.field.Equals(field))
				{
					if (other.custom == null)
					{
						if (custom == null) return true;
					}
					else if (other.custom.Equals(custom))
					{
						return true;
					}
				}
			}
			return false;
		}

		/* Composes a hashcode based on the field and type. */
		public override int GetHashCode()
		{
			return field.GetHashCode() ^ (custom == null ? 0 : custom.GetHashCode());
		}


	}

}
