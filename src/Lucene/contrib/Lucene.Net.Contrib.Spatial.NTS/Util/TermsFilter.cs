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
using System.Diagnostics;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace Lucene.Net.Spatial.Util
{
	/// <summary>
	/// Constructs a filter for docs matching any of the terms added to this class.
	/// Unlike a RangeFilter this can be used for filtering on multiple terms that are not necessarily in
	/// a sequence. An example might be a collection of primary keys from a database query result or perhaps
	/// a choice of "category" labels picked by the end user. As a filter, this is much faster than the
	/// equivalent query (a BooleanQuery with many "should" TermQueries)
	/// </summary>
	public class TermsFilter : Filter
	{
		private readonly SortedSet<Term> terms = new SortedSet<Term>();

		/// <summary>
		/// Adds a term to the list of acceptable terms
		/// </summary>
		/// <param name="term"></param>
		public void AddTerm(Term term)
		{
			terms.Add(term);
		}

		public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
		{
			var result = new FixedBitSet(reader.MaxDoc);
			var fields = reader.GetFieldNames(IndexReader.FieldOption.ALL);

			if (fields == null || fields.Count == 0)
			{
				return result;
			}

			String lastField = null;
			TermsEnumCompatibility termsEnum = null;
			foreach (Term term in terms)
			{
				if (!term.Field.Equals(lastField))
				{
					var termsC = new TermsEnumCompatibility(reader, term.Field, state);
					if (termsC.Term() == null)
					{
						return result;
					}
					termsEnum = termsC;
					lastField = term.Field;
				}

				if (terms != null)
				{
					// TODO this check doesn't make sense, decide which variable its supposed to be for
					Debug.Assert(termsEnum != null);
					if (termsEnum.SeekCeil(term.Text, state) == TermsEnumCompatibility.SeekStatus.FOUND)
					{
						termsEnum.Docs(result, state);
					}
				}
			}
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
				return true;

			if ((obj == null) || (obj.GetType() != this.GetType()))
				return false;

			var test = (TermsFilter)obj;
			if (terms == test.terms)
				return true;
			if (terms == null || terms.Count != test.terms.Count)
				return false;

			var e1 = terms.GetEnumerator();
			var e2 = test.terms.GetEnumerator();
			while (e1.MoveNext() && e2.MoveNext())
			{
				if (!e1.Current.Equals(e2.Current)) return false;
			}
			return true;
		}

		public override int GetHashCode()
		{
			int hash = 9;
			foreach (Term term in terms)
			{
				hash = 31 * hash + term.GetHashCode();
			}
			return hash;
		}
	}
}
