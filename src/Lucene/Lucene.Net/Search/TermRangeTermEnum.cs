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
using Term = Lucene.Net.Index.Term;
using StringHelper = Lucene.Net.Util.StringHelper;

namespace Lucene.Net.Search
{
	
	/// <summary> Subclass of FilteredTermEnum for enumerating all terms that match the
	/// specified range parameters.
	/// <p/>
	/// Term enumerations are always ordered by Term.compareTo().  Each term in
	/// the enumeration is greater than all that precede it.
	/// </summary>
	/// <since> 2.9
	/// </since>
	public class TermRangeTermEnum:FilteredTermEnum
	{
		
		private System.Globalization.CompareInfo collator = null;
		private bool endEnum = false;
		private System.String field;
		private System.String upperTermText;
		private System.String lowerTermText;
		private bool includeLower;
		private bool includeUpper;
		
		/// <summary> Enumerates all terms greater/equal than <c>lowerTerm</c>
		/// but less/equal than <c>upperTerm</c>. 
		/// 
		/// If an endpoint is null, it is said to be "open". Either or both 
		/// endpoints may be open.  Open endpoints may not be exclusive 
		/// (you can't select all but the first or last term without 
		/// explicitly specifying the term to exclude.)
		/// 
		/// </summary>
		/// <param name="reader">
		/// </param>
		/// <param name="field">An interned field that holds both lower and upper terms.
		/// </param>
		/// <param name="lowerTermText">The term text at the lower end of the range
		/// </param>
		/// <param name="upperTermText">The term text at the upper end of the range
		/// </param>
		/// <param name="includeLower">If true, the <c>lowerTerm</c> is included in the range.
		/// </param>
		/// <param name="includeUpper">If true, the <c>upperTerm</c> is included in the range.
		/// </param>
		/// <param name="collator">The collator to use to collate index Terms, to determine their
		/// membership in the range bounded by <c>lowerTerm</c> and
		/// <c>upperTerm</c>.
		/// 
		/// </param>
		/// <throws>  IOException </throws>
		public TermRangeTermEnum(IndexReader reader, System.String field, System.String lowerTermText, System.String upperTermText, bool includeLower, bool includeUpper, System.Globalization.CompareInfo collator, IState state)
		{
			this.collator = collator;
			this.upperTermText = upperTermText;
			this.lowerTermText = lowerTermText;
			this.includeLower = includeLower;
			this.includeUpper = includeUpper;
			this.field = StringHelper.Intern(field);
			
			// do a little bit of normalization...
			// open ended range queries should always be inclusive.
			if (this.lowerTermText == null)
			{
				this.lowerTermText = "";
				this.includeLower = true;
			}
			
			if (this.upperTermText == null)
			{
				this.includeUpper = true;
			}
			
			System.String startTermText = collator == null?this.lowerTermText:"";
			SetEnum(reader.Terms(new Term(this.field, startTermText), state), state);
		}
		
		public override float Difference()
		{
			return 1.0f;
		}
		
		public override bool EndEnum()
		{
			return endEnum;
		}

	    protected internal override bool TermCompare(Term term)
		{
			if (collator == null)
			{
				// Use Unicode code point ordering
			    bool checkLower = !includeLower;
			    if (term != null && (System.Object) term.Field == (System.Object) field)
				{
					// interned comparison
					if (!checkLower || null == lowerTermText || String.CompareOrdinal(term.Text, lowerTermText) > 0)
					{
						checkLower = false;
						if (upperTermText != null)
						{
							int compare = String.CompareOrdinal(upperTermText, term.Text);
							/*
							* if beyond the upper term, or is exclusive and this is equal to
							* the upper term, break out
							*/
							if ((compare < 0) || (!includeUpper && compare == 0))
							{
								endEnum = true;
								return false;
							}
						}
						return true;
					}
				}
				else
				{
					// break
					endEnum = true;
					return false;
				}
				return false;
			}
			else
			{
				if (term != null && (System.Object) term.Field == (System.Object) field)
				{
					// interned comparison
					if ((lowerTermText == null || (includeLower?collator.Compare(term.Text.ToString(), lowerTermText.ToString()) >= 0:collator.Compare(term.Text.ToString(), lowerTermText.ToString()) > 0)) && (upperTermText == null || (includeUpper?collator.Compare(term.Text.ToString(), upperTermText.ToString()) <= 0:collator.Compare(term.Text.ToString(), upperTermText.ToString()) < 0)))
					{
						return true;
					}
					return false;
				}
				endEnum = true;
				return false;
			}
		}
	}
}