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
using ToStringUtils = Lucene.Net.Util.ToStringUtils;

namespace Lucene.Net.Search
{

    /// <summary>Implements the wildcard search query. Supported wildcards are <c>*</c>, which
    /// matches any character sequence (including the empty one), and <c>?</c>,
    /// which matches any single character. Note this query can be slow, as it
    /// needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
    /// a Wildcard term should not start with one of the wildcards <c>*</c> or
    /// <c>?</c>.
    /// 
    /// <p/>This query uses the <see cref="MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT" />
    ///
    /// rewrite method.
    /// 
    /// </summary>
    /// <seealso cref="WildcardTermEnum">
    /// </seealso>

        [Serializable]
    public class WildcardQuery : MultiTermQuery
	{
		private readonly bool _termContainsWildcard;
	    private readonly bool _termIsPrefix;
		protected internal Term internalTerm;
		
		public WildcardQuery(Term term)
		{ 
			this.internalTerm = term;
		    string text = term.Text;
		    _termContainsWildcard = (term.Text.IndexOf('*') != -1)
		                                || (term.Text.IndexOf('?') != -1);
		    _termIsPrefix = _termContainsWildcard
		                        && (text.IndexOf('?') == -1)
		                        && (text.IndexOf('*') == text.Length - 1);
		}
		
		protected internal override FilteredTermEnum GetEnum(IndexReader reader, IState state)
		{
            if (_termContainsWildcard)
            {
                return new WildcardTermEnum(reader, Term, state);
            }
            else
            {
                return new SingleTermEnum(reader, Term, state);
            }
		}

	    /// <summary> Returns the pattern term.</summary>
	    public Term Term
	    {
	        get { return internalTerm; }
	    }

	    public override Query Rewrite(IndexReader reader, IState state)
		{
            if (_termIsPrefix)
            {
                MultiTermQuery rewritten =
                    new PrefixQuery(internalTerm.CreateTerm(internalTerm.Text.Substring(0, internalTerm.Text.IndexOf('*'))));
                rewritten.Boost = Boost;
                rewritten.RewriteMethod = RewriteMethod;
                return rewritten;
            }
            else
            {
                return base.Rewrite(reader, state);
            }
		}
		
		/// <summary>Prints a user-readable version of this query. </summary>
		public override System.String ToString(System.String field)
		{
			System.Text.StringBuilder buffer = new System.Text.StringBuilder();
			if (!internalTerm.Field.Equals(field))
			{
				buffer.Append(internalTerm.Field);
				buffer.Append(":");
			}
			buffer.Append(internalTerm.Text);
			buffer.Append(ToStringUtils.Boost(Boost));
			return buffer.ToString();
		}
		
		//@Override
		public override int GetHashCode()
		{
			int prime = 31;
			int result = base.GetHashCode();
			result = prime * result + ((internalTerm == null)?0:internalTerm.GetHashCode());
			return result;
		}
		
		//@Override
		public  override bool Equals(System.Object obj)
		{
			if (this == obj)
				return true;
			if (!base.Equals(obj))
				return false;
			if (GetType() != obj.GetType())
				return false;
			WildcardQuery other = (WildcardQuery) obj;
			if (internalTerm == null)
			{
				if (other.internalTerm != null)
					return false;
			}
			else if (!internalTerm.Equals(other.internalTerm))
				return false;
			return true;
		}
	}
}