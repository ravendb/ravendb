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

namespace Lucene.Net.Search
{

    /// <summary>A clause in a BooleanQuery. </summary>
    [Serializable]
    public class BooleanClause
    {
	    private Occur occur;
		
		/// <summary>Constructs a BooleanClause.</summary>
		public BooleanClause(Query query, Occur occur)
		{
            this._query = query;
			this.occur = occur;
		}

	    public virtual Occur Occur
	    {
	        get { return occur; }
	        set { this.occur = value; }
	    }

	    private Query _query;

	    /// <summary>The query whose matching documents are combined by the boolean query.</summary>
	    public virtual Query Query
	    {
	        get { return _query; }
	        set { this._query = value; }
	    }

	    public virtual bool IsProhibited
	    {
	        get { return Occur.MUST_NOT.Equals(occur); }
	    }

	    public virtual bool IsRequired
	    {
	        get { return Occur.MUST.Equals(occur); }
	    }


	    /// <summary>Returns true if <c>o</c> is equal to this. </summary>
		public  override bool Equals(System.Object o)
		{
			if (o == null || !(o is BooleanClause))
				return false;
			BooleanClause other = (BooleanClause) o;
			return this.Query.Equals(other.Query) && this.occur.Equals(other.occur);
		}
		
		/// <summary>Returns a hash code value for this object.</summary>
		public override int GetHashCode()
		{
			return Query.GetHashCode() ^ (Occur.MUST.Equals(occur)?1:0) ^ (Occur.MUST_NOT.Equals(occur)?2:0);
		}
		
		
		public override System.String ToString()
		{
            return OccurExtensions.ToString(occur) + Query;
		}
	}

	public enum Occur
	{
		MUST,
		SHOULD,
		MUST_NOT
	}

	public static class OccurExtensions
	{
		public static System.String ToString(this Occur occur)
		{
			if (occur == Occur.MUST)
				return "+";
			if (occur == Occur.MUST_NOT)
				return "-";
			return "";
		}
	}
}