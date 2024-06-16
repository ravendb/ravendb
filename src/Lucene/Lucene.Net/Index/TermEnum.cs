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

namespace Lucene.Net.Index
{
	
	/// <summary>Abstract class for enumerating terms.
	/// <p/>Term enumerations are always ordered by Term.compareTo().  Each term in
	/// the enumeration is greater than all that precede it.  
	/// </summary>
	public abstract class TermEnum : IDisposable
	{
		/// <summary>Increments the enumeration to the next element.  True if one exists.</summary>
		public abstract bool Next(IState state);

	    /// <summary>Returns the current Term in the enumeration.</summary>
	    public abstract Term Term { get; }

	    /// <summary>Returns the docFreq of the current Term in the enumeration.</summary>
		public abstract int DocFreq();

        /// <summary>Closes the enumeration to further activity, freeing resources. </summary>
        [Obsolete("Use Dispose() instead")]
        public void Close()
        {
            Dispose();
        }

	    /// <summary>Closes the enumeration to further activity, freeing resources. </summary>
	    public void Dispose()
	    {
	        Dispose(true);
	    }

	    protected abstract void Dispose(bool disposing);
	}
}