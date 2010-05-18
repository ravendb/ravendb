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

namespace Lucene.Net.Util
{
	
	/// <summary> A serializable Enum class.</summary>
	[Serializable]
    public abstract class Parameter : System.Runtime.Serialization.IObjectReference
	{
		internal static System.Collections.IDictionary allParameters = new System.Collections.Hashtable();
		
		private System.String name;
		
		private Parameter()
		{
			// typesafe enum pattern, no public constructor
		}
		
		protected internal Parameter(System.String name)
		{
			// typesafe enum pattern, no public constructor
			this.name = name;
			System.String key = MakeKey(name);
			
			if (allParameters.Contains(key))
				throw new System.ArgumentException("Parameter name " + key + " already used!");
			
			allParameters[key] = this;
		}
		
		private System.String MakeKey(System.String name)
		{
			return GetType() + " " + name;
		}
		
		public override System.String ToString()
		{
			return name;
		}
		
		/// <summary> Resolves the deserialized instance to the local reference for accurate
		/// equals() and == comparisons.
		/// 
		/// </summary>
		/// <returns> a reference to Parameter as resolved in the local VM
		/// </returns>
		/// <throws>  ObjectStreamException </throws>
		protected internal virtual System.Object ReadResolve()
		{
			System.Object par = allParameters[MakeKey(name)];
			
			if (par == null)
				throw new System.IO.IOException("Unknown parameter value: " + name);
			
			return par;
		}

        // "ReadResolve"s equivalent for .NET
        public Object GetRealObject(System.Runtime.Serialization.StreamingContext context)
        {
            return ReadResolve();
        }
	}
}