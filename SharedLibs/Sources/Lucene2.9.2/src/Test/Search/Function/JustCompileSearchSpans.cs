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
using FieldCache = Lucene.Net.Search.FieldCache;

namespace Lucene.Net.Search.Function
{
	
	/// <summary> Holds all implementations of classes in the o.a.l.s.function package as a
	/// back-compatibility test. It does not run any tests per-se, however if
	/// someone adds a method to an interface or abstract method to an abstract
	/// class, one of the implementations here will fail to compile and so we know
	/// back-compat policy was violated.
	/// </summary>
	sealed class JustCompileSearchFunction
	{
		
		private const System.String UNSUPPORTED_MSG = "unsupported: used for back-compat testing only !";
		
		internal sealed class JustCompileDocValues:DocValues
		{
			
			public override float FloatVal(int doc)
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Function.JustCompileSearchFunction.UNSUPPORTED_MSG);
			}
			
			public override System.String ToString(int doc)
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Function.JustCompileSearchFunction.UNSUPPORTED_MSG);
			}
		}
		
		[Serializable]
		internal sealed class JustCompileFieldCacheSource:FieldCacheSource
		{
			
			public JustCompileFieldCacheSource(System.String field):base(field)
			{
			}
			
			public override bool CachedFieldSourceEquals(FieldCacheSource other)
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Function.JustCompileSearchFunction.UNSUPPORTED_MSG);
			}
			
			public override int CachedFieldSourceHashCode()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Function.JustCompileSearchFunction.UNSUPPORTED_MSG);
			}
			
			public override DocValues GetCachedFieldValues(FieldCache cache, System.String field, IndexReader reader)
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Function.JustCompileSearchFunction.UNSUPPORTED_MSG);
			}
		}
		
		[Serializable]
		internal sealed class JustCompileValueSource:ValueSource
		{
			
			public override System.String Description()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Function.JustCompileSearchFunction.UNSUPPORTED_MSG);
			}
			
			public  override bool Equals(System.Object o)
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Function.JustCompileSearchFunction.UNSUPPORTED_MSG);
			}
			
			public override DocValues GetValues(IndexReader reader)
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Function.JustCompileSearchFunction.UNSUPPORTED_MSG);
			}
			
			public override int GetHashCode()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Function.JustCompileSearchFunction.UNSUPPORTED_MSG);
			}
		}
	}
}