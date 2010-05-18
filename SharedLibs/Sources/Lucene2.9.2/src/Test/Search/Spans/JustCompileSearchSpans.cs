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
using Similarity = Lucene.Net.Search.Similarity;
using Weight = Lucene.Net.Search.Weight;

namespace Lucene.Net.Search.Spans
{
	
	/// <summary> Holds all implementations of classes in the o.a.l.s.spans package as a
	/// back-compatibility test. It does not run any tests per-se, however if
	/// someone adds a method to an interface or abstract method to an abstract
	/// class, one of the implementations here will fail to compile and so we know
	/// back-compat policy was violated.
	/// </summary>
	sealed class JustCompileSearchSpans
	{
		
		private const System.String UNSUPPORTED_MSG = "unsupported: used for back-compat testing only !";
		
		internal sealed class JustCompileSpans:Spans
		{
			
			public override int Doc()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override int End()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override bool Next()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override bool SkipTo(int target)
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override int Start()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override System.Collections.Generic.ICollection<byte[]> GetPayload()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override bool IsPayloadAvailable()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
		}
		
		[Serializable]
		internal sealed class JustCompileSpanQuery:SpanQuery
		{
			
			public override System.String GetField()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override Spans GetSpans(IndexReader reader)
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			/// <deprecated> delete in 3.0. 
			/// </deprecated>
            [Obsolete("delete in 3.0")]
			public override System.Collections.ICollection GetTerms()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override System.String ToString(System.String field)
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
		}
		
		internal sealed class JustCompilePayloadSpans:Spans
		{
			
			public override System.Collections.Generic.ICollection<byte[]> GetPayload()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override bool IsPayloadAvailable()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override int Doc()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override int End()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override bool Next()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override bool SkipTo(int target)
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
			
			public override int Start()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
		}
		
		internal sealed class JustCompileSpanScorer:SpanScorer
		{
			
			internal JustCompileSpanScorer(Spans spans, Weight weight, Similarity similarity, byte[] norms):base(spans, weight, similarity, norms)
			{
			}
			
			public /*protected internal*/ override bool SetFreqCurrentDoc()
			{
				throw new System.NotSupportedException(Lucene.Net.Search.Spans.JustCompileSearchSpans.UNSUPPORTED_MSG);
			}
		}
	}
}