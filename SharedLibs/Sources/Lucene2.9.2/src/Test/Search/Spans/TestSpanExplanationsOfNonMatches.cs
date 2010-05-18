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

using CheckHits = Lucene.Net.Search.CheckHits;
using Query = Lucene.Net.Search.Query;

namespace Lucene.Net.Search.Spans
{
	
	
	/// <summary> subclass of TestSimpleExplanations that verifies non matches.</summary>
    [TestFixture]
	public class TestSpanExplanationsOfNonMatches:TestSpanExplanations
	{
		
		/// <summary> Overrides superclass to ignore matches and focus on non-matches
		/// 
		/// </summary>
		/// <seealso cref="CheckHits.checkNoMatchExplanations">
		/// </seealso>
		public override void  Qtest(Query q, int[] expDocNrs)
		{
			CheckHits.CheckNoMatchExplanations(q, FIELD, searcher, expDocNrs);
		}
	}
}