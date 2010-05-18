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

using WhitespaceAnalyzer = Lucene.Net.Analysis.WhitespaceAnalyzer;
using Document = Lucene.Net.Documents.Document;
using Field = Lucene.Net.Documents.Field;
using IndexReader = Lucene.Net.Index.IndexReader;
using IndexWriter = Lucene.Net.Index.IndexWriter;
using Term = Lucene.Net.Index.Term;
using ParseException = Lucene.Net.QueryParsers.ParseException;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using RAMDirectory = Lucene.Net.Store.RAMDirectory;

namespace Lucene.Net.Search
{
	
	/// <summary> subclass of TestSimpleExplanations that verifies non matches.</summary>
	public class TestComplexExplanationsOfNonMatches:TestComplexExplanations
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