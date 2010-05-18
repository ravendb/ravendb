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
using DocIdBitSet = Lucene.Net.Util.DocIdBitSet;

namespace Lucene.Net.Search
{
	
	[Serializable]
	public class SingleDocTestFilter:Filter
	{
		private int doc;
		
		public SingleDocTestFilter(int doc)
		{
			this.doc = doc;
		}
		
		public override DocIdSet GetDocIdSet(IndexReader reader)
		{
			System.Collections.BitArray bits = new System.Collections.BitArray((reader.MaxDoc() % 64 == 0?reader.MaxDoc() / 64:reader.MaxDoc() / 64 + 1) * 64);
			bits.Set(doc, true);
			return new DocIdBitSet(bits);
		}
	}
}