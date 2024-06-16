/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Collections.Generic;
using Spatial4n.Shapes;

namespace Lucene.Net.Spatial.Util
{
    /// <summary>
    /// Bounded Cache of Shapes associated with docIds.  Note, multiple Shapes can be
    /// associated with a given docId
    /// </summary>
    /// <typeparam name="T"></typeparam>
	public class ShapeFieldCache<T> where T : IShape
	{
		private readonly IList<T>[] cache;
		public int defaultLength;

		public ShapeFieldCache(int length, int defaultLength)
		{
			cache = new IList<T>[length];
			this.defaultLength = defaultLength;
		}

		public void Add(int docid, T s)
		{
			IList<T> list = cache[docid];
			if (list == null)
			{
				list = cache[docid] = new List<T>(defaultLength);
			}
			list.Add(s);
		}

		public IList<T> GetShapes(int docid)
		{
			return cache[docid];
		}

	}
}
