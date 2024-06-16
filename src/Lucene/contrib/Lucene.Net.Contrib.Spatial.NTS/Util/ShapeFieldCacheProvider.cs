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

using System;
using System.Runtime.CompilerServices;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Spatial4n.Shapes;
#if NET35
using Lucene.Net.Support;
#endif

namespace Lucene.Net.Spatial.Util
{
    /// <summary>
    /// Provides access to a {@link ShapeFieldCache} for a given {@link AtomicReader}.
    /// 
    /// If a Cache does not exist for the Reader, then it is built by iterating over
    /// the all terms for a given field, reconstructing the Shape from them, and adding
    /// them to the Cache.
    /// </summary>
    /// <typeparam name="T"></typeparam>
	public abstract class ShapeFieldCacheProvider<T> where T : IShape
	{
		//private Logger log = Logger.getLogger(getClass().getName());

		// it may be a List<T> or T
#if !NET35
		private readonly ConditionalWeakTable<IndexReader, ShapeFieldCache<T>> sidx =
			new ConditionalWeakTable<IndexReader, ShapeFieldCache<T>>(); // WeakHashMap
#else
	    private readonly WeakDictionary<IndexReader, ShapeFieldCache<T>> sidx =
	        new WeakDictionary<IndexReader, ShapeFieldCache<T>>();
#endif


		protected readonly int defaultSize;
		protected readonly String shapeField;

		protected ShapeFieldCacheProvider(String shapeField, int defaultSize)
		{
			this.shapeField = shapeField;
			this.defaultSize = defaultSize;
		}

		protected abstract T ReadShape(/*BytesRef*/ Term term);

		private readonly object locker = new object();

		public ShapeFieldCache<T> GetCache(IndexReader reader, IState state)
		{
			lock (locker)
			{
				ShapeFieldCache<T> idx;
				if (sidx.TryGetValue(reader, out idx) && idx != null)
				{
					return idx;
				}

				//long startTime = System.CurrentTimeMillis();
				//log.fine("Building Cache [" + reader.MaxDoc() + "]");

				idx = new ShapeFieldCache<T>(reader.MaxDoc, defaultSize);
				var count = 0;
				var tec = new TermsEnumCompatibility(reader, shapeField, state);

				var term = tec.Next(state);
				while (term != null)
				{
					var shape = ReadShape(term);
					if (shape != null)
					{
						var docs = reader.TermDocs(new Term(shapeField, tec.Term().Text), state);
						while (docs.Next(state))
						{
							idx.Add(docs.Doc, shape);
							count++;
						}
					}
					term = tec.Next(state);
				}

				sidx.Add(reader, idx);
				tec.Close();

				//long elapsed = System.CurrentTimeMillis() - startTime;
				//log.fine("Cached: [" + count + " in " + elapsed + "ms] " + idx);
				return idx;
			}
		}
	}
}
