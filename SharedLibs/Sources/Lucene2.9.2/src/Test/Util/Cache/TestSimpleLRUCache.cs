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

using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Util.Cache
{
	
	[TestFixture]
	public class TestSimpleLRUCache:LuceneTestCase
	{
		
		[Test]
		public virtual void  TestLRUCache()
		{
			int n = 100;
			System.Object dummy = new System.Object();
			
			Cache cache = new SimpleLRUCache(n);
			
			for (int i = 0; i < n; i++)
			{
				cache.Put((System.Object) i, dummy);
			}
			
			// access every 2nd item in cache
			for (int i = 0; i < n; i += 2)
			{
				Assert.IsNotNull(cache.Get((System.Object) i));
			}
			
			// add n/2 elements to cache, the ones that weren't
			// touched in the previous loop should now be thrown away
			for (int i = n; i < n + (n / 2); i++)
			{
				cache.Put((System.Object) i, dummy);
			}
			
			// access every 4th item in cache
			for (int i = 0; i < n; i += 4)
			{
				Assert.IsNotNull(cache.Get((System.Object) i));
			}
			
			// add 3/4n elements to cache, the ones that weren't
			// touched in the previous loops should now be thrown away
			for (int i = n; i < n + (n * 3 / 4); i++)
			{
				cache.Put((System.Object) i, dummy);
			}
			
			// access every 4th item in cache
			for (int i = 0; i < n; i += 4)
			{
				Assert.IsNotNull(cache.Get((System.Object) i));
			}
		}
	}
}