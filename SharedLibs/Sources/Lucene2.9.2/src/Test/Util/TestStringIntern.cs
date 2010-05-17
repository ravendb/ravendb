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
using System.Threading;

using NUnit.Framework;

namespace Lucene.Net.Util
{
	
    [TestFixture]
	public class TestStringIntern:LuceneTestCase
	{
		public TestStringIntern()
		{
			InitBlock();
		}
		private class TestRun
		{
			public TestRun(Int32 seed, int iter, bool newStrings, TestStringIntern enclosingInstance)
			{
				this.seed = seed;
				this.iter = iter;
				this.newStrings = newStrings;
				this.enclosingInstance = enclosingInstance;
                this.Reset = new ManualResetEvent(false);
			}
			private System.Int32 seed;
			private int iter;
			private bool newStrings;
			private TestStringIntern enclosingInstance;
			public TestStringIntern Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}

            public ManualResetEvent Reset;

			public void Run(System.Object state)
			{
				System.Random rand = new Random(seed);
				System.String[] myInterned = new System.String[Enclosing_Instance.testStrings.Length];
				for (int j = 0; j < iter; j++)
				{
					int idx = rand.Next(Enclosing_Instance.testStrings.Length);
					System.String s = Enclosing_Instance.testStrings[idx];
					if (newStrings == true && (new System.Random().NextDouble()) > 0.5)
						s = new System.Text.StringBuilder(s).ToString(); // make a copy half of the time
					System.String interned = StringHelper.Intern(s);
					System.String prevInterned = myInterned[idx];
					System.String otherInterned = Enclosing_Instance.internedStrings[idx];
					
					// test against other threads
					if (otherInterned != null && otherInterned != interned)
					{
						Assert.Fail(); // TestCase.fail();
					}
					Enclosing_Instance.internedStrings[idx] = interned;
					
					// test against local copy
					if (prevInterned != null && prevInterned != interned)
					{
						Assert.Fail(); // TestCase.fail();
					}
					myInterned[idx] = interned;
				}
                this.Reset.Set();
			}
		}
		private void  InitBlock()
		{
			r = NewRandom();
		}
		internal System.String[] testStrings;
		internal System.String[] internedStrings;
		internal System.Random r;
		
		private System.String RandStr(int len)
		{
			char[] arr = new char[len];
			for (int i = 0; i < len; i++)
			{
				arr[i] = (char) ('a' + r.Next(26));
			}
			return new System.String(arr);
		}
		
		private void  MakeStrings(int sz)
		{
			testStrings = new System.String[sz];
			internedStrings = new System.String[sz];
			for (int i = 0; i < sz; i++)
			{
				testStrings[i] = RandStr(r.Next(8) + 3);
			}
		}
		
        [Test]
		public virtual void  TestStringIntern_Renamed()
		{
			MakeStrings(1024 * 10); // something greater than the capacity of the default cache size
			// makeStrings(100);  // realistic for perf testing
			int nThreads = 20;
			// final int iter=100000;
			int iter = 1000000;
			bool newStrings = true;
			
			// try native intern
			// StringHelper.interner = new StringInterner();
			
			TestRun[] threads = new TestRun[nThreads];
            ManualResetEvent[] resets = new ManualResetEvent[nThreads];
			for (int i = 0; i < nThreads; i++)
			{
				int seed = i;
				threads[i] = new TestRun(seed, iter, newStrings, this);
                resets[i] = threads[i].Reset;
                ThreadPool.QueueUserWorkItem(new WaitCallback(threads[i].Run));
			}

            WaitHandle.WaitAll(resets);
		}
	}
}