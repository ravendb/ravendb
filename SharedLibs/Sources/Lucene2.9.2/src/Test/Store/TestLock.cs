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

namespace Lucene.Net.Store
{
	
	[TestFixture]
	public class TestLock:LuceneTestCase
	{
		
		[Test]
		public virtual void  TestObtain()
		{
			LockMock lock_Renamed = new LockMock(this);
			Lock.LOCK_POLL_INTERVAL = 10;
			
			try
			{
				lock_Renamed.Obtain(Lock.LOCK_POLL_INTERVAL);
				Assert.Fail("Should have failed to obtain lock");
			}
			catch (System.IO.IOException e)
			{
				Assert.AreEqual(lock_Renamed.lockAttempts, 2, "should attempt to lock more than once");
			}
		}
		
		private class LockMock:Lock
		{
			public LockMock(TestLock enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestLock enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestLock enclosingInstance;
			public TestLock Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public int lockAttempts;
			
			public override bool Obtain()
			{
				lockAttempts++;
				return false;
			}
			public override void  Release()
			{
				// do nothing
			}
			public override bool IsLocked()
			{
				return false;
			}
		}
	}
}