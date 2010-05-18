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

namespace Lucene.Net.Util
{
	
    [TestFixture]
	public class TestCloseableThreadLocal:LuceneTestCase
	{
		public const System.String TEST_VALUE = "initvaluetest";
		
        [Test]
		public virtual void  TestInitValue()
		{
			InitValueThreadLocal tl = new InitValueThreadLocal(this);
			System.String str = (System.String) tl.Get();
			Assert.AreEqual(TEST_VALUE, str);
		}
		
        [Test]
		public virtual void  TestNullValue()
		{
			// Tests that null can be set as a valid value (LUCENE-1805). This
			// previously failed in get().
			CloseableThreadLocal ctl = new CloseableThreadLocal();
			ctl.Set((System.Object) null);
			Assert.IsNull(ctl.Get());
		}
		
        [Test]
		public virtual void  TestDefaultValueWithoutSetting()
		{
			// LUCENE-1805: make sure default get returns null,
			// twice in a row
			CloseableThreadLocal ctl = new CloseableThreadLocal();
			Assert.IsNull(ctl.Get());
			Assert.IsNull(ctl.Get());
		}
		
		public class InitValueThreadLocal:CloseableThreadLocal
		{
			public InitValueThreadLocal(TestCloseableThreadLocal enclosingInstance)
			{
				InitBlock(enclosingInstance);
			}
			private void  InitBlock(TestCloseableThreadLocal enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
			}
			private TestCloseableThreadLocal enclosingInstance;
			public TestCloseableThreadLocal Enclosing_Instance
			{
				get
				{
					return enclosingInstance;
				}
				
			}
			public /*protected internal*/ override System.Object InitialValue()
			{
				return Lucene.Net.Util.TestCloseableThreadLocal.TEST_VALUE;
			}
		}
	}
}