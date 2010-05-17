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

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestTerm:LuceneTestCase
	{
		
		[Test]
		public virtual void  TestEquals()
		{
			Term base_Renamed = new Term("same", "same");
			Term same = new Term("same", "same");
			Term differentField = new Term("different", "same");
			Term differentText = new Term("same", "different");
			System.String differentType = "AString";
			Assert.AreEqual(base_Renamed, base_Renamed);
			Assert.AreEqual(base_Renamed, same);
			Assert.IsFalse(base_Renamed.Equals(differentField));
			Assert.IsFalse(base_Renamed.Equals(differentText));
			Assert.IsFalse(base_Renamed.Equals(differentType));
		}
	}
}