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

namespace Lucene.Net.Analysis
{
	
    [TestFixture]
	public class TestCharFilter:LuceneTestCase
	{
		
        [Test]
		public virtual void  TestCharFilter1()
		{
			CharStream cs = new CharFilter1(CharReader.Get(new System.IO.StringReader("")));
			Assert.AreEqual(1, cs.CorrectOffset(0), "corrected offset is invalid");
		}
		
        [Test]
		public virtual void  TestCharFilter2()
		{
			CharStream cs = new CharFilter2(CharReader.Get(new System.IO.StringReader("")));
			Assert.AreEqual(2, cs.CorrectOffset(0), "corrected offset is invalid");
		}
		
        [Test]
		public virtual void  TestCharFilter12()
		{
			CharStream cs = new CharFilter2(new CharFilter1(CharReader.Get(new System.IO.StringReader(""))));
			Assert.AreEqual(3, cs.CorrectOffset(0), "corrected offset is invalid");
		}
		
        [Test]
		public virtual void  TestCharFilter11()
		{
			CharStream cs = new CharFilter1(new CharFilter1(CharReader.Get(new System.IO.StringReader(""))));
			Assert.AreEqual(2, cs.CorrectOffset(0), "corrected offset is invalid");
		}
		
		internal class CharFilter1:CharFilter
		{
			
			protected internal CharFilter1(CharStream in_Renamed):base(in_Renamed)
			{
			}
			
			public /*protected internal*/ override int Correct(int currentOff)
			{
				return currentOff + 1;
			}
		}
		
		internal class CharFilter2:CharFilter
		{
			
			protected internal CharFilter2(CharStream in_Renamed):base(in_Renamed)
			{
			}
			
			public /*protected internal*/ override int Correct(int currentOff)
			{
				return currentOff + 2;
			}
		}
	}
}