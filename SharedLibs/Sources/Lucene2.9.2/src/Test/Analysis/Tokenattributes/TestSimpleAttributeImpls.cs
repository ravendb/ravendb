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

using Payload = Lucene.Net.Index.Payload;
using AttributeImpl = Lucene.Net.Util.AttributeImpl;
using LuceneTestCase = Lucene.Net.Util.LuceneTestCase;

namespace Lucene.Net.Analysis.Tokenattributes
{
	
    [TestFixture]
	public class TestSimpleAttributeImpls:LuceneTestCase
	{
		
		public TestSimpleAttributeImpls():base("")
		{
		}
        
        [Test]
		public virtual void  TestFlagsAttribute()
		{
			FlagsAttributeImpl att = new FlagsAttributeImpl();
			Assert.AreEqual(0, att.GetFlags());
			
			att.SetFlags(1234);
			Assert.AreEqual("flags=1234", att.ToString());
			
			FlagsAttributeImpl att2 = (FlagsAttributeImpl) AssertCloneIsEqual(att);
			Assert.AreEqual(1234, att2.GetFlags());
			
			att2 = (FlagsAttributeImpl) AssertCopyIsEqual(att);
			Assert.AreEqual(1234, att2.GetFlags());
			
			att.Clear();
			Assert.AreEqual(0, att.GetFlags());
		}
		
        [Test]
		public virtual void  TestPositionIncrementAttribute()
		{
			PositionIncrementAttributeImpl att = new PositionIncrementAttributeImpl();
			Assert.AreEqual(1, att.GetPositionIncrement());
			
			att.SetPositionIncrement(1234);
			Assert.AreEqual("positionIncrement=1234", att.ToString());
			
			PositionIncrementAttributeImpl att2 = (PositionIncrementAttributeImpl) AssertCloneIsEqual(att);
			Assert.AreEqual(1234, att2.GetPositionIncrement());
			
			att2 = (PositionIncrementAttributeImpl) AssertCopyIsEqual(att);
			Assert.AreEqual(1234, att2.GetPositionIncrement());
			
			att.Clear();
			Assert.AreEqual(1, att.GetPositionIncrement());
		}
		
        [Test]
		public virtual void  TestTypeAttribute()
		{
			TypeAttributeImpl att = new TypeAttributeImpl();
			Assert.AreEqual(TypeAttributeImpl.DEFAULT_TYPE, att.Type());
			
			att.SetType("hallo");
			Assert.AreEqual("type=hallo", att.ToString());
			
			TypeAttributeImpl att2 = (TypeAttributeImpl) AssertCloneIsEqual(att);
			Assert.AreEqual("hallo", att2.Type());
			
			att2 = (TypeAttributeImpl) AssertCopyIsEqual(att);
			Assert.AreEqual("hallo", att2.Type());
			
			att.Clear();
			Assert.AreEqual(TypeAttributeImpl.DEFAULT_TYPE, att.Type());
		}
		
        [Test]
		public virtual void  TestPayloadAttribute()
		{
			PayloadAttributeImpl att = new PayloadAttributeImpl();
			Assert.IsNull(att.GetPayload());
			
			Payload pl = new Payload(new byte[]{1, 2, 3, 4});
			att.SetPayload(pl);
			
			PayloadAttributeImpl att2 = (PayloadAttributeImpl) AssertCloneIsEqual(att);
			Assert.AreEqual(pl, att2.GetPayload());
			Assert.AreNotSame(pl, att2.GetPayload());
			
			att2 = (PayloadAttributeImpl) AssertCopyIsEqual(att);
			Assert.AreEqual(pl, att2.GetPayload());
            Assert.AreNotSame(pl, att2.GetPayload());
			
			att.Clear();
			Assert.IsNull(att.GetPayload());
		}
		
        [Test]
		public virtual void  TestOffsetAttribute()
		{
			OffsetAttributeImpl att = new OffsetAttributeImpl();
			Assert.AreEqual(0, att.StartOffset());
			Assert.AreEqual(0, att.EndOffset());
			
			att.SetOffset(12, 34);
			// no string test here, because order unknown
			
			OffsetAttributeImpl att2 = (OffsetAttributeImpl) AssertCloneIsEqual(att);
			Assert.AreEqual(12, att2.StartOffset());
			Assert.AreEqual(34, att2.EndOffset());
			
			att2 = (OffsetAttributeImpl) AssertCopyIsEqual(att);
			Assert.AreEqual(12, att2.StartOffset());
			Assert.AreEqual(34, att2.EndOffset());
			
			att.Clear();
			Assert.AreEqual(0, att.StartOffset());
			Assert.AreEqual(0, att.EndOffset());
		}
		
		public static AttributeImpl AssertCloneIsEqual(AttributeImpl att)
		{
			AttributeImpl clone = (AttributeImpl) att.Clone();
			Assert.AreEqual(att, clone, "Clone must be equal");
			Assert.AreEqual(att.GetHashCode(), clone.GetHashCode(), "Clone's hashcode must be equal");
			return clone;
		}
		
		public static AttributeImpl AssertCopyIsEqual(AttributeImpl att)
		{
			AttributeImpl copy = (AttributeImpl) System.Activator.CreateInstance(att.GetType());
			att.CopyTo(copy);
			Assert.AreEqual(att, copy, "Copied instance must be equal");
			Assert.AreEqual(att.GetHashCode(), copy.GetHashCode(), "Copied instance's hashcode must be equal");
			return copy;
		}
	}
}