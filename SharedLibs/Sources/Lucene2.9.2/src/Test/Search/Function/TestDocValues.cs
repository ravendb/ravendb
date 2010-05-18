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

namespace Lucene.Net.Search.Function
{
	
	/// <summary> DocValues TestCase  </summary>
    [TestFixture]
    public class TestDocValues : LuceneTestCase
	{
		
		/* @override constructor */
		public TestDocValues(System.String name):base(name)
		{
		}
        public TestDocValues()
            : base()
        {
        }
        [Test]
		public virtual void  TestGetMinValue()
		{
			float[] innerArray = new float[]{1.0f, 2.0f, - 1.0f, 100.0f};
			DocValuesTestImpl docValues = new DocValuesTestImpl(innerArray);
			Assert.AreEqual(- 1.0f, docValues.GetMinValue(), 0, "-1.0f is the min value in the source array");
			
			// test with without values - NaN
			innerArray = new float[]{};
			docValues = new DocValuesTestImpl(innerArray);
			Assert.IsTrue(System.Single.IsNaN(docValues.GetMinValue()), "max is NaN - no values in inner array");
		}
		
        [Test]
		public virtual void  TestGetMaxValue()
		{
			float[] innerArray = new float[]{1.0f, 2.0f, - 1.0f, 10.0f};
			DocValuesTestImpl docValues = new DocValuesTestImpl(innerArray);
			Assert.AreEqual(10.0f, docValues.GetMaxValue(), 0, "10.0f is the max value in the source array");
			
			innerArray = new float[]{- 3.0f, - 1.0f, - 100.0f};
			docValues = new DocValuesTestImpl(innerArray);
			Assert.AreEqual(- 1.0f, docValues.GetMaxValue(), 0, "-1.0f is the max value in the source array");
			
			innerArray = new float[]{- 3.0f, - 1.0f, 100.0f, System.Single.MaxValue, System.Single.MaxValue - 1};
			docValues = new DocValuesTestImpl(innerArray);
			Assert.AreEqual(System.Single.MaxValue, docValues.GetMaxValue(), 0, System.Single.MaxValue + " is the max value in the source array");
			
			// test with without values - NaN
			innerArray = new float[]{};
			docValues = new DocValuesTestImpl(innerArray);
			Assert.IsTrue(System.Single.IsNaN(docValues.GetMaxValue()), "max is NaN - no values in inner array");
		}
		
        [Test]
		public virtual void  TestGetAverageValue()
		{
			float[] innerArray = new float[]{1.0f, 1.0f, 1.0f, 1.0f};
			DocValuesTestImpl docValues = new DocValuesTestImpl(innerArray);
			Assert.AreEqual(1.0f, docValues.GetAverageValue(), 0, "the average is 1.0f");
			
			innerArray = new float[]{1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f};
			docValues = new DocValuesTestImpl(innerArray);
			Assert.AreEqual(3.5f, docValues.GetAverageValue(), 0, "the average is 3.5f");
			
			// test with negative values
			innerArray = new float[]{- 1.0f, 2.0f};
			docValues = new DocValuesTestImpl(innerArray);
			Assert.AreEqual(0.5f, docValues.GetAverageValue(), 0, "the average is 0.5f");
			
			// test with without values - NaN
			innerArray = new float[]{};
			docValues = new DocValuesTestImpl(innerArray);
			Assert.IsTrue(System.Single.IsNaN(docValues.GetAverageValue()), "the average is NaN - no values in inner array");
		}
		
		internal class DocValuesTestImpl:DocValues
		{
			internal float[] innerArray;
			
			internal DocValuesTestImpl(float[] innerArray)
			{
				this.innerArray = innerArray;
			}
			
			/// <seealso cref="Lucene.Net.Search.Function.DocValues.FloatVal(int)">
			/// </seealso>
			/* @Override */
			public override float FloatVal(int doc)
			{
				return innerArray[doc];
			}
			
			/// <seealso cref="Lucene.Net.Search.Function.DocValues.ToString(int)">
			/// </seealso>
			/* @Override */
			public override System.String ToString(int doc)
			{
				return System.Convert.ToString(doc);
			}
		}
	}
}