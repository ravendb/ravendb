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
	
	
	/// <summary> 
	/// 
	/// 
	/// </summary>
    [TestFixture]
	public class ArrayUtilTest:LuceneTestCase
	{
		
        [Test]
		public virtual void  TestParseInt()
		{
			int test;
			try
			{
				test = ArrayUtil.ParseInt("".ToCharArray());
				Assert.IsTrue(false);
			}
			catch (System.FormatException e)
			{
				//expected
			}
			try
			{
				test = ArrayUtil.ParseInt("foo".ToCharArray());
				Assert.IsTrue(false);
			}
			catch (System.FormatException e)
			{
				//expected
			}
			try
			{
				test = ArrayUtil.ParseInt(System.Convert.ToString(System.Int64.MaxValue).ToCharArray());
				Assert.IsTrue(false);
			}
			catch (System.FormatException e)
			{
				//expected
			}
			try
			{
				test = ArrayUtil.ParseInt("0.34".ToCharArray());
				Assert.IsTrue(false);
			}
			catch (System.FormatException e)
			{
				//expected
			}
			
			try
			{
				test = ArrayUtil.ParseInt("1".ToCharArray());
				Assert.IsTrue(test == 1, test + " does not equal: " + 1);
				test = ArrayUtil.ParseInt("-10000".ToCharArray());
				Assert.IsTrue(test == - 10000, test + " does not equal: " + (- 10000));
				test = ArrayUtil.ParseInt("1923".ToCharArray());
				Assert.IsTrue(test == 1923, test + " does not equal: " + 1923);
				test = ArrayUtil.ParseInt("-1".ToCharArray());
				Assert.IsTrue(test == - 1, test + " does not equal: " + (- 1));
				test = ArrayUtil.ParseInt("foo 1923 bar".ToCharArray(), 4, 4);
				Assert.IsTrue(test == 1923, test + " does not equal: " + 1923);
			}
			catch (System.FormatException e)
			{
				System.Console.Error.WriteLine(e.StackTrace);
				Assert.IsTrue(false);
			}
		}
	}
}