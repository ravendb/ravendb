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
	public class TestRamUsageEstimator
	{
		
        [Test]
		public virtual void  TestBasic()
		{
			System.String string_Renamed = new System.Text.StringBuilder("test str").ToString();
			RamUsageEstimator rue = new RamUsageEstimator();
			long size = rue.EstimateRamUsage(string_Renamed);
			System.Console.Out.WriteLine("size:" + size);
			
			string_Renamed = new System.Text.StringBuilder("test strin").ToString();
			size = rue.EstimateRamUsage(string_Renamed);
			System.Console.Out.WriteLine("size:" + size);
			
			Holder holder = new Holder();
			holder.holder = new Holder("string2", 5000L);
			size = rue.EstimateRamUsage(holder);
			System.Console.Out.WriteLine("size:" + size);
			
			System.String[] strings = new System.String[]{new System.Text.StringBuilder("test strin").ToString(), new System.Text.StringBuilder("hollow").ToString(), new System.Text.StringBuilder("catchmaster").ToString()};
			size = rue.EstimateRamUsage(strings);
			System.Console.Out.WriteLine("size:" + size);
		}
		
		private sealed class Holder
		{
			internal long field1 = 5000L;
			internal System.String name = "name";
			internal Holder holder;
			
			internal Holder()
			{
			}
			
			internal Holder(System.String name, long field1)
			{
				this.name = name;
				this.field1 = field1;
			}
		}
	}
}