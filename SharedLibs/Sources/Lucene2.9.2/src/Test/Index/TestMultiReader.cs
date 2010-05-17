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

namespace Lucene.Net.Index
{
	
    [TestFixture]
	public class TestMultiReader:TestDirectoryReader
	{
		public TestMultiReader(System.String s):base(s)
		{
		}
        public TestMultiReader()
            : base()
        {
        }
		
		protected internal override IndexReader OpenReader()
		{
			IndexReader reader;
			
			sis.Read(dir);
			SegmentReader reader1 = SegmentReader.Get(sis.Info(0));
			SegmentReader reader2 = SegmentReader.Get(sis.Info(1));
			readers[0] = reader1;
			readers[1] = reader2;
			Assert.IsTrue(reader1 != null);
			Assert.IsTrue(reader2 != null);
			
			reader = new MultiReader(readers);
			
			Assert.IsTrue(dir != null);
			Assert.IsTrue(sis != null);
			Assert.IsTrue(reader != null);
			
			return reader;
		}
	}
}