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
	public class TestPositionBasedTermVectorMapper:LuceneTestCase
	{
		protected internal System.String[] tokens;
		protected internal int[][] thePositions;
		protected internal TermVectorOffsetInfo[][] offsets;
		protected internal int numPositions;
		
		
		public TestPositionBasedTermVectorMapper(System.String s):base(s)
		{
		}

        public TestPositionBasedTermVectorMapper() : base("")
        {
        }
		
		[SetUp]
		public override void  SetUp()
		{
			base.SetUp();
			tokens = new System.String[]{"here", "is", "some", "text", "to", "test", "extra"};
			thePositions = new int[tokens.Length][];
			offsets = new TermVectorOffsetInfo[tokens.Length][];
			numPositions = 0;
			//save off the last one so we can add it with the same positions as some of the others, but in a predictable way
			for (int i = 0; i < tokens.Length - 1; i++)
			{
				thePositions[i] = new int[2 * i + 1]; //give 'em all some positions
				for (int j = 0; j < thePositions[i].Length; j++)
				{
					thePositions[i][j] = numPositions++;
				}
				offsets[i] = new TermVectorOffsetInfo[thePositions[i].Length];
				for (int j = 0; j < offsets[i].Length; j++)
				{
					offsets[i][j] = new TermVectorOffsetInfo(j, j + 1); //the actual value here doesn't much matter
				}
			}
			thePositions[tokens.Length - 1] = new int[1];
			thePositions[tokens.Length - 1][0] = 0; //put this at the same position as "here"
			offsets[tokens.Length - 1] = new TermVectorOffsetInfo[1];
			offsets[tokens.Length - 1][0] = new TermVectorOffsetInfo(0, 1);
		}
		
		[Test]
		public virtual void  Test()
		{
			PositionBasedTermVectorMapper mapper = new PositionBasedTermVectorMapper();
			
			mapper.SetExpectations("test", tokens.Length, true, true);
			//Test single position
			for (int i = 0; i < tokens.Length; i++)
			{
				System.String token = tokens[i];
				mapper.Map(token, 1, null, thePositions[i]);
			}
			System.Collections.IDictionary map = mapper.GetFieldToTerms();
			Assert.IsTrue(map != null, "map is null and it shouldn't be");
			Assert.IsTrue(map.Count == 1, "map Size: " + map.Count + " is not: " + 1);
			System.Collections.IDictionary positions = (System.Collections.IDictionary) map["test"];
			Assert.IsTrue(positions != null, "thePositions is null and it shouldn't be");
			
			Assert.IsTrue(positions.Count == numPositions, "thePositions Size: " + positions.Count + " is not: " + numPositions);
			System.Collections.BitArray bits = new System.Collections.BitArray((numPositions % 64 == 0?numPositions / 64:numPositions / 64 + 1) * 64);
			for (System.Collections.IEnumerator iterator = positions.GetEnumerator(); iterator.MoveNext(); )
			{
				System.Collections.DictionaryEntry entry = (System.Collections.DictionaryEntry) iterator.Current;
				PositionBasedTermVectorMapper.TVPositionInfo info = (PositionBasedTermVectorMapper.TVPositionInfo) entry.Value;
				Assert.IsTrue(info != null, "info is null and it shouldn't be");
				int pos = ((System.Int32) entry.Key);
				bits.Set(pos, true);
				Assert.IsTrue(info.Position == pos, info.Position + " does not equal: " + pos);
				Assert.IsTrue(info.Offsets != null, "info.getOffsets() is null and it shouldn't be");
				if (pos == 0)
				{
					Assert.IsTrue(info.Terms.Count == 2, "info.getTerms() Size: " + info.Terms.Count + " is not: " + 2); //need a test for multiple terms at one pos
					Assert.IsTrue(info.Offsets.Count == 2, "info.getOffsets() Size: " + info.Offsets.Count + " is not: " + 2);
				}
				else
				{
					Assert.IsTrue(info.Terms.Count == 1, "info.getTerms() Size: " + info.Terms.Count + " is not: " + 1); //need a test for multiple terms at one pos
					Assert.IsTrue(info.Offsets.Count == 1, "info.getOffsets() Size: " + info.Offsets.Count + " is not: " + 1);
				}
			}
			Assert.IsTrue(SupportClass.BitSetSupport.Cardinality(bits) == numPositions, "Bits are not all on");
		}
    }
}