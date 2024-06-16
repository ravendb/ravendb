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

using FieldInvertState = Lucene.Net.Index.FieldInvertState;

namespace Lucene.Net.Search
{
    /// <summary>Expert: Delegating scoring implementation.  Useful in <see cref="Query.GetSimilarity(Searcher)" />
    /// implementations, to override only certain
    /// methods of a Searcher's Similiarty implementation.. 
    /// </summary>

        [Serializable]
    public class SimilarityDelegator:Similarity
	{
		private Similarity delegee;
		
		/// <summary>Construct a <see cref="Similarity" /> that delegates all methods to another.</summary>
		/// <param name="delegee">the Similarity implementation to delegate to</param>
		public SimilarityDelegator(Similarity delegee)
		{
			this.delegee = delegee;
		}
		
		public override float ComputeNorm(System.String fieldName, FieldInvertState state)
		{
			return delegee.ComputeNorm(fieldName, state);
		}
		
		public override float LengthNorm(System.String fieldName, int numTerms)
		{
			return delegee.LengthNorm(fieldName, numTerms);
		}
		
		public override float QueryNorm(float sumOfSquaredWeights)
		{
			return delegee.QueryNorm(sumOfSquaredWeights);
		}
		
		public override float Tf(float freq)
		{
			return delegee.Tf(freq);
		}
		
		public override float SloppyFreq(int distance)
		{
			return delegee.SloppyFreq(distance);
		}
		
		public override float Idf(int docFreq, int numDocs)
		{
			return delegee.Idf(docFreq, numDocs);
		}
		
		public override float Coord(int overlap, int maxOverlap)
		{
			return delegee.Coord(overlap, maxOverlap);
		}

        public override float ScorePayload(int docId, string fieldName, int start, int end, byte[] payload, int offset, int length)
		{
            return delegee.ScorePayload(docId, fieldName, start, end, payload, offset, length);
		}
	}
}