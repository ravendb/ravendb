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
using Lucene.Net.Store;
using TermDocs = Lucene.Net.Index.TermDocs;

namespace Lucene.Net.Search
{
	
	/// <summary>Expert: A <c>Scorer</c> for documents matching a <c>Term</c>.</summary>
	public sealed class TermScorer:Scorer
	{
		
		private static readonly float[] SIM_NORM_DECODER;
		
		private Weight weight;
		private TermDocs termDocs;
		private byte[] norms;
		private float weightValue;
		private int doc = - 1;
		
		private int[] docs = new int[32]; // buffered doc numbers
		private int[] freqs = new int[32]; // buffered term freqs
		private int pointer;
		private int pointerMax;
		
		private const int SCORE_CACHE_SIZE = 32;
		private float[] scoreCache = new float[SCORE_CACHE_SIZE];
		
		/// <summary> Construct a <c>TermScorer</c>.
		/// 
		/// </summary>
		/// <param name="weight">The weight of the <c>Term</c> in the query.
		/// </param>
		/// <param name="td">An iterator over the documents matching the <c>Term</c>.
		/// </param>
		/// <param name="similarity">The <c>Similarity</c> implementation to be used for score
		/// computations.
		/// </param>
		/// <param name="norms">The field norms of the document fields for the <c>Term</c>.
		/// </param>
		public /*internal*/ TermScorer(Weight weight, TermDocs td, Similarity similarity, byte[] norms):base(similarity)
		{
			this.weight = weight;
			this.termDocs = td;
			this.norms = norms;
			this.weightValue = weight.Value;
			
			for (int i = 0; i < SCORE_CACHE_SIZE; i++)
				scoreCache[i] = Similarity.Tf(i) * weightValue;
		}
		
		public override void  Score(Collector c, IState state)
		{
			Score(c, System.Int32.MaxValue, NextDoc(state), state);
		}
		
		// firstDocID is ignored since nextDoc() sets 'doc'
		public /*protected internal*/ override bool Score(Collector c, int end, int firstDocID, IState state)
		{
			c.SetScorer(this);
			while (doc < end)
			{
				// for docs in window
				c.Collect(doc, state); // collect score
				
				if (++pointer >= pointerMax)
				{
					pointerMax = termDocs.Read(docs, freqs, state); // refill buffers
					if (pointerMax != 0)
					{
						pointer = 0;
					}
					else
					{
						termDocs.Close(); // close stream
						doc = System.Int32.MaxValue; // set to sentinel value
						return false;
					}
				}
				doc = docs[pointer];
			}
			return true;
		}
		
		public override int DocID()
		{
			return doc;
		}
		
		/// <summary> Advances to the next document matching the query. <br/>
		/// The iterator over the matching documents is buffered using
		/// <see cref="TermDocs.Read(int[],int[])" />.
		/// 
		/// </summary>
		/// <returns> the document matching the query or -1 if there are no more documents.
		/// </returns>
		public override int NextDoc(IState state)
		{
			pointer++;
			if (pointer >= pointerMax)
			{
				pointerMax = termDocs.Read(docs, freqs, state); // refill buffer
				if (pointerMax != 0)
				{
					pointer = 0;
				}
				else
				{
					termDocs.Close(); // close stream
					return doc = NO_MORE_DOCS;
				}
			}
			doc = docs[pointer];
			return doc;
		}
		
		public override float Score(IState state)
		{
			System.Diagnostics.Debug.Assert(doc != - 1);
			int f = freqs[pointer];
			float raw = f < SCORE_CACHE_SIZE?scoreCache[f]:Similarity.Tf(f) * weightValue; // cache miss
			
			return norms == null?raw:raw * SIM_NORM_DECODER[norms[doc] & 0xFF]; // normalize for field
		}
		
		/// <summary> Advances to the first match beyond the current whose document number is
		/// greater than or equal to a given target. <br/>
		/// The implementation uses <see cref="TermDocs.SkipTo(int)" />.
		/// 
		/// </summary>
		/// <param name="target">The target document number.
		/// </param>
		/// <returns> the matching document or -1 if none exist.
		/// </returns>
		public override int Advance(int target, IState state)
		{
			// first scan in cache
			for (pointer++; pointer < pointerMax; pointer++)
			{
				if (docs[pointer] >= target)
				{
					return doc = docs[pointer];
				}
			}
			
			// not found in cache, seek underlying stream
			bool result = termDocs.SkipTo(target, state);
			if (result)
			{
				pointerMax = 1;
				pointer = 0;
				docs[pointer] = doc = termDocs.Doc;
				freqs[pointer] = termDocs.Freq;
			}
			else
			{
				doc = NO_MORE_DOCS;
			}
			return doc;
		}
		
		/// <summary>Returns a string representation of this <c>TermScorer</c>. </summary>
		public override System.String ToString()
		{
			return "scorer(" + weight + ")";
		}
		static TermScorer()
		{
			SIM_NORM_DECODER = Search.Similarity.GetNormDecoder();
		}
	}
}