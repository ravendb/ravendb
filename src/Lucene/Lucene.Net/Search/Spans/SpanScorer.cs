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
using Explanation = Lucene.Net.Search.Explanation;
using Scorer = Lucene.Net.Search.Scorer;
using Similarity = Lucene.Net.Search.Similarity;
using Weight = Lucene.Net.Search.Weight;

namespace Lucene.Net.Search.Spans
{
	/// <summary> Public for extension only.</summary>
	public class SpanScorer:Scorer
	{
		protected internal Spans spans;
		protected internal Weight weight;
		protected internal byte[] norms;
		protected internal float value_Renamed;
		
		protected internal bool more = true;
		
		protected internal int doc;
		protected internal float freq;
		
		protected internal SpanScorer(Spans spans, Weight weight, Similarity similarity, byte[] norms, IState state):base(similarity)
		{
			this.spans = spans;
			this.norms = norms;
			this.weight = weight;
			this.value_Renamed = weight.Value;
			if (this.spans.Next(state))
			{
				doc = - 1;
			}
			else
			{
				doc = NO_MORE_DOCS;
				more = false;
			}
		}
		
		public override int NextDoc(IState state)
		{
			if (!SetFreqCurrentDoc(state))
			{
				doc = NO_MORE_DOCS;
			}
			return doc;
		}
		
		public override int Advance(int target, IState state)
		{
			if (!more)
			{
				return doc = NO_MORE_DOCS;
			}
			if (spans.Doc() < target)
			{
				// setFreqCurrentDoc() leaves spans.doc() ahead
				more = spans.SkipTo(target, state);
			}
			if (!SetFreqCurrentDoc(state))
			{
				doc = NO_MORE_DOCS;
			}
			return doc;
		}
		
		public /*protected internal*/ virtual bool SetFreqCurrentDoc(IState state)
		{
			if (!more)
			{
				return false;
			}
			doc = spans.Doc();
			freq = 0.0f;
			do 
			{
				int matchLength = spans.End() - spans.Start();
				freq += Similarity.SloppyFreq(matchLength);
				more = spans.Next(state);
			}
			while (more && (doc == spans.Doc()));
			return true;
		}
		
		public override int DocID()
		{
			return doc;
		}
		
		public override float Score(IState state)
		{
			float raw = Similarity.Tf(freq) * value_Renamed; // raw score
			return norms == null?raw:raw * Similarity.DecodeNorm(norms[doc]); // normalize
		}
		
        /// <summary>
        /// This method is no longer an official member of <see cref="Scorer"/>
        /// but it is needed by SpanWeight to build an explanation.
        /// </summary>
		protected internal virtual Explanation Explain(int doc, IState state)
		{
			Explanation tfExplanation = new Explanation();
			
			int expDoc = Advance(doc, state);
			
			float phraseFreq = (expDoc == doc)?freq:0.0f;
			tfExplanation.Value = Similarity.Tf(phraseFreq);
			tfExplanation.Description = "tf(phraseFreq=" + phraseFreq + ")";
			
			return tfExplanation;
		}
	}
}