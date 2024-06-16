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
using TermPositions = Lucene.Net.Index.TermPositions;

namespace Lucene.Net.Search
{
	
	/// <summary>Expert: Scoring functionality for phrase queries.
	/// <br/>A document is considered matching if it contains the phrase-query terms  
	/// at "valid" positons. What "valid positions" are
	/// depends on the type of the phrase query: for an exact phrase query terms are required 
	/// to appear in adjacent locations, while for a sloppy phrase query some distance between 
	/// the terms is allowed. The abstract method <see cref="PhraseFreq()" /> of extending classes
	/// is invoked for each document containing all the phrase query terms, in order to 
	/// compute the frequency of the phrase query in that document. A non zero frequency
	/// means a match. 
	/// </summary>
	abstract class PhraseScorer:Scorer
	{
		private Weight weight;
		protected internal byte[] norms;
		protected internal float value_Renamed;
		
		private bool firstTime = true;
		private bool more = true;
		protected internal PhraseQueue pq;
		protected internal PhrasePositions first, last;
		
		private float freq; //prhase frequency in current doc as computed by phraseFreq().
		
		internal PhraseScorer(Weight weight, TermPositions[] tps, int[] offsets, Similarity similarity, byte[] norms):base(similarity)
		{
			this.norms = norms;
			this.weight = weight;
			this.value_Renamed = weight.Value;
			
			// convert tps to a list of phrase positions.
			// note: phrase-position differs from term-position in that its position
			// reflects the phrase offset: pp.pos = tp.pos - offset.
			// this allows to easily identify a matching (exact) phrase 
			// when all PhrasePositions have exactly the same position.
			for (int i = 0; i < tps.Length; i++)
			{
				PhrasePositions pp = new PhrasePositions(tps[i], offsets[i]);
				if (last != null)
				{
					// add next to end of list
					last.next = pp;
				}
				else
				{
					first = pp;
				}
				last = pp;
			}
			
			pq = new PhraseQueue(tps.Length); // construct empty pq
			first.doc = - 1;
		}
		
		public override int DocID()
		{
			return first.doc;
		}
		
		public override int NextDoc(IState state)
		{
			if (firstTime)
			{
				Init(state);
				firstTime = false;
			}
			else if (more)
			{
				more = last.Next(state); // trigger further scanning
			}
			if (!DoNext(state))
			{
				first.doc = NO_MORE_DOCS;
			}
			return first.doc;
		}
		
		// next without initial increment
		private bool DoNext(IState state)
		{
			while (more)
			{
				while (more && first.doc < last.doc)
				{
					// find doc w/ all the terms
					more = first.SkipTo(last.doc, state); // skip first upto last
					FirstToLast(); // and move it to the end
				}
				
				if (more)
				{
					// found a doc with all of the terms
					freq = PhraseFreq(state); // check for phrase
					if (freq == 0.0f)
					// no match
						more = last.Next(state);
					// trigger further scanning
					else
						return true; // found a match
				}
			}
			return false; // no more matches
		}
		
		public override float Score(IState state)
		{
			//System.out.println("scoring " + first.doc);
			float raw = Similarity.Tf(freq) * value_Renamed; // raw score
			return norms == null?raw:raw * Similarity.DecodeNorm(norms[first.doc]); // normalize
		}
		
		public override int Advance(int target, IState state)
		{
			firstTime = false;
			for (PhrasePositions pp = first; more && pp != null; pp = pp.next)
			{
				more = pp.SkipTo(target, state);
			}
			if (more)
			{
				Sort(); // re-sort
			}
			if (!DoNext(state))
			{
				first.doc = NO_MORE_DOCS;
			}
			return first.doc;
		}

        /// <summary>
        /// Phrase frequency in current doc as computed by PhraseFreq()
        /// </summary>
        /// <returns></returns>
        public float CurrentFreq()
        {
            return freq;
        }

		/// <summary> For a document containing all the phrase query terms, compute the
		/// frequency of the phrase in that document. 
		/// A non zero frequency means a match.
		/// <br/>Note, that containing all phrase terms does not guarantee a match - they have to be found in matching locations.  
		/// </summary>
		/// <returns> frequency of the phrase in current doc, 0 if not found. 
		/// </returns>
		protected internal abstract float PhraseFreq(IState state);
		
		private void  Init(IState state)
		{
			for (PhrasePositions pp = first; more && pp != null; pp = pp.next)
			{
				more = pp.Next(state);
			}
			if (more)
			{
				Sort();
			}
		}
		
		private void  Sort()
		{
			pq.Clear();
			for (PhrasePositions pp = first; pp != null; pp = pp.next)
			{
				pq.Add(pp);
			}
			PqToList();
		}
		
		protected internal void  PqToList()
		{
			last = first = null;
			while (pq.Top() != null)
			{
				PhrasePositions pp = pq.Pop();
				if (last != null)
				{
					// add next to end of list
					last.next = pp;
				}
				else
					first = pp;
				last = pp;
				pp.next = null;
			}
		}
		
		protected internal void  FirstToLast()
		{
			last.next = first; // move first to end of list
			last = first;
			first = first.next;
			last.next = null;
		}
		
		public override System.String ToString()
		{
			return "scorer(" + weight + ")";
		}
	}
}