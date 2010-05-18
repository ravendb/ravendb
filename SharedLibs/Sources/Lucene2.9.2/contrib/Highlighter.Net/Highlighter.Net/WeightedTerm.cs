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

namespace Lucene.Net.Highlight
{
	/// <summary>Lightweight class to hold term and a weight value used for scoring this term </summary>
	/// <author>  Mark Harwood
	/// </author>
	public class WeightedTerm
	{
		internal float weight; // multiplier
		internal System.String term; //stemmed form
		public WeightedTerm(float weight, System.String term)
		{
			this.weight = weight;
			this.term = term;
		}
		
		
		/// <returns> the term value (stemmed)
		/// </returns>
		public virtual System.String GetTerm()
		{
			return term;
		}
		
		/// <returns> the weight associated with this term
		/// </returns>
		public virtual float GetWeight()
		{
			return weight;
		}
		
		/// <param name="term">the term value (stemmed)
		/// </param>
		public virtual void  SetTerm(System.String term)
		{
			this.term = term;
		}
		
		/// <param name="weight">the weight associated with this term
		/// </param>
		public virtual void  SetWeight(float weight)
		{
			this.weight = weight;
		}
	}
}