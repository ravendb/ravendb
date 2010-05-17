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

namespace Lucene.Net.Analysis
{
	
	/// <summary> Base utility class for implementing a {@link CharFilter}.
	/// You subclass this, and then record mappings by calling
	/// {@link #addOffCorrectMap}, and then invoke the correct
	/// method to correct an offset.
	/// 
	/// <p/><b>NOTE</b>: This class is not particularly efficient.
	/// For example, a new class instance is created for every
	/// call to {@link #addOffCorrectMap}, which is then appended
	/// to a private list.
	/// </summary>
	public abstract class BaseCharFilter:CharFilter
	{
		
		//private List<OffCorrectMap> pcmList;
		private System.Collections.IList pcmList;
		
		public BaseCharFilter(CharStream in_Renamed):base(in_Renamed)
		{
		}
		
		/// <summary>Retrieve the corrected offset.  Note that this method
		/// is slow, if you correct positions far before the most
		/// recently added position, as it's a simple linear
		/// search backwards through all offset corrections added
		/// by {@link #addOffCorrectMap}.
		/// </summary>
		public /*protected internal*/ override int Correct(int currentOff)
		{
			if (pcmList == null || (pcmList.Count == 0))
			{
				return currentOff;
			}
			for (int i = pcmList.Count - 1; i >= 0; i--)
			{
				if (currentOff >= ((OffCorrectMap) pcmList[i]).off)
				{
					return currentOff + ((OffCorrectMap) pcmList[i]).cumulativeDiff;
				}
			}
			return currentOff;
		}
		
		protected internal virtual int GetLastCumulativeDiff()
		{
			return pcmList == null || (pcmList.Count == 0)?0:((OffCorrectMap) pcmList[pcmList.Count - 1]).cumulativeDiff;
		}
		
		protected internal virtual void  AddOffCorrectMap(int off, int cumulativeDiff)
		{
			if (pcmList == null)
			{
				pcmList = new System.Collections.ArrayList();
			}
			pcmList.Add(new OffCorrectMap(off, cumulativeDiff));
		}
		
		internal class OffCorrectMap
		{
			
			internal int off;
			internal int cumulativeDiff;
			
			internal OffCorrectMap(int off, int cumulativeDiff)
			{
				this.off = off;
				this.cumulativeDiff = cumulativeDiff;
			}
			
			public override System.String ToString()
			{
				System.Text.StringBuilder sb = new System.Text.StringBuilder();
				sb.Append('(');
				sb.Append(off);
				sb.Append(',');
				sb.Append(cumulativeDiff);
				sb.Append(')');
				return sb.ToString();
			}
		}
	}
}