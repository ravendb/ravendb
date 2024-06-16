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

namespace Lucene.Net.Index
{
	
	/// <summary>A TermInfo is the record of information stored for a term.</summary>
	
	public struct TermInfo
	{
		/// <summary>The number of documents which contain the term. </summary>
		internal int docFreq;
		
		internal long freqPointer;
		internal long proxPointer;
		internal int skipOffset;

        public bool IsEmpty => docFreq == 0 && freqPointer == 0 && proxPointer == 0 && skipOffset == 0;

		internal TermInfo(int df, long fp, long pp)
		{
			docFreq = df;
			freqPointer = fp;
			proxPointer = pp;
            skipOffset = 0;
        }
		
		internal TermInfo(TermInfo ti)
		{
			docFreq = ti.docFreq;
			freqPointer = ti.freqPointer;
			proxPointer = ti.proxPointer;
			skipOffset = ti.skipOffset;
		}
		
		internal void  Set(int docFreq, long freqPointer, long proxPointer, int skipOffset)
		{
			this.docFreq = docFreq;
			this.freqPointer = freqPointer;
			this.proxPointer = proxPointer;
			this.skipOffset = skipOffset;
		}
		
		internal void  Set(TermInfo ti)
		{
			docFreq = ti.docFreq;
			freqPointer = ti.freqPointer;
			proxPointer = ti.proxPointer;
			skipOffset = ti.skipOffset;
		}
	}
}