/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Text;
using Toffs = Lucene.Net.Search.Vectorhighlight.FieldPhraseList.WeightedPhraseInfo.Toffs;
using WeightedPhraseInfo = Lucene.Net.Search.Vectorhighlight.FieldPhraseList.WeightedPhraseInfo;


namespace Lucene.Net.Search.Vectorhighlight
{
    ///<summary>
    /// FieldFragList has a list of "frag info" that is used by FragmentsBuilder class
    /// to create fragments (snippets).
    ///</summary>
    public class FieldFragList
    {
        private int fragCharSize;
        public List<WeightedFragInfo> fragInfos = new List<WeightedFragInfo>();

        
        /// <summary>
        /// a constructor.
        /// </summary>
        /// <param name="fragCharSize">the length (number of chars) of a fragment</param>
        public FieldFragList(int fragCharSize)
        {
            this.fragCharSize = fragCharSize;
        }
                
        /// <summary>
        /// convert the list of WeightedPhraseInfo to WeightedFragInfo, then add it to the fragInfos 
        /// </summary>
        /// <param name="startOffset">start offset of the fragment</param>
        /// <param name="endOffset">end offset of the fragment</param>
        /// <param name="phraseInfoList">list of WeightedPhraseInfo objects</param>
        public void Add(int startOffset, int endOffset, List<WeightedPhraseInfo> phraseInfoList)
        {
            fragInfos.Add(new WeightedFragInfo(startOffset, endOffset, phraseInfoList));
        }

        public class WeightedFragInfo
        {

            internal List<SubInfo> subInfos;
            internal float totalBoost;
            internal int startOffset;
            internal int endOffset;

            public WeightedFragInfo(int startOffset, int endOffset, List<WeightedPhraseInfo> phraseInfoList)
            {
                this.startOffset = startOffset;
                this.endOffset = endOffset;
                subInfos = new List<SubInfo>();
                foreach (WeightedPhraseInfo phraseInfo in phraseInfoList)
                {
                    SubInfo subInfo = new SubInfo(phraseInfo.text, phraseInfo.termsOffsets, phraseInfo.seqnum);
                    subInfos.Add(subInfo);
                    totalBoost += phraseInfo.boost;
                }
            }

            public override string ToString()
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("subInfos=(");
                foreach (SubInfo si in subInfos)
                    sb.Append(si.ToString());
                sb.Append(")/").Append(totalBoost.ToString(".0").Replace(System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator,".")).Append('(').Append(startOffset).Append(',').Append(endOffset).Append(')');
                return sb.ToString();
            }

            internal class SubInfo
            {
                internal String text;  // unnecessary member, just exists for debugging purpose
                internal List<Toffs> termsOffsets;   // usually termsOffsets.size() == 1,
                // but if position-gap > 1 and slop > 0 then size() could be greater than 1
                internal int seqnum;
                internal SubInfo(String text, List<Toffs> termsOffsets, int seqnum)
                {
                    this.text = text;
                    this.termsOffsets = termsOffsets;
                    this.seqnum = seqnum;
                }

                public override string ToString()
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append(text).Append('(');
                    foreach (Toffs to in termsOffsets)
                        sb.Append(to.ToString());
                    sb.Append(')');
                    return sb.ToString();
                }
            }
        }
    }
}
