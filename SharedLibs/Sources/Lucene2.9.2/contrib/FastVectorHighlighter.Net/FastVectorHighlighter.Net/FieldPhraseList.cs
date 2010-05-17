/**
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

using Lucene.Net.Documents;
using Lucene.Net.Search;
using Lucene.Net.Index;

using TermInfo = Lucene.Net.Search.Vectorhighlight.FieldTermStack.TermInfo;
using QueryPhraseMap = Lucene.Net.Search.Vectorhighlight.FieldQuery.QueryPhraseMap;

namespace Lucene.Net.Search.Vectorhighlight
{
    /// <summary>
    /// FieldPhraseList has a list of WeightedPhraseInfo that is used by FragListBuilder
    /// to create a FieldFragList object.
    /// </summary>
    public class FieldPhraseList
    {
        public LinkedList<WeightedPhraseInfo> phraseList = new LinkedList<WeightedPhraseInfo>();
        
        /// <summary>
        /// a constructor. 
        /// </summary>
        /// <param name="fieldTermStack">FieldTermStack object</param>
        /// <param name="fieldQuery">FieldQuery object</param>
        public FieldPhraseList(FieldTermStack fieldTermStack, FieldQuery fieldQuery)
        {
            String field = fieldTermStack.GetFieldName();

            LinkedList<TermInfo> phraseCandidate = new LinkedList<TermInfo>();
            QueryPhraseMap currMap = null;
            QueryPhraseMap nextMap = null;
            while (!fieldTermStack.IsEmpty())
            {

                phraseCandidate.Clear();

                TermInfo ti = fieldTermStack.Pop();
                currMap = fieldQuery.GetFieldTermMap(field, ti.GetText());

                // if not found, discard top TermInfo from stack, then try next element
                if (currMap == null) continue;

                // if found, search the longest phrase
                phraseCandidate.AddLast(ti);
                while (true)
                {
                    ti = fieldTermStack.Pop();
                    nextMap = null;
                    if (ti != null)
                        nextMap = currMap.GetTermMap(ti.GetText());
                    if (ti == null || nextMap == null)
                    {
                        if (ti != null)
                            fieldTermStack.Push(ti);
                        if (currMap.IsValidTermOrPhrase(new List<TermInfo>(phraseCandidate)))
                        {
                            AddIfNoOverlap(new WeightedPhraseInfo(phraseCandidate, currMap.GetBoost(), currMap.GetTermOrPhraseNumber()));
                        }
                        else
                        {
                            while (phraseCandidate.Count > 1)
                            {
                                TermInfo last = phraseCandidate.Last.Value;
                                phraseCandidate.RemoveLast();
                                fieldTermStack.Push(last);
                                currMap = fieldQuery.SearchPhrase(field, new List<TermInfo>(phraseCandidate));
                                if (currMap != null)
                                {
                                    AddIfNoOverlap(new WeightedPhraseInfo(phraseCandidate, currMap.GetBoost(), currMap.GetTermOrPhraseNumber()));
                                    break;
                                }
                            }
                        }
                        break;
                    }
                    else
                    {
                        phraseCandidate.AddLast(ti);
                        currMap = nextMap;
                    }
                }
            }
        }

        void AddIfNoOverlap(WeightedPhraseInfo wpi)
        {
            foreach (WeightedPhraseInfo existWpi in phraseList)
            {
                if (existWpi.IsOffsetOverlap(wpi)) return;
            }
            phraseList.AddLast(wpi);
        }

        public class WeightedPhraseInfo
        {

            internal String text;  // unnecessary member, just exists for debugging purpose
            internal List<Toffs> termsOffsets;   // usually termsOffsets.size() == 1,
            // but if position-gap > 1 and slop > 0 then size() could be greater than 1
            internal float boost;  // query boost
            internal int seqnum;

            public WeightedPhraseInfo(LinkedList<TermInfo> terms, float boost):  this(terms, boost, 0)
            {
            }

            public WeightedPhraseInfo(LinkedList<TermInfo> terms, float boost, int number)
            {
                this.boost = boost;
                this.seqnum = number;
                termsOffsets = new List<Toffs>(terms.Count);
                TermInfo ti = terms.First.Value;
                termsOffsets.Add(new Toffs(ti.GetStartOffset(), ti.GetEndOffset()));
                if (terms.Count == 1)
                {
                    text = ti.GetText();
                    return;
                }
                StringBuilder sb = new StringBuilder();
                sb.Append(ti.GetText());
                int pos = ti.GetPosition();

                bool dummy = true;
                foreach(TermInfo ti2 in terms)
                //for (int i = 1; i < terms.Count; i++)
                {
                    if (dummy) { dummy = false; continue; } //Skip First Item {{DIGY}}
                    ti = ti2;
                    //ti = terms.get(i);
                    sb.Append(ti.GetText());
                    if (ti.GetPosition() - pos == 1)
                    {
                        Toffs to = termsOffsets[termsOffsets.Count - 1];
                        to.SetEndOffset(ti.GetEndOffset());
                    }
                    else
                    {
                        termsOffsets.Add(new Toffs(ti.GetStartOffset(), ti.GetEndOffset()));
                    }
                    pos = ti.GetPosition();
                }
                text = sb.ToString();
            }

            public int GetStartOffset()
            {
                return termsOffsets[0].startOffset;
            }

            public int GetEndOffset()
            {
                return termsOffsets[termsOffsets.Count - 1].endOffset;
            }

            public bool IsOffsetOverlap(WeightedPhraseInfo other)
            {
                int so = GetStartOffset();
                int eo = GetEndOffset();
                int oso = other.GetStartOffset();
                int oeo = other.GetEndOffset();
                if (so <= oso && oso < eo) return true;
                if (so < oeo && oeo <= eo) return true;
                if (oso <= so && so < oeo) return true;
                if (oso < eo && eo <= oeo) return true;
                return false;
            }

            public override string ToString()
            {
                StringBuilder sb = new StringBuilder();
                
                sb.Append(text).Append('(').Append(boost.ToString(".0").Replace(System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator,".")).Append(")(");
                foreach (Toffs to in termsOffsets)
                {
                    sb.Append(to);
                }
                sb.Append(')');
                return sb.ToString();
            }

            public class Toffs
            {
                internal int startOffset;
                internal int endOffset;
                public Toffs(int startOffset, int endOffset)
                {
                    this.startOffset = startOffset;
                    this.endOffset = endOffset;
                }
                internal void SetEndOffset(int endOffset)
                {
                    this.endOffset = endOffset;
                }
                public override string ToString()
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append('(').Append(startOffset).Append(',').Append(endOffset).Append(')');
                    return sb.ToString();
                }
            }
        }
    }
}
