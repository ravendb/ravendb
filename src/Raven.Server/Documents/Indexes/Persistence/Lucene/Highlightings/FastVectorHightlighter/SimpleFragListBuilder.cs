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
using WeightedPhraseInfo = Lucene.Net.Search.Vectorhighlight.FieldPhraseList.WeightedPhraseInfo;

namespace Lucene.Net.Search.Vectorhighlight
{
    /// <summary>
    /// A simple implementation of FragListBuilder.
    /// </summary>
    public class SimpleFragListBuilder : FragListBuilder
    {

        public static int MARGIN = 6;
        public static int MIN_FRAG_CHAR_SIZE = MARGIN * 3;

        public FieldFragList CreateFieldFragList(FieldPhraseList fieldPhraseList, int fragCharSize)
        {
            if (fragCharSize < MIN_FRAG_CHAR_SIZE)
                throw new ArgumentException("fragCharSize(" + fragCharSize + ") is too small. It must be " +
                    MIN_FRAG_CHAR_SIZE + " or higher.");

            FieldFragList ffl = new FieldFragList(fragCharSize);

            List<WeightedPhraseInfo> wpil = new List<WeightedPhraseInfo>();
            LinkedList<WeightedPhraseInfo>.Enumerator ite = fieldPhraseList.phraseList.GetEnumerator();

            WeightedPhraseInfo phraseInfo = null;
            int startOffset = 0;
            bool taken = false;
            while (true)
            {
                if (!taken)
                {
                    if (!ite.MoveNext()) break;
                    phraseInfo = ite.Current;
                }
                taken = false;
                if (phraseInfo == null) break;

                // if the phrase violates the border of previous fragment, discard it and try next phrase
                if (phraseInfo.StartOffset < startOffset)
                {
                    if(phraseInfo.EndOffset < startOffset)
                        continue;
                    startOffset = phraseInfo.StartOffset;
                }

                wpil.Clear();
                wpil.Add(phraseInfo);
                int st = phraseInfo.StartOffset - MARGIN < startOffset ?
                    startOffset : phraseInfo.StartOffset - MARGIN;
                int en = st + fragCharSize;
                if (phraseInfo.EndOffset > en)
                    en = phraseInfo.EndOffset;
                startOffset = en;

                while (true)
                {
                    if (ite.MoveNext())
                    {
                        phraseInfo = ite.Current;
                        taken = true;
                        if (phraseInfo == null) break;
                    }
                    else
                        break;
                    if (phraseInfo.EndOffset <= en)
                        wpil.Add(phraseInfo);
                    else
                        break;
                }
                ffl.Add(st, en, wpil);
            }
            return ffl;
        }

    }
}
