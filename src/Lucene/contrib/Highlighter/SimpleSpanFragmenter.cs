/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Search.Highlight
{
    public class SimpleSpanFragmenter : IFragmenter
    {
        private static int DEFAULT_FRAGMENT_SIZE = 100;
        private int fragmentSize;
        private int currentNumFrags;
        private int position = -1;
        private QueryScorer queryScorer;
        private int waitForPos = -1;
        private int textSize;
        private ITermAttribute termAtt;
        private IPositionIncrementAttribute posIncAtt;
        private IOffsetAttribute offsetAtt;

        /// <param name="queryScorer">QueryScorer that was used to score hits</param>
        public SimpleSpanFragmenter(QueryScorer queryScorer)
            : this(queryScorer, DEFAULT_FRAGMENT_SIZE)
        {

        }

        /// <param name="queryScorer">QueryScorer that was used to score hits</param>
        /// <param name="fragmentSize">size in bytes of each fragment</param>
        public SimpleSpanFragmenter(QueryScorer queryScorer, int fragmentSize)
        {
            this.fragmentSize = fragmentSize;
            this.queryScorer = queryScorer;
        }

        /// <seealso cref="IFragmenter.IsNewFragment"/>
        public bool IsNewFragment()
        {
            position += posIncAtt.PositionIncrement;

            if (waitForPos == position)
            {
                waitForPos = -1;
            }
            else if (waitForPos != -1)
            {
                return false;
            }

            WeightedSpanTerm wSpanTerm = queryScorer.GetWeightedSpanTerm(termAtt.Term);

            if (wSpanTerm != null)
            {
                List<PositionSpan> positionSpans = wSpanTerm.GetPositionSpans();

                for (int i = 0; i < positionSpans.Count; i++)
                {
                    if (positionSpans[i].Start == position)
                    {
                        waitForPos = positionSpans[i].End + 1;
                        break;
                    }
                }
            }

            bool isNewFrag = offsetAtt.EndOffset >= (fragmentSize*currentNumFrags)
                             && (textSize - offsetAtt.EndOffset) >= ((uint) fragmentSize >> 1);


            if (isNewFrag)
            {
                currentNumFrags++;
            }

            return isNewFrag;
        }

        /// <seealso cref="IFragmenter.Start(string, TokenStream)"/>
        public void Start(String originalText, TokenStream tokenStream)
        {
            position = -1;
            currentNumFrags = 1;
            textSize = originalText.Length;
            termAtt = tokenStream.AddAttribute<ITermAttribute>();
            posIncAtt = tokenStream.AddAttribute<IPositionIncrementAttribute>();
            offsetAtt = tokenStream.AddAttribute<IOffsetAttribute>();
        }
    }
}
