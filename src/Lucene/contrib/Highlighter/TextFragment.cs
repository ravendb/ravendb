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
using System.Text;

namespace Lucene.Net.Search.Highlight
{
    /// <summary> Low-level class used to record information about a section of a document 
    /// with a score.
    /// </summary>
    public class TextFragment
    {
        private StringBuilder markedUpText;


        public TextFragment(StringBuilder markedUpText, int textStartPos, int fragNum)
        {
            this.markedUpText = markedUpText;
            this.TextStartPos = textStartPos;
            this.FragNum = fragNum;
        }

        public float Score { get; protected internal set; }
        public int TextEndPos { get; protected internal set; }
        public int TextStartPos { get; protected internal set; }

        /// <summary>
        /// the fragment sequence number
        /// </summary>
        public int FragNum { get; protected internal set; }


        /// <summary></summary>
        /// <param name="frag2">Fragment to be merged into this one</param>
        public void Merge(TextFragment frag2)
        {
            TextEndPos = frag2.TextEndPos;
            Score = Math.Max(Score, frag2.Score);
        }

        /// <summary>
        /// true if this fragment follows the one passed
        /// </summary>
        public bool Follows(TextFragment fragment)
        {
            return TextStartPos == fragment.TextEndPos;
        }

        /// <summary>
        /// Returns the marked-up text for this text fragment 
        /// </summary>
        public override String ToString()
        {
            return markedUpText.ToString(TextStartPos, TextEndPos - TextStartPos);
        }

    }
}