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
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Search.Highlight
{

    /// <summary> <see cref="IFragmenter"/> implementation which breaks text up into same-size 
    /// fragments with no concerns over spotting sentence boundaries.
    /// </summary>
    /// <author>  mark@searcharea.co.uk
    /// </author>
    public class SimpleFragmenter : IFragmenter
    {
        private static int DEFAULT_FRAGMENT_SIZE = 100;
        private int currentNumFrags;
        private int fragmentSize;
        private IOffsetAttribute offsetAtt;

        public SimpleFragmenter()
            : this(DEFAULT_FRAGMENT_SIZE)
        {
        }

        /*
         * 
         * @param fragmentSize size in number of characters of each fragment
         */

        public SimpleFragmenter(int fragmentSize)
        {
            this.fragmentSize = fragmentSize;
        }


        /* (non-Javadoc)
         * @see org.apache.lucene.search.highlight.Fragmenter#start(java.lang.String, org.apache.lucene.analysis.TokenStream)
         */

        public void Start(String originalText, TokenStream stream)
        {
            offsetAtt = stream.AddAttribute<IOffsetAttribute>();
            currentNumFrags = 1;
        }


        /* (non-Javadoc)
         * @see org.apache.lucene.search.highlight.Fragmenter#isNewFragment()
         */

        public bool IsNewFragment()
        {
            bool isNewFrag = offsetAtt.EndOffset >= (fragmentSize*currentNumFrags);
            if (isNewFrag)
            {
                currentNumFrags++;
            }
            return isNewFrag;
        }

        /// <summary>
        /// Gets or sets the size in number of characters of each fragment
        /// </summary>
        public int FragmentSize
        {
            get { return fragmentSize; }
            set { fragmentSize = value; }
        }
    }
}