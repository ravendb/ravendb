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
using Lucene.Net.Analysis.Tokenattributes;

namespace Lucene.Net.Analysis.Position
{
    /* Set the positionIncrement of all tokens to the "positionIncrement",
     * except the first return token which retains its original positionIncrement value.
     * The default positionIncrement value is zero.
     */
    public sealed class PositionFilter : TokenFilter
    {

        /* Position increment to assign to all but the first token - default = 0 */
        private int positionIncrement = 0;

        /* The first token must have non-zero positionIncrement **/
        private bool firstTokenPositioned = false;

        private IPositionIncrementAttribute posIncrAtt;

        /*
         * Constructs a PositionFilter that assigns a position increment of zero to
         * all but the first token from the given input stream.
         * 
         * @param input the input stream
         */
        public PositionFilter(TokenStream input)
            : base(input)
        {
            posIncrAtt = AddAttribute<IPositionIncrementAttribute>();
        }

        /*
         * Constructs a PositionFilter that assigns the given position increment to
         * all but the first token from the given input stream.
         * 
         * @param input the input stream
         * @param positionIncrement position increment to assign to all but the first
         *  token from the input stream
         */
        public PositionFilter(TokenStream input, int positionIncrement)
            : this(input)
        {
            this.positionIncrement = positionIncrement;
        }

        public sealed override bool IncrementToken()
        {
            if (input.IncrementToken())
            {
                if (firstTokenPositioned)
                {
                    posIncrAtt.PositionIncrement = positionIncrement;
                }
                else
                {
                    firstTokenPositioned = true;
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        public override void Reset()
        {
            base.Reset();
            firstTokenPositioned = false;
        }
    }
}
