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

namespace Lucene.Net.Analysis.Shingle.Codec
{
    /// <summary>
    /// A codec that creates a two dimensional matrix
    /// by treating tokens from the input stream with 0 position increment
    /// as new rows to the current column.
    /// </summary>
    public class TwoDimensionalNonWeightedSynonymTokenSettingsCodec : TokenSettingsCodec
    {
        public override TokenPositioner GetTokenPositioner(Token token)
        {
            return
                token.PositionIncrement == 0 ? TokenPositioner.NewRow : TokenPositioner.NewColumn;
        }

        public override void SetTokenPositioner(Token token, TokenPositioner tokenPositioner)
        {
            throw new NotSupportedException();
        }

        public override float GetWeight(Token token)
        {
            return 1f;
        }

        public override void SetWeight(Token token, float weight)
        {
        }
    }
}