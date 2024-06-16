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

namespace Lucene.Net.Analysis.Shingle.Codec
{
    /// <summary>
    /// Using this codec makes a ShingleMatrixFilter act like ShingleFilter.
    /// It produces the most simple sort of shingles, ignoring token position increments, etc.
    /// 
    /// It adds each token as a new column.
    /// </summary>
    public class OneDimensionalNonWeightedTokenSettingsCodec : TokenSettingsCodec
    {
        public override TokenPositioner GetTokenPositioner(Token token)
        {
            return TokenPositioner.NewColumn;
        }

        public override void SetTokenPositioner(Token token, TokenPositioner tokenPositioner)
        {
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
