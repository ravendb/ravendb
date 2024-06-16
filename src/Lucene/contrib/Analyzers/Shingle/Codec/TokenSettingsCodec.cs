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
    /// Strategy used to code and decode meta data of the tokens from the input stream
    /// regarding how to position the tokens in the matrix, set and retreive weight, etc.
    /// </summary>
    public abstract class TokenSettingsCodec
    {
        /// <summary>
        /// Retrieves information on how a Token is to be inserted to a ShingleMatrixFilter.Matrix.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public abstract TokenPositioner GetTokenPositioner(Token token);

        /// <summary>
        /// Sets information on how a Token is to be inserted to a ShingleMatrixFilter.Matrix.
        /// </summary>
        /// <param name="token"></param>
        /// <param name="tokenPositioner"></param>
        public abstract void SetTokenPositioner(Token token, TokenPositioner tokenPositioner);

        /// <summary>
        /// Have this method return 1f in order to 'disable' weights.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public abstract float GetWeight(Token token);

        /// <summary>
        /// Have this method do nothing in order to 'disable' weights.
        /// </summary>
        /// <param name="token"></param>
        /// <param name="weight"></param>
        public abstract void SetWeight(Token token, float weight);
    }
}