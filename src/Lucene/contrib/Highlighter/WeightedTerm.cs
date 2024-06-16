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

namespace Lucene.Net.Search.Highlight
{
    /// <summary>
    /// Lightweight class to hold term and a Weight value used for scoring this term
    /// </summary>
    public class WeightedTerm
    {
        public WeightedTerm(float weight, String term)
        {
            this.Weight = weight;
            this.Term = term;
        }

        /// <summary>
        /// the term value (stemmed)
        /// </summary>
        public string Term { get; set; }

        /// <summary>
        /// the Weight associated with this term
        /// </summary>
        /// <value> </value>
        public float Weight { get; set; }
    }
}